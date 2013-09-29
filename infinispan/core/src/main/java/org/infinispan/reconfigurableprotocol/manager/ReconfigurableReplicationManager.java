/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.reconfigurableprotocol.manager;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.ReconfigurableProtocolCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;
import org.infinispan.reconfigurableprotocol.ReconfigurableProtocolRegistry;
import org.infinispan.reconfigurableprotocol.exception.AlreadyRegisterProtocolException;
import org.infinispan.reconfigurableprotocol.exception.NoSuchReconfigurableProtocolException;
import org.infinispan.reconfigurableprotocol.exception.SwitchInProgressException;
import org.infinispan.reconfigurableprotocol.protocol.PassiveReplicationCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TotalOrderCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TwoPhaseCommitProtocol;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.stack.IpAddress;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.infinispan.commands.remote.ReconfigurableProtocolCommand.Type;

/**
 * Manages everything about the replication protocols, namely the switch between protocols and the registry of new
 * replication protocols
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "ReconfigurableReplicationManager", description = "Manages the replication protocol used to commit" +
      " the transactions and for the switching between them")
public class ReconfigurableReplicationManager {

   private static final Log log = LogFactory.getLog(ReconfigurableReplicationManager.class);
   private final ReconfigurableProtocolRegistry registry;
   private final ProtocolManager protocolManager;
   private final CoolDownTimeManager coolDownTimeManager;
   private final StatisticManager statisticManager;
   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private Configuration configuration;
   private ComponentRegistry componentRegistry;
   private volatile boolean allowSwitch = false;
   private boolean defaultProtocolsRegistered;

   public ReconfigurableReplicationManager() {
      statisticManager = new StatisticManager();
      registry = new ReconfigurableProtocolRegistry();
      protocolManager = new ProtocolManager(statisticManager);
      coolDownTimeManager = new CoolDownTimeManager();
      this.defaultProtocolsRegistered = false;
   }

   @Inject
   public final void inject(InterceptorChain interceptorChain, RpcManager rpcManager, CommandsFactory commandsFactory,
                            Configuration configuration, ComponentRegistry componentRegistry) {
      allowSwitch = configuration.clustering().cacheMode().isClustered();
      registry.inject(interceptorChain);
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.configuration = configuration;
      this.componentRegistry = componentRegistry;

      synchronized (this) {
         if (defaultProtocolsRegistered) {
            return;
         }
         defaultProtocolsRegistered = true;
      }

      ReconfigurableProtocol protocol = new TwoPhaseCommitProtocol();
      try {
         protocol.initialize(configuration, componentRegistry, this);
         registry.registerNewProtocol(protocol);
      } catch (AlreadyRegisterProtocolException e) {
         //this exception is catch when you inject the extended statistics
      }
      protocol = new PassiveReplicationCommitProtocol();
      try {
         protocol.initialize(configuration, componentRegistry, this);
         registry.registerNewProtocol(protocol);
      } catch (AlreadyRegisterProtocolException e) {
         //this exception is catch when you inject the extended statistics
      }
      protocol = new TotalOrderCommitProtocol();
      try {
         protocol.initialize(configuration, componentRegistry, this);
         registry.registerNewProtocol(protocol);
      } catch (AlreadyRegisterProtocolException e) {
         //this exception is catch when you inject the extended statistics
      }
      initialProtocol();
   }

   /**
    * method invoked when a message is received from the network. it contains data for the specific replication
    * protocol
    *
    * @param protocolId the target protocol Id
    * @param data       the data
    * @param from       the sender
    */
   public final void handleProtocolData(String protocolId, Object data, Address from) {
      ReconfigurableProtocol protocol = registry.getProtocolById(protocolId);
      if (protocol != null) {
         protocol.handleData(data, from);
      } else {
         log.warnf("Received data [%s] for protocol %s from %s but it does not exits", data, protocol, from);
      }
   }

   public final String beginTransaction(Transaction transaction) throws InterruptedException {
      if (log.isDebugEnabled()) {
         log.debugf("[%s] local transaction %s is starting. check if switch is in progress...",
                    Thread.currentThread().getName(), transaction);
      }
      protocolManager.ensureNotInProgress();
      ReconfigurableProtocol currentProtocol = protocolManager.getCurrent();
      currentProtocol.startTransaction(transaction);

      if (log.isDebugEnabled()) {
         log.debugf("[%s] local transaction %s will use %s while executing", Thread.currentThread().getName(),
                    transaction, currentProtocol.getUniqueProtocolName());
      }

      return currentProtocol.getUniqueProtocolName();
   }

   /**
    * notifies the protocol to a new local transaction that wants to commit. sets the epoch and the protocol to use for
    * this transaction. it blocks if a switch between protocols is in progress.
    *
    * @param globalTransaction the global transaction
    * @throws InterruptedException if interrupted while waiting for the switch to finish
    */
   public final void notifyLocalTransaction(GlobalTransaction globalTransaction, Object[] affectedKeys,
                                            String executionProtocolId, Transaction transaction)
         throws InterruptedException {
      //returns immediately if no switch is in progress
      if (log.isDebugEnabled()) {
         log.debugf("[%s] local transaction %s [%s] wants to commit",
                    Thread.currentThread().getName(), globalTransaction.globalId(), transaction);
      }

      ProtocolManager.CurrentProtocolInfo currentProtocolInfo =
            protocolManager.startCommitTransaction(globalTransaction, affectedKeys,
                                                   registry.getProtocolById(executionProtocolId), transaction);

      if (currentProtocolInfo == null) {
         if (log.isDebugEnabled()) {
            log.debugf("[%s] local transaction %s has start to commit with %s", Thread.currentThread().getName(),
                       globalTransaction.globalId(), globalTransaction.getReconfigurableProtocol());
         }
         return;
      }
      long epoch = currentProtocolInfo.getEpoch();
      ReconfigurableProtocol actual = currentProtocolInfo.getCurrent();

      globalTransaction.setEpochId(epoch);
      globalTransaction.setProtocolId(actual.getUniqueProtocolName());
      globalTransaction.setReconfigurableProtocol(actual);

      if (log.isDebugEnabled()) {
         log.debugf("[%s] local transaction %s will use %s as commit protocol", Thread.currentThread().getName(),
                    globalTransaction.globalId(), currentProtocolInfo);
      }
   }

   public final void notifyLocalTransactionForRollback(LocalTransaction localTransaction, String executionProtocolId) {

      GlobalTransaction globalTransaction = localTransaction.getGlobalTransaction();

      if (log.isDebugEnabled()) {
         log.debugf("[%s] local transaction %s [%s] wants to rollback",
                    Thread.currentThread().getName(), globalTransaction.globalId(), localTransaction.getTransaction());
      }

      if (globalTransaction.getReconfigurableProtocol() == null) {
         String protocolId = globalTransaction.getProtocolId();
         if (protocolId == null) {
            protocolId = executionProtocolId;
         }
         //this probably will originate a epoch mismatch, but it is not a problem
         globalTransaction.setReconfigurableProtocol(registry.getProtocolById(protocolId));
         globalTransaction.setProtocolId(protocolId);
         globalTransaction.setEpochId(-1);
      }

      try {
         globalTransaction.getReconfigurableProtocol().commitTransaction(localTransaction.getTransaction());
      } catch (Exception e) {
         //ignore: this probably will throw an exception saying that the transaction is marked for rollback...
      }
   }

   /**
    * notifies the actual protocol for a remote transaction. if the transaction epoch is lower than the actual epoch
    * then the actual protocol is notified and decides if the transaction can commit or should be aborted
    * <p/>
    * if a transaction with higher epoch is received then it blocks it until the epoch changes
    *
    * @param globalTransaction the global transaction
    * @param affectedKeys      the affected keys, null if it has a clear command
    * @throws InterruptedException if interrupted while waiting for the new epoch
    */
   public final void notifyRemoteTransaction(GlobalTransaction globalTransaction, Object[] affectedKeys)
         throws InterruptedException, NoSuchReconfigurableProtocolException {
      long txEpoch = globalTransaction.getEpochId();
      ReconfigurableProtocol txProtocol = globalTransaction.getReconfigurableProtocol();
      ProtocolManager.CurrentProtocolInfo currentProtocolInfo = protocolManager.getCurrentProtocolInfo();
      long epoch = currentProtocolInfo.getEpoch();

      if (log.isDebugEnabled()) {
         log.debugf("[%s] remote transaction %s received. Epoch is %s (current epoch is %s) and protocol ID is %s",
                    Thread.currentThread().getName(), globalTransaction.globalId(), txEpoch, epoch,
                    globalTransaction.getProtocolId());
      }
      if (txProtocol == null) {
         log.warnf("Protocol ID %s does not exists. Transaction %s will be aborted", globalTransaction.getProtocolId(),
                   globalTransaction.globalId());
         throw new NoSuchReconfigurableProtocolException(globalTransaction.getProtocolId());
      }

      ReconfigurableProtocol currentProtocol = currentProtocolInfo.getCurrent();
      protocolManager.ensure(txEpoch);

      if (txEpoch < epoch) {
         txProtocol.processOldTransaction(globalTransaction, affectedKeys, currentProtocol);
      } else if (txEpoch == epoch) {
         if (!currentProtocol.equals(txProtocol)) {
            throw new IllegalStateException("Transaction protocol differs from the Current transaction protocol for " +
                                                  "the same epoch");
         }
         if (currentProtocolInfo.isUnsafe()) {
            currentProtocol.processSpeculativeTransaction(globalTransaction, affectedKeys, currentProtocolInfo.getOld());
         } else {
            currentProtocol.processTransaction(globalTransaction, affectedKeys);
         }
      }
      txProtocol.addRemoteTransaction(globalTransaction, affectedKeys);
   }

   /**
    * notifies the ending of the local transaction
    *
    * @param globalTransaction the global transaction
    */
   public final void notifyLocalTransactionFinished(GlobalTransaction globalTransaction) {
      ReconfigurableProtocol protocol = registry.getProtocolById(globalTransaction.getProtocolId());

      if (protocol != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Local transaction %s is finished", globalTransaction.globalId());
         }
         protocol.removeLocalTransaction(globalTransaction);
      } else {
         log.fatalf("Local transaction %s is finished but the commit protocol %s does not exits",
                    globalTransaction.globalId(), globalTransaction.getProtocolId());
      }
   }

   /**
    * notifies the ending of the remote transaction
    *
    * @param globalTransaction the global transaction
    */
   public final void notifyRemoteTransactionFinished(GlobalTransaction globalTransaction) {
      ReconfigurableProtocol protocol = registry.getProtocolById(globalTransaction.getProtocolId());

      if (protocol != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Remote transaction %s is finished", globalTransaction.globalId());
         }
         protocol.removeRemoteTransaction(globalTransaction);
      } else {
         log.fatalf("Remote transaction %s is finished but the commit protocol %s does not exits",
                    globalTransaction.globalId(), globalTransaction.getProtocolId());
      }
   }

   /**
    * initializes the global transaction (possibly remote)
    *
    * @param globalTransaction the global transaction
    */
   public final void initGlobalTransactionIfNeeded(GlobalTransaction globalTransaction) {
      if (globalTransaction == null || globalTransaction.getReconfigurableProtocol() != null) {
         return;
      }
      ReconfigurableProtocol protocol = registry.getProtocolById(globalTransaction.getProtocolId());
      if (protocol == null) {
         throw new IllegalArgumentException("Protocol " + globalTransaction.getProtocolId() + " does not exists");
      }
      globalTransaction.setReconfigurableProtocol(protocol);
   }

   /**
    * register a new replication protocol in the ReconfigurableProtocolRegistry.
    *
    * @param clazzName the full class name
    * @throws Exception if it was not registered, due to the class does not extend ReconfigurableProtocol or the
    *                   protocol is already registered
    */
   public final void internalRegister(String clazzName) throws Exception {
      Class<?> clazz = Util.loadClass(clazzName, this.getClass().getClassLoader());
      if (!ReconfigurableProtocol.class.isAssignableFrom(clazz)) {
         log.warnf("Tried to register new replication protocol %s but it does not extends ReconfigurableProtocol",
                   clazzName);
         throw new Exception("Class " + clazzName + " does not extends ReconfigurableProtocol class");
      }
      ReconfigurableProtocol newProtocol = (ReconfigurableProtocol) clazz.newInstance();
      newProtocol.initialize(configuration, componentRegistry, this);
      registry.registerNewProtocol(newProtocol);
   }

   /**
    * change the protocol and set the state as safe (i.e it is safe to process new epoch transactions)
    *
    * @param newProtocol the new replication protocol
    */
   public final void safeSwitch(ReconfigurableProtocol newProtocol) {
      protocolManager.change(newProtocol, true);
   }

   /**
    * change the protocol and set the state as unsafe (i.e it is not safe to process new epoch transactions and some
    * precautions may be needed)
    *
    * @param newProtocol the new replication protocol
    */
   public final void unsafeSwitch(ReconfigurableProtocol newProtocol) {
      protocolManager.change(newProtocol, false);
   }

   public final void internalSetSwitchCoolDownTime(int seconds) {
      coolDownTimeManager.setCoolDownTimePeriod(seconds);
   }

   public final boolean isTotalOrderBasedProtocol(String protocolId) {
      ReconfigurableProtocol protocol = registry.getProtocolById(protocolId);
      return protocol != null && protocol.useTotalOrder();
   }

   public final void startSwitchTask(String protocolId, boolean forceStopTheWorld, boolean abortOnStop, CountDownLatch notifier) {
      new Thread(new SwitchTask(protocolId, forceStopTheWorld, abortOnStop, notifier), "Switch-Thread").start();
   }

   public final void addNumberOfAbortedTransactionDueToSwitch(int val) {
      protocolManager.addNumberOfTransactionsAborted(val);
   }

   public final void initialProtocol(String protocolName, long epoch) {
      ReconfigurableProtocol protocol = registry.getProtocolById(protocolName);

      if (protocol == null) {
         throw new RuntimeException("Protocol ID [" + protocolName + "] not found!");
      }

      if (log.isTraceEnabled()) {
         log.tracef("Initial replication protocol is %s", protocol.getUniqueProtocolName());
      }
      protocolManager.init(protocol, epoch);
   }

   public final void initialProtocol() {
      ReconfigurableProtocol protocol = null;
      switch (configuration.transaction().transactionProtocol()) {
         case TOTAL_ORDER:
            protocol = registry.getProtocolById(TotalOrderCommitProtocol.UID);
            break;
         case DEFAULT:
            protocol = registry.getProtocolById(TwoPhaseCommitProtocol.UID);
            break;
         case PASSIVE_REPLICATION:
            protocol = registry.getProtocolById(PassiveReplicationCommitProtocol.UID);
            break;
      }

      if (protocol == null) {
         throw new RuntimeException("Protocol for " + configuration.transaction().transactionProtocol() + " not found!");
      }

      if (log.isTraceEnabled()) {
         log.tracef("Initial replication protocol is %s", protocol.getUniqueProtocolName());
      }
      protocolManager.init(protocol, 0);
   }

   public final ProtocolManager getProtocolManager() {
      return protocolManager;
   }

   @ManagedOperation(description = "Registers a new replication protocol. The new protocol must extend the " +
         "ReconfigurableProtocol", displayName = "Register protocol")
   public final void register(@Parameter(name = "Class name") String clazzName) throws Exception {
      if (!allowSwitch) {
         return;
      }
      try {
         internalRegister(clazzName);
         ReconfigurableProtocolCommand command = commandsFactory.buildReconfigurableProtocolCommand(Type.REGISTER, clazzName);
         rpcManager.broadcastRpcCommand(command, false, false);
      } catch (Exception e) {
         throw new Exception("Exception while registering class: " + e.getMessage());
      }
   }

   @ManagedOperation(description = "Switch the current replication protocol for the new one. It fails if the protocol " +
         "does not exists or it is equals to the current. It returns -3 if the switch is not allowed, -2 if it is not " +
         "the coordinator and -1 if the protocol is already in use. Otherwise, it returns the next round id.",
                     displayName = "Trigger protocol switch")
   public final long switchTo(@Parameter(name = "Protocol ID") String protocolId,
                              @Parameter(name = "Force Stop the World") boolean forceStopTheWorld,
                              @Parameter(name = "Abort running transactions") boolean abortOnStop) throws Exception {
      if (!allowSwitch) {
         return -3;
      }
      if (!rpcManager.getTransport().isCoordinator()) {
         ReconfigurableProtocolCommand command = commandsFactory.buildReconfigurableProtocolCommand(Type.SWITCH_REQ, protocolId);
         command.setForceStop(forceStopTheWorld);
         command.setAbortOnStop(abortOnStop);
         rpcManager.invokeRemotely(Collections.singleton(getCoordinator()), command, true, false);
         return -2;
      }

      if (registry.getProtocolById(protocolId) == null) {
         log.warnf("Tried to switch the replication protocol to %s but it does not exist", protocolId);
         throw new NoSuchReconfigurableProtocolException(protocolId);
      } else if (protocolManager.isCurrentProtocol(registry.getProtocolById(protocolId))) {
         log.warnf("Tried to switch the replication protocol to %s but it is already the current protocol", protocolId);
         return -1; //nothing to do
      } else if (!coolDownTimeManager.checkAndSetToSwitch()) {
         log.warnf("Tried to switch to %s but you cannot do it right now...", protocolId);
         throw new Exception("You need to wait before perform a new switch");
      }

      ReconfigurableProtocolCommand command = commandsFactory.buildReconfigurableProtocolCommand(Type.SWITCH, protocolId);
      command.setForceStop(forceStopTheWorld);
      command.setAbortOnStop(abortOnStop);

      if (protocolManager.getCurrent().useTotalOrder()) {
         rpcManager.broadcastRpcCommand(command, false, true);
         return protocolManager.getEpoch() + 1;
      }

      rpcManager.broadcastRpcCommand(command, false, false);
      startSwitchTask(protocolId, forceStopTheWorld, abortOnStop, new CountDownLatch(1));
      return protocolManager.getEpoch() + 1;
   }

   @ManagedAttribute(description = "Returns a collection of replication protocols IDs that can be used in the switchTo",
                     writable = false, displayName = "Available protocols IDs")
   public final String getAvailableProtocolIds() {
      Collection<ReconfigurableProtocol> protocols = registry.getAllAvailableProtocols();
      StringBuilder builder = new StringBuilder(128);
      for (ReconfigurableProtocol p : protocols) {
         builder.append(", ").append(p.getUniqueProtocolName());
      }
      return builder.substring(builder.length() >= 2 ? 2 : 0);
   }

   @ManagedAttribute(description = "Returns a collection with the information about the replication protocols available, " +
         "namely, the protocol ID and the class name", writable = false, displayName = "Available protocols information")
   public final Map<String, String> getAvailableProtocolsInfo() {
      Collection<ReconfigurableProtocol> protocols = registry.getAllAvailableProtocols();
      Map<String, String> result = new HashMap<String, String>(protocols.size() * 2);
      for (ReconfigurableProtocol p : protocols) {
         result.putAll(getProtocolInfo(p));
      }
      return result;
   }

   @ManagedAttribute(description = "Returns the current replication protocol ID", writable = false,
                     displayName = "Current protocol ID")
   public final String getCurrentProtocolId() {
      return protocolManager.getCurrent().getUniqueProtocolName();
   }

   @ManagedAttribute(description = "Returns the current replication protocol state", writable = false,
                     displayName = "Current state")
   public final String getCurrentState() {
      return protocolManager.getState().toString();
   }

   @ManagedOperation(description = "Returns the coordinator IP address.",
                     displayName = "Coordinator Host Name")
   public final String getCoordinatorHostName() {
      Address coordinator = getCoordinator();
      if (coordinator instanceof JGroupsAddress) {
         org.jgroups.Address jgroupsAddress = ((JGroupsAddress) coordinator).getJGroupsAddress();
         if (jgroupsAddress instanceof IpAddress) {
            return ((IpAddress) jgroupsAddress).getIpAddress().getHostName();
         }
      }
      String hostname = coordinator.toString();
      int index = hostname.lastIndexOf('-');
      return index == -1 ? hostname : hostname.substring(0, index);
   }

   @ManagedAttribute(description = "Returns the current replication protocol information, namely the protocol ID and " +
         "the class name", writable = false, displayName = "Current protocol information")
   public final Map<String, String> getCurrentProtocolInfo() {
      return getProtocolInfo(protocolManager.getCurrent());
   }

   @ManagedAttribute(description = "Returns the cool down time period in seconds", writable = false,
                     displayName = "Cool Down Time")
   public final int getSwitchCoolDownTime() {
      return coolDownTimeManager.getCoolDownTimePeriod();
   }

   @ManagedOperation(description = "Sets the new cool down time period (in seconds) to wait before two consecutive switches",
                     displayName = "Set Cool Down Time")
   public final void setSwitchCoolDownTime(@Parameter(name = "Cool Down Time in seconds") int seconds) {
      if (!allowSwitch) {
         return;
      }
      internalSetSwitchCoolDownTime(seconds);
      ReconfigurableProtocolCommand command = commandsFactory.buildReconfigurableProtocolCommand(Type.SET_COOL_DOWN_TIME, null);
      command.setData(seconds);
      rpcManager.broadcastRpcCommand(command, false, false);
   }

   @ManagedOperation(description = "Prints the current state", displayName = "Current State")
   public final String printState() {
      ProtocolManager.CurrentProtocolInfo currentProtocolInfo = protocolManager.getCurrentProtocolInfo();
      StringBuilder sb = new StringBuilder();
      sb.append("Epoch=").append(currentProtocolInfo.getEpoch()).append("\n");

      Map.Entry<String, String> info = getProtocolInfo(currentProtocolInfo.getCurrent()).entrySet().iterator().next();

      sb.append("Current Protocol ID=").append(info.getKey()).append("\n");
      sb.append("Current Protocol Class=").append(info.getValue()).append("\n");

      if (currentProtocolInfo.getOld() == null) {
         sb.append("Old Protocol ID=null\n");
         sb.append("Old Protocol Class=null\n");
      } else {
         info = getProtocolInfo(currentProtocolInfo.getOld()).entrySet().iterator().next();
         sb.append("Old Protocol ID=").append(info.getKey()).append("\n");
         sb.append("Old Protocol Class=").append(info.getValue()).append("\n");
      }

      sb.append("State=").append(currentProtocolInfo.printState());
      return sb.toString();
   }

   @ManagedAttribute(description = "Current Epoch", displayName = "Current Epoch")
   public final long getCurrentEpoch() {
      return protocolManager.getEpoch();
   }

   @ManagedOperation(description = "Prints the pending local transactions for the protocol Id",
                     displayName = "Local transactions for protocol ID")
   public final String printLocalTransactions(@Parameter(name = "Protocol ID") String protocolId) {
      ReconfigurableProtocol protocol = registry.getProtocolById(protocolId);
      return protocol == null ? "No such protocol" : protocol.printLocalTransactions();
   }

   @ManagedOperation(description = "Prints the pending remote transactions for the protocol Id",
                     displayName = "Remote transactions for protocol ID")
   public final String printRemoteTransactions(@Parameter(name = "Protocol ID") String protocolId) {
      ReconfigurableProtocol protocol = registry.getProtocolById(protocolId);
      return protocol == null ? "No such protocol" : protocol.printRemoteTransactions();
   }

   @ManagedOperation(description = "Returns the average time in duration between two safe states for a particular " +
         "switch", displayName = "Average Safe to Safe duration between two protocols")
   public final double getAvgSafeToSafeDuration(@Parameter(name = "From protocol ID") String from,
                                                @Parameter(name = "To protocol ID") String to) {
      return statisticManager.getSafeToSafe(from, to);
   }

   @ManagedOperation(description = "Returns the average time in duration to change between a safe state to an unsafe " +
         "state for a particular switch", displayName = "Average Safe to Unsafe duration between two protocols")
   public final double getAvgSafeToUnsafeDuration(@Parameter(name = "From protocol ID") String from,
                                                  @Parameter(name = "To protocol ID") String to) {
      return statisticManager.getSafeToUnsafe(from, to);
   }

   @ManagedOperation(description = "Returns the average time in duration to change between an unsafe state to a safe " +
         "state for a particular switch", displayName = "Average Unsafe to Safe duration between two protocols")
   public final double getAvgUnsafeToSafeDuration(@Parameter(name = "From protocol ID") String from,
                                                  @Parameter(name = "To protocol ID") String to) {
      return statisticManager.getUnsafeToSafe(from, to);
   }

   @ManagedOperation(description = "Returns the number of times that this particular switch happen",
                     displayName = "Switch counter between two protocols")
   public final int getSwitchCounter(@Parameter(name = "From protocol ID") String from,
                                     @Parameter(name = "To protocol ID") String to) {
      return statisticManager.getSwitchCounter(from, to);
   }

   @ManagedOperation(description = "Returns the number of transactions aborted by this particular switch",
                     displayName = "Aborted transactions in switch between two protocols")
   public final int getNumberOfAbortedTransactions(@Parameter(name = "From protocol ID") String from,
                                                   @Parameter(name = "To protocol ID") String to) {
      return statisticManager.getNumberOfAbortedTransactions(from, to);
   }

   @ManagedOperation(description = "Returns all the averages times for all the switches",
                     displayName = "All average durations")
   public final String printSwitchAvgDurations() {
      return statisticManager.printAllStats();
   }

   @ManagedOperation(description = "Resets the switch statistics", displayName = "Reset Statistic")
   public final void resetSwitchStats() {
      statisticManager.reset();
   }

   @ManagedAttribute(description = "Returns true if it is possible to change the reconfigurable protocol", writable = false,
                     displayName = "Allow Switch?")
   public final boolean getAllowProtocolSwitch() {
      return allowSwitch;
   }

   private Address getCoordinator() {
      return rpcManager.getTransport().getCoordinator();
   }

   /**
    * Returns the information about the protocol, namely the protocol ID and the full class name
    *
    * @param protocol the protocol
    * @return the information about the protocol, namely the protocol ID and the full class name
    */
   private Map<String, String> getProtocolInfo(ReconfigurableProtocol protocol) {
      Map<String, String> info = new LinkedHashMap<String, String>();
      info.put(protocol.getUniqueProtocolName(), protocol.getClass().getCanonicalName());
      return info;
   }

   /**
    * switch the replication protocol with the new. the switch will not happen if you try to switch to the same
    * replication protocol or the new protocol does not exist
    * <p/>
    * Note: 1) first it tries to use the non-blocking switch (switchTo method in protocol) 2) if the first fails, it
    * uses the stop-the-world model
    *
    * @param protocolId        the new protocol ID
    * @param forceStopTheWorld true if it must use the stop the world switch (no optimization)
    * @throws NoSuchReconfigurableProtocolException
    *                              if the new protocol does not exist
    * @throws InterruptedException if it is interrupted
    */
   private void internalSwitchTo(String protocolId, boolean forceStopTheWorld, boolean abortOnStop, CountDownLatch notifier)
         throws NoSuchReconfigurableProtocolException, InterruptedException, SwitchInProgressException {
      try {
         ReconfigurableProtocol newProtocol = registry.getProtocolById(protocolId);
         if (newProtocol == null) {
            log.warnf("Tried to switch the replication protocol to %s but it does not exist", protocolId);
            throw new NoSuchReconfigurableProtocolException(protocolId);
         } else if (protocolManager.isCurrentProtocol(newProtocol)) {
            log.warnf("Tried to switch the replication protocol to %s but it is already the current protocol", protocolId);
            return; //nothing to do
         } else if (protocolManager.isInProgress() || protocolManager.isUnsafe()) {
            log.warnf("Tried to switch the replication protocol to %s but a switch is already in progress", protocolId);
            throw new SwitchInProgressException("Switch is in progress");
         }

         protocolManager.inProgress();
         ReconfigurableProtocol currentProtocol = protocolManager.getCurrent();

         if (!forceStopTheWorld && currentProtocol.canSwitchTo(newProtocol)) {
            if (log.isDebugEnabled()) {
               log.debugf("Perform switch from %s to %s with the optimized switch", currentProtocol.getUniqueProtocolName(),
                          newProtocol.getUniqueProtocolName());
            }
            currentProtocol.switchTo(newProtocol);
         } else {
            if (log.isDebugEnabled()) {
               log.debugf("Perform switch from %s to %s by stop-the-world model", currentProtocol.getUniqueProtocolName(),
                          newProtocol.getUniqueProtocolName());
            }
            notifier.countDown();
            currentProtocol.stopProtocol(abortOnStop);
            newProtocol.bootProtocol();
            safeSwitch(newProtocol);
         }
      } finally {
         notifier.countDown();
      }
   }

   /**
    * manages the cool down time between two consecutive switches
    */
   private class CoolDownTimeManager {
      private long nextSwitchTime; //in milliseconds
      private long coolDownTimePeriod; //in milliseconds;

      public CoolDownTimeManager() {
         nextSwitchTime = System.currentTimeMillis();
         coolDownTimePeriod = 60000; //1 min
      }

      public synchronized boolean checkAndSetToSwitch() {
         if (nextSwitchTime <= System.currentTimeMillis()) {
            nextSwitchTime = System.currentTimeMillis() + coolDownTimePeriod;
            return true;
         } else {
            return false;
         }
      }

      public synchronized int getCoolDownTimePeriod() {
         return (int) (coolDownTimePeriod / 1000);
      }

      public synchronized void setCoolDownTimePeriod(int seconds) {
         coolDownTimePeriod = seconds * 1000;
      }
   }

   private class SwitchTask implements Runnable {

      private final String protocolId;
      private final CountDownLatch notifier;
      private final boolean forceStopTheWorld;
      private final boolean abortOnStop;

      private SwitchTask(String protocolId, boolean forceStopTheWorld, boolean abortOnStop, CountDownLatch notifier) {
         this.protocolId = protocolId;
         this.notifier = notifier;
         this.forceStopTheWorld = forceStopTheWorld;
         this.abortOnStop = abortOnStop;
      }

      @Override
      public void run() {
         try {
            internalSwitchTo(protocolId, forceStopTheWorld, abortOnStop, notifier);
         } catch (Exception e) {
            if (log.isDebugEnabled()) {
               log.debugf(e, "Error switching protocol to %s.", protocolId);
            } else {
               log.warnf("Error switching protocol to %s. %s", protocolId, e.getMessage());
            }
         }
      }
   }

}
