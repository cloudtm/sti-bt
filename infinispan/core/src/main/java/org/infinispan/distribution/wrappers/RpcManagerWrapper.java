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
package org.infinispan.distribution.wrappers;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GMUClusteredGetCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderGMUPrepareCommand;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.AbstractResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.stats.ExposedStatistic;
import org.infinispan.stats.PiggyBackStat;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.container.TransactionStatistics;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.concurrent.ResponseFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.infinispan.stats.ExposedStatistic.*;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class RpcManagerWrapper implements RpcManager {
   private static final Log log = LogFactory.getLog(RpcManagerWrapper.class);
   private final RpcManager actual;
   private final RpcDispatcher.Marshaller marshaller;
   private Address myAddress;

   public RpcManagerWrapper(RpcManager actual) {
      this.actual = actual;
      Transport t = actual.getTransport();
      if (t instanceof JGroupsTransport) {
         marshaller = ((JGroupsTransport) t).getCommandAwareRpcDispatcher().getMarshaller();
      } else {
         marshaller = null;
      }
      myAddress = actual.getTransport().getAddress();
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, boolean usePriorityQueue,
                                                ResponseFilter responseFilter, boolean totalOrder) {
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter, totalOrder);
      updateStats(rpcCommand, mode.isSynchronous(), currentTime, recipients, null, ret);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, boolean usePriorityQueue, boolean totalOrder) {
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, totalOrder);
      updateStats(rpcCommand, mode.isSynchronous(), currentTime, recipients, null, ret);
      return ret;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean totalOrder) {
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, totalOrder);
      updateStats(rpcCommand, mode.isSynchronous(), currentTime, recipients, null, ret);
      return ret;
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean totalOrder) throws RpcException {
      long currentTime = System.nanoTime();
      actual.broadcastRpcCommand(rpc, sync, totalOrder);
      updateStats(rpc, sync, currentTime, null, null, null);
   }

   @Override
   public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue, boolean totalOrder) throws RpcException {
      long currentTime = System.nanoTime();
      actual.broadcastRpcCommand(rpc, sync, usePriorityQueue, totalOrder);
      updateStats(rpc, sync, currentTime, null, null, null);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
      long currentTime = System.nanoTime();
      actual.broadcastRpcCommandInFuture(rpc, future);
      updateStats(rpc, false, currentTime, null, null, null);
   }

   @Override
   public void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue,
                                           NotifyingNotifiableFuture<Object> future) {
      long currentTime = System.nanoTime();
      actual.broadcastRpcCommandInFuture(rpc, usePriorityQueue, future);
      updateStats(rpc, false, currentTime, null, null, null);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean totalOrder) throws RpcException {
      long currentTime = System.nanoTime();
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpc, sync, totalOrder);
      updateStats(rpc, sync, currentTime, recipients, null, ret);
      return ret;
   }

   @Override
   //This should be the method invoked at prepareTime
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync,
                                                boolean usePriorityQueue, boolean totalOrder) throws RpcException {
      boolean isPrepareCmd = rpc instanceof PrepareCommand;
      final TransactionStatistics transactionStatistics = TransactionsStatisticsRegistry.getTransactionStatistics();

      long currentTime = System.nanoTime();
      if (isPrepareCmd && transactionStatistics != null) {
         transactionStatistics.markPrepareSent();
      }
      Map<Address, Response> ret = actual.invokeRemotely(recipients, rpc, sync, usePriorityQueue, totalOrder);
      if (transactionStatistics != null) {
         updateStats(rpc, sync, currentTime, recipients, null, ret);
      }
      return ret;
   }

   @Override
   public ResponseFuture invokeRemotelyWithFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, boolean totalOrder) {
      long currentTime = System.nanoTime();
      ResponseFuture ret = actual.invokeRemotelyWithFuture(recipients, rpc, usePriorityQueue, totalOrder);
      updateStats(rpc, true, currentTime, recipients, ret, null);
      return ret;
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc,
                                      NotifyingNotifiableFuture<Object> future) {
      long currentTime = System.nanoTime();
      actual.invokeRemotelyInFuture(recipients, rpc, future);
      updateStats(rpc, false, currentTime, recipients, null, null);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future) {
      long currentTime = System.nanoTime();
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
      updateStats(rpc, false, currentTime, recipients, null, null);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future, long timeout) {
      long currentTime = System.nanoTime();
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout);
      updateStats(rpc, false, currentTime, recipients, null, null);
   }

   @Override
   public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue,
                                      NotifyingNotifiableFuture<Object> future, long timeout, boolean ignoreLeavers) {
      long currentTime = System.nanoTime();
      actual.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout, ignoreLeavers);
      updateStats(rpc, false, currentTime, recipients, null, null);
   }

   @Override
   public Transport getTransport() {
      return actual.getTransport();
   }

   @Override
   public List<Address> getMembers() {
      return actual.getMembers();
   }

   @Override
   public Address getAddress() {
      return actual.getAddress();
   }

   @Override
   public int getTopologyId() {
      return actual.getTopologyId();
   }

   private void updateStats(ReplicableCommand command, boolean sync, long init, Collection<Address> recipients, ResponseFuture future, Map<Address, Response> responseMap) {
      final TransactionStatistics transactionStatistics = TransactionsStatisticsRegistry.getTransactionStatistics();
      if (!TransactionsStatisticsRegistry.isActive() || transactionStatistics == null &&
            !(command instanceof TxCompletionNotificationCommand)) {
         if (log.isTraceEnabled()) {
            log.tracef("Does not update stats for command %s. No statistic collector found", command);
         }
         return;
      } else if (transactionStatistics != null && !transactionStatistics.isLocal()) {
         if (log.isTraceEnabled()) {
            log.tracef("Does not update stats for command %s. The command is remote!", command);
         }
         return;
      }
      ExposedStatistic durationStat;
      ExposedStatistic counterStat;
      ExposedStatistic recipientSizeStat;
      ExposedStatistic commandSizeStat = null;
      long contactedNodesMinusMe = recipientListSize(recipients) - (isCurrentNodeInvolved(recipients) ? 1 : 0);
      long wallClockTimeTaken = System.nanoTime() - init;
      if (command instanceof PrepareCommand) {
         if (sync) {
            durationStat = RTT_PREPARE;
            counterStat = NUM_RTTS_PREPARE;
         } else {
            durationStat = ASYNC_PREPARE;
            counterStat = NUM_ASYNC_PREPARE;
         }
         recipientSizeStat = NUM_NODES_PREPARE;
         commandSizeStat = PREPARE_COMMAND_SIZE;

         if (command instanceof TotalOrderGMUPrepareCommand) {
            WaitStats w = new WaitStats(responseMap);
            long maxW = w.maxConditionalWaitTime;
            long avgW = w.avgUnconditionalWaitTime;
            long condAvg = w.avgConditionalWaitTime;
            long waits = w.numWaitedNodes;
            transactionStatistics.addValue(TO_GMU_PREPARE_COMMAND_RTT_MINUS_MAX, wallClockTimeTaken - maxW);
            transactionStatistics.addValue(TO_GMU_PREPARE_COMMAND_RTT_MINUS_AVG, wallClockTimeTaken - avgW);
            if (waits > 0) {
               transactionStatistics.incrementValue(NUM_TO_GMU_PREPARE_COMMAND_AT_LEAST_ONE_WAIT);
               transactionStatistics.addValue(TO_GMU_PREPARE_COMMAND_NODES_WAITED, waits);
               transactionStatistics.addValue(TO_GMU_PREPARE_COMMAND_AVG_WAIT_TIME, condAvg);
               transactionStatistics.addValue(TO_GMU_PREPARE_COMMAND_MAX_WAIT_TIME, maxW);
            } else {
               transactionStatistics.incrementValue(NUM_TO_GMU_PREPARE_COMMAND_RTT_NO_WAITED);//NB this could be obtained by taking the total number of prepare-the ones that waited
               transactionStatistics.addValue(TO_GMU_PREPARE_COMMAND_RTT_NO_WAIT, wallClockTimeTaken);
            }
         }
      } else if (command instanceof RollbackCommand) {
         if (sync) {
            durationStat = RTT_ROLLBACK;
            counterStat = NUM_RTTS_ROLLBACK;
         } else {
            durationStat = ASYNC_ROLLBACK;
            counterStat = NUM_ASYNC_ROLLBACK;
         }
         recipientSizeStat = NUM_NODES_ROLLBACK;
         commandSizeStat = ROLLBACK_COMMAND_SIZE;
      } else if (command instanceof CommitCommand) {
         if (sync) {
            durationStat = RTT_COMMIT;
            counterStat = NUM_RTTS_COMMIT;
            transactionStatistics.addValue(SENT_SYNC_COMMIT, contactedNodesMinusMe);
         } else {
            durationStat = ASYNC_COMMIT;
            counterStat = NUM_ASYNC_COMMIT;
            transactionStatistics.addValue(SENT_ASYNC_COMMIT, contactedNodesMinusMe);
         }
         recipientSizeStat = NUM_NODES_COMMIT;
         commandSizeStat = COMMIT_COMMAND_SIZE;
      } else if (command instanceof ClusteredGetCommand) {
         durationStat = RTT_GET;
         counterStat = NUM_RTTS_GET;
         recipientSizeStat = NUM_NODES_GET;
         commandSizeStat = CLUSTERED_GET_COMMAND_SIZE;
         //Take rtt sample if the remote read has not waited
         if (command instanceof GMUClusteredGetCommand) {
            if (pickGmuRemoteGetWaitingTime(responseMap) == 0) {
               transactionStatistics.incrementValue(NUM_RTT_GET_NO_WAIT);
               transactionStatistics.addValue(RTT_GET_NO_WAIT, wallClockTimeTaken);
            }
         }
      } else if (command instanceof TxCompletionNotificationCommand) {
         durationStat = ASYNC_COMPLETE_NOTIFY;
         counterStat = NUM_ASYNC_COMPLETE_NOTIFY;
         recipientSizeStat = NUM_NODES_COMPLETE_NOTIFY;
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Does not update stats for command %s. The command is not needed", command);
         }
         return;
      }

      if (log.isTraceEnabled()) {
         log.tracef("Update stats for command %s. Is sync? %s. Duration stat is %s, counter stats is %s, " +
                          "recipient size stat is %s", command, sync, durationStat, counterStat, recipientSizeStat);
      }

      if (future != null) {
         future.setUpdateStats(transactionStatistics, init, durationStat, counterStat, recipientSizeStat, commandSizeStat,
                               getCommandSize(command), recipientListSize(recipients));
      } else if (transactionStatistics != null) {
         transactionStatistics.addValue(durationStat, wallClockTimeTaken);
         transactionStatistics.incrementValue(counterStat);
         transactionStatistics.addValue(recipientSizeStat, recipientListSize(recipients));
         if (commandSizeStat != null) {
            transactionStatistics.addValue(commandSizeStat, getCommandSize(command));
         }
      } else {
         TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(durationStat, wallClockTimeTaken, true);
         TransactionsStatisticsRegistry.incrementValueAndFlushIfNeeded(counterStat, true);
         TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(recipientSizeStat, recipientListSize(recipients), true);
      }
   }

   private int recipientListSize(Collection<Address> recipients) {
      return recipients == null ? actual.getTransport().getMembers().size() : recipients.size();
   }

   private boolean isCurrentNodeInvolved(Collection<Address> recipients) {
      //If recipients is null it's either a BroadCast (or I am the only one in the cluster, which is a trivial broadcast)
      return recipients == null || recipients.contains(myAddress);
   }

   private int getCommandSize(ReplicableCommand command) {
      try {
         Buffer buffer = marshaller.objectToBuffer(command);
         return buffer != null ? buffer.getLength() : 0;
      } catch (Exception e) {
         return 0;
      }
   }

   private long pickGmuRemoteGetWaitingTime(Map<Address, Response> map) {
      if (map == null || map.size() == 0) {
         if (log.isDebugEnabled())
            log.debug("GmuClusteredGetCommand reply is empty");
         return -1;
      }
      AbstractResponse r;
      long w;
      PiggyBackStat pbs;
      for (Map.Entry<Address, Response> e : map.entrySet()) {
         if (e != null && (r = (AbstractResponse) e.getValue()) != null) {
            pbs = r.getPiggyBackStat();
            if (pbs != null) {
               if ((w = pbs.getWaitTime()) > 0) {
                  return w;
               }
            }
         }
      }
      return 0;
   }

   private class WaitStats {
      private long numWaitedNodes;
      private long avgConditionalWaitTime;
      private long maxConditionalWaitTime;
      private long avgUnconditionalWaitTime;


      WaitStats(Map<Address, Response> map) {
         if (map == null || map.size() == 0)
            return;
         long max = 0, sum = 0, temp;
         long waited = 0;
         AbstractResponse r;
         Set<Map.Entry<Address, Response>> set = map.entrySet();
         for (Map.Entry<Address, Response> e : set) {
            r = (AbstractResponse) e.getValue();
            temp = r.getPiggyBackStat().getWaitTime();
            if (temp > 0) {
               waited++;
               if (temp > max) {
                  temp = max;
               }
               sum += temp;
            }
         }
         long unAvg = (sum / set.size());
         long coAvg = waited != 0 ? (sum / waited) : 0;

         this.maxConditionalWaitTime = max;
         this.avgUnconditionalWaitTime = unAvg;
         this.avgConditionalWaitTime = coAvg;
         this.numWaitedNodes = waited;
      }
   }


}
