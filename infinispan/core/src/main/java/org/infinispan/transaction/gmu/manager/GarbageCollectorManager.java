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

package org.infinispan.transaction.gmu.manager;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.GarbageCollectorControlCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.gmu.L1GMUContainer;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.infinispan.commands.remote.GarbageCollectorControlCommand.Type.*;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Listener
public class GarbageCollectorManager {
   private static final Log log = LogFactory.getLog(GarbageCollectorManager.class);
   private CommitLog commitLog;
   private CommandsFactory commandsFactory;
   private RpcManager rpcManager;
   private Configuration configuration;
   private GMUVersionGenerator versionGenerator;
   private DataContainer dataContainer;
   private TransactionTable transactionTable;
   private L1GMUContainer l1GMUContainer;
   private ClusterTopologyManager clusterTopologyManager;
   private boolean enabled;
   private VersionGarbageCollectorThread versionGarbageCollectorThread;
   private L1GarbageCollectorThread l1GarbageCollectorThread;
   private ViewGarbageCollectorThread viewGarbageCollectorThread;
   private CacheManagerNotifier cacheManagerNotifier;

   @Inject
   public void inject(CommitLog commitLog, CommandsFactory commandsFactory, RpcManager rpcManager,
                      Configuration configuration, VersionGenerator versionGenerator, DataContainer dataContainer,
                      TransactionTable transactionTable, L1GMUContainer l1GMUContainer, ClusterTopologyManager clusterTopologyManager,
                      CacheManagerNotifier cacheManagerNotifier) {
      this.configuration = configuration;
      if (configuration.locking().isolationLevel() != IsolationLevel.SERIALIZABLE) {
         return;
      }
      this.commitLog = commitLog;
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
      this.dataContainer = dataContainer;
      this.transactionTable = transactionTable;
      this.l1GMUContainer = l1GMUContainer;
      this.clusterTopologyManager = clusterTopologyManager;
      this.cacheManagerNotifier = cacheManagerNotifier;
   }

   @Start
   public void start() {
      this.enabled = configuration.garbageCollector().enabled() &&
            configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE;

      if (enabled) {
         versionGarbageCollectorThread = new VersionGarbageCollectorThread(configuration.garbageCollector().transactionThreshold(),
                                                                           configuration.garbageCollector().versionGCMaxIdle());
         l1GarbageCollectorThread = new L1GarbageCollectorThread(configuration.garbageCollector().l1GCInterval());
         viewGarbageCollectorThread = new ViewGarbageCollectorThread(configuration.garbageCollector().viewGCBackOff());

         versionGarbageCollectorThread.start();
         l1GarbageCollectorThread.start();
         viewGarbageCollectorThread.start();
         cacheManagerNotifier.addListener(this);
      }
   }

   @Stop
   public void stop() {
      if (enabled) {
         versionGarbageCollectorThread.interrupt();
         l1GarbageCollectorThread.interrupt();
         viewGarbageCollectorThread.interrupt();
         cacheManagerNotifier.removeListener(this);
      }
   }

   /**
    * The main goal is to delete all version that cannot be visible by any transaction in the system. To do that, some
    * remote interaction is needed.
    * <p/>
    * The algorithm is performed in 4 steps:
    * <p/>
    * 1 - for each node i, returns the i-th position for the oldest commit log entry used by it owns local transactions
    * 2 - a version (VC) is created with all the previous values 3 - gets the newest commit log entry that is lower than
    * the VC created in 2) 4 - delete all commit log entries older than the version in 3) and delete all values with
    * version older than the version in 3)
    */
   public final synchronized void triggerVersionGarbageCollection() {
      if (!enabled) {
         return;
      }
      versionGarbageCollectorThread.trigger();
   }

   /**
    * The main goal is to delete all older view id => addresses that are not in use by any version
    * <p/>
    * Note: the view id is used in gmu distributed version, which are only used in transactions versions and commit log
    * entries. checking in the commit log should be enough to determine the minimum view id
    * <p/>
    * Note: this algorithm is triggered only by the coordinator
    * <p/>
    * The algorithm is performed in 3 steps: 1 - collect the minimum view id in commit log from local node and remote
    * node 2 - broadcast the minimum view id to all members 3 - when the message in 2) is received, delete all view ids
    * less than minimum view id
    */
   public final synchronized void triggerViewGarbageCollection() {
      if (!enabled) {
         return;
      }
      viewGarbageCollectorThread.triggerNow();
   }

   /**
    * The main goal is to delete all versions that are not visible any more. Note that the L1 is used only be local
    * transactions, so no remote interaction is needed.
    * <p/>
    * The algorithm is performed in 3 steps: 1 - pick the oldest version in commit log used by the local transactions 2
    * - delete all values with creation version less than the version in 1)
    */
   public final synchronized void triggerL1GarbageCollection() {
      if (!enabled || configuration.clustering().cacheMode().isReplicated()) {
         return;
      }
      l1GarbageCollectorThread.trigger();
   }

   public final GMUVersion handleGetMinimumVisibleVersion() {
      List<EntryVersion> localTransactionsCommitLogEntries = new LinkedList<EntryVersion>();
      for (LocalTransaction localTransaction : transactionTable.getLocalTransactions()) {
         localTransactionsCommitLogEntries.add(localTransaction.getTransactionVersion());
      }

      EntryVersion[] array = new EntryVersion[localTransactionsCommitLogEntries.size()];
      array = localTransactionsCommitLogEntries.toArray(array);

      GMUVersion minimumVisibleVersion = versionGenerator.mergeAndMin(array);

      if (log.isTraceEnabled()) {
         log.tracef("handleGetMinimumVisibleVersion() ==> %s", minimumVisibleVersion);
      }

      return minimumVisibleVersion;
   }

   public final int handleGetMinimumVisibleViewId() {
      int minimumVisibleViewId = commitLog.calculateMinimumViewId();
      if (log.isTraceEnabled()) {
         log.tracef("handleGetMinimumVisibleViewId() ==> %s", minimumVisibleViewId);
      }
      return minimumVisibleViewId;
   }

   public final void handleDeleteOlderViewId(int minimumVisibleId) {
      if (log.isTraceEnabled()) {
         log.tracef("Deleting older view than %s", minimumVisibleId);
      }
      clusterTopologyManager.gcUnreachableCacheTopology(commandsFactory.getCacheName(), minimumVisibleId);
      versionGenerator.gcTopologyIds(minimumVisibleId);
   }

   public final void notifyCommittedTransactions(int size) {
      if (!enabled) {
         return;
      }
      versionGarbageCollectorThread.addCommittedTx(size);
   }

   @ViewChanged
   @Merged
   public final void handle(Event event) {
      viewGarbageCollectorThread.trigger();
   }

   private long getTimeout() {
      return configuration.clustering().sync().replTimeout();
   }

   private <T> Map<Address, T> convert(Map<Address, Response> responseMap, Class<T> tClass) throws Exception {
      Map<Address, T> retVal = new HashMap<Address, T>();
      for (Map.Entry<Address, Response> entry : responseMap.entrySet()) {
         Response response = entry.getValue();
         if (!response.isSuccessful()) {
            throw new Exception("Error executing command");
         }
         retVal.put(entry.getKey(), tClass.cast(((SuccessfulResponse) response).getResponseValue()));
      }
      return retVal;
   }

   private class VersionGarbageCollectorThread extends Thread {

      private final int transactionThreshold;
      private final int maxIdleTime;
      private int txCommittedSinceLastReset;
      private volatile boolean running;

      private VersionGarbageCollectorThread(int transactionThreshold, int maxIdleTime) {
         super("Version-GC-Thread");
         this.transactionThreshold = transactionThreshold;
         this.maxIdleTime = maxIdleTime;
      }

      @Override
      public void run() {
         running = true;
         while (running) {
            block();
            if (running) {
               gc();
            }
         }
      }

      @Override
      public void interrupt() {
         running = false;
         super.interrupt();
      }

      public final synchronized void addCommittedTx(int committed) {
         txCommittedSinceLastReset += committed;
         if (txCommittedSinceLastReset > transactionThreshold) {
            notify();
         }
      }

      public final synchronized void trigger() {
         notify();
      }

      /**
       * The main goal is to delete all version that cannot be visible by any transaction in the system. To do that,
       * some remote interaction is needed.
       * <p/>
       * The algorithm is performed in 4 steps:
       * <p/>
       * 1 - for each node i, returns the i-th position for the oldest commit log entry used by it owns local
       * transactions 2 - a version (VC) is created with all the previous values 3 - gets the newest commit log entry
       * that is lower than the VC created in 2) 4 - delete all commit log entries older than the version in 3) and
       * delete all values with version older than the version in 3)
       */
      private void gc() {
         try {
            if (log.isTraceEnabled()) {
               log.tracef("Starting Garbage Collection for old versions");
            }
            //step 1
            GarbageCollectorControlCommand cmd = commandsFactory.buildGarbageCollectorControlCommand(GET_VERSION, -1);
            Map<Address, Response> responseMap = rpcManager.invokeRemotely(null, cmd,
                                                                           ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS,
                                                                           getTimeout(), true, false);
            Map<Address, EntryVersion> minVersionValues = convert(responseMap, EntryVersion.class);
            commandsFactory.initializeReplicableCommand(cmd, false);
            minVersionValues.put(rpcManager.getAddress(), (EntryVersion) cmd.perform(null));

            EntryVersion[] array = new EntryVersion[minVersionValues.values().size()];
            array = minVersionValues.values().toArray(array);

            //step 2
            GMUVersion globalMinimumVersion = versionGenerator.mergeAndMin(array);

            if (log.isTraceEnabled()) {
               log.tracef("Finished collection current transactions versions. Minimum version is %s",
                          globalMinimumVersion);
            }

            //step 3
            GMUVersion minimumLocalVersion = commitLog.gcOlderVersions(globalMinimumVersion);

            if (log.isTraceEnabled()) {
               log.tracef("Minimum local visible version is %s", minimumLocalVersion);
            }

            //step 4
            dataContainer.gc(minimumLocalVersion);
         } catch (Throwable throwable) {
            log.warnf("Exception caught while garbage collecting oldest versions: " + throwable.getLocalizedMessage());
         }
      }

      private synchronized void block() {
         txCommittedSinceLastReset = 0;
         try {
            wait(maxIdleTime * 1000);
         } catch (InterruptedException e) {
            //no-op
         }
      }
   }

   private class L1GarbageCollectorThread extends Thread {

      private final int interval;
      private volatile boolean running;

      private L1GarbageCollectorThread(int interval) {
         super("L1-GC-Thread");
         this.interval = interval;
      }

      @Override
      public void run() {
         if (interval == 0 || configuration.clustering().cacheMode().isReplicated()) {
            return;
         }
         running = true;
         while (running) {
            block();
            if (running) {
               gc();
            }
         }
      }

      @Override
      public void interrupt() {
         running = false;
         super.interrupt();
      }

      public final synchronized void trigger() {
         notify();
      }

      /**
       * The main goal is to delete all versions that are not visible any more. Note that the L1 is used only be local
       * transactions, so no remote interaction is needed.
       * <p/>
       * The algorithm is performed in 2 steps: 1 - pick the oldest version in commit log used by the local transactions
       * 2 - delete all values with creation version less than the version in 1)
       */
      private void gc() {
         if (log.isTraceEnabled()) {
            log.tracef("Starting Garbage Collection for old L1 versions");
         }
         //step 1
         GMUVersion minLocalVersion = handleGetMinimumVisibleVersion();

         if (log.isTraceEnabled()) {
            log.tracef("Garbage Collector old versions in L1 Cache. Minimum local visible version is %s",
                       minLocalVersion);
         }

         //step 2
         l1GMUContainer.gc(minLocalVersion);
      }

      private synchronized void block() {
         try {
            wait(interval * 1000);
         } catch (InterruptedException e) {
            //no-op
         }
      }
   }

   private class ViewGarbageCollectorThread extends Thread {

      private final int backOff;
      private boolean running;
      private boolean triggered;
      private boolean triggerNow;

      private ViewGarbageCollectorThread(int backOff) {
         super("View-GC-Thread");
         this.backOff = backOff;
      }

      @Override
      public void run() {
         running = true;
         while (running) {
            block();
            if (running) {
               gc();
            }
         }
      }

      public final synchronized void triggerNow() {
         triggerNow = true;
         notify();
      }

      @Override
      public void interrupt() {
         running = false;
         super.interrupt();
      }

      @ViewChanged
      @Merged
      public void handle(Event event) {
         trigger();
      }

      public final synchronized void trigger() {
         triggered = true;
         notify();
      }

      /**
       * The main goal is to delete all older view id => addresses that are not in use by any version
       * <p/>
       * Note: the view id is used in gmu distributed version, which are only used in transactions versions and commit
       * log entries. checking in the commit log should be enough to determine the minimum view id
       * <p/>
       * Note: this algorithm is triggered only by the coordinator
       * <p/>
       * The algorithm is performed in 3 steps: 1 - collect the minimum view id in commit log from local node and remote
       * node 2 - broadcast the minimum view id to all members 3 - when the message in 2) is received, delete all view
       * ids less than minimum view id
       */
      private void gc() {
         try {
            if (log.isTraceEnabled()) {
               log.tracef("Starting Garbage Collector for old cache views");
            }
            GarbageCollectorControlCommand request = commandsFactory.buildGarbageCollectorControlCommand(GET_VIEW_ID, -1);

            Map<Address, Response> responseMap = rpcManager.invokeRemotely(null, request,
                                                                           ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS,
                                                                           getTimeout(), true, false);
            Map<Address, Integer> viewIdMap = convert(responseMap, Integer.class);
            commandsFactory.initializeReplicableCommand(request, false);
            viewIdMap.put(rpcManager.getAddress(), (Integer) request.perform(null));
            Iterator<Integer> iterator = viewIdMap.values().iterator();
            int minimumViewId;

            if (iterator.hasNext()) {
               minimumViewId = iterator.next();
            } else {
               if (log.isTraceEnabled()) {
                  log.tracef("No Garbage Collect needed");
               }
               //no GC
               return;
            }

            while (iterator.hasNext()) {
               minimumViewId = Math.min(minimumViewId, iterator.next());
            }

            if (log.isTraceEnabled()) {
               log.tracef("Garbage collect cache views older than %s", minimumViewId);
            }

            GarbageCollectorControlCommand control = commandsFactory.buildGarbageCollectorControlCommand(SET_VIEW_ID,
                                                                                                         minimumViewId);
            rpcManager.broadcastRpcCommand(control, false, false);
            commandsFactory.initializeReplicableCommand(control, false);
            control.perform(null);
         } catch (Throwable throwable) {
            log.warnf("Exception caught while garbage collecting oldest cache views: " + throwable.getLocalizedMessage());
         }
      }

      private synchronized void block() {
         try {
            while (!triggered && !triggerNow) {
               wait();
            }
            while (triggered && !triggerNow) {
               triggered = false;
               wait(backOff * 1000);
            }
         } catch (InterruptedException e) {
            //no-op
         }
         triggered = false;
         triggerNow = false;
      }
   }

}
