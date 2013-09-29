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
package org.infinispan.stats;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.stats.container.LocalTransactionStatistics;
import org.infinispan.stats.container.RemoteTransactionStatistics;
import org.infinispan.stats.container.TransactionStatistics;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Websiste: www.cloudtm.eu Date: 20/04/12
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public final class TransactionsStatisticsRegistry {

   public static final String DEFAULT_ISPN_CLASS = "DEFAULT_ISPN_CLASS";
   private static final Log log = LogFactory.getLog(TransactionsStatisticsRegistry.class);
   //Now it is unbounded, we can define a MAX_NO_CLASSES
   private static final ConcurrentMap<String, NodeScopeStatisticCollector> transactionalClassesStatsMap
         = ConcurrentMapFactory.makeConcurrentMap();
   private static final ConcurrentMap<GlobalTransaction, RemoteTransactionStatistics> remoteTransactionStatistics =
         ConcurrentMapFactory.makeConcurrentMap();
   //Comment for reviewers: do we really need threadLocal? If I have the global id of the transaction, I can
   //retrieve the transactionStatistics
   private static final ThreadLocal<TransactionStatistics> thread = new ThreadLocal<TransactionStatistics>();
   private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
   private static volatile boolean active = false;
   private static volatile boolean gmuWaitingActive = false;
   private static ConcurrentHashMap<String, Map<Object, Long>> pendingLocks = new ConcurrentHashMap<String, Map<Object, Long>>();
   private static Configuration configuration;
   private static ThreadLocal<TransactionTS> lastTransactionTS = new ThreadLocal<TransactionTS>();
   private static boolean sampleServiceTime = false;

   public static boolean isActive() {
      return active;
   }

   public static void setActive(boolean enabled) {
      active = enabled;
   }

   public static boolean isSampleServiceTime() {
      return active && sampleServiceTime;
   }

   public static void init(Configuration configuration) {
      log.info("Initializing transactionalClassesMap");
      TransactionsStatisticsRegistry.configuration = configuration;
      sampleServiceTime = configuration.customStatsConfiguration().sampleServiceTimes();
      setGmuWaitingActive(configuration.customStatsConfiguration().gmuWaitingTimeEnabled());
      transactionalClassesStatsMap.put(DEFAULT_ISPN_CLASS, new NodeScopeStatisticCollector());
      active = true;
   }

   public static long getThreadCPUTime() {
      if (isSampleServiceTime() && THREAD_MX_BEAN != null) {
         return THREAD_MX_BEAN.getCurrentThreadCpuTime();
      }
      return 0;
   }

   public static void appendLocks() {
      if (!active) {
         return;
      }
      TransactionStatistics stats = thread.get();
      if (stats == null) {
         log.debug("Trying to append locks " +
                         " but no transaction is associated to the thread");
         return;
      }
      appendLocks(stats.getTakenLocks(), stats.getId());
   }

   public static TransactionStatistics getTransactionStatistics() {
      return active ? thread.get() : null;
   }

   /**
    * DIE This is used when you do not have the transactionStatics anymore (i.e., after a commitCommand) and still have
    * to update some statistics
    */
   public static void addValueAndFlushIfNeeded(ExposedStatistic param, double value, boolean local) {
      if (!active) {
         return;
      }
      if (log.isDebugEnabled()) {
         log.debugf("Flushing value %s to parameter %s without attached xact",
                    value, param);
      }
      NodeScopeStatisticCollector nssc = transactionalClassesStatsMap.get(DEFAULT_ISPN_CLASS);
      if (local) {
         nssc.addLocalValue(param, value);
      } else {
         nssc.addRemoteValue(param, value);
      }
   }

   public static void incrementValueAndFlushIfNeeded(ExposedStatistic param, boolean local) {
      if (!active) {
         return;
      }
      NodeScopeStatisticCollector nssc = transactionalClassesStatsMap.get(DEFAULT_ISPN_CLASS);
      if (local) {
         nssc.addLocalValue(param, 1D);
      } else {
         nssc.addRemoteValue(param, 1D);
      }
   }

   public static void terminateTransaction(TransactionStatistics transactionStatistics) {
      if (!active) {
         return;
      }
      TransactionStatistics txs = transactionStatistics == null ? thread.get() : transactionStatistics;
      if (txs == null) {
         if (log.isDebugEnabled()) {
            log.debug("Trying to invoke terminate() but no transaction is associated to the thread");
         }
         return;
      }
      long init = System.nanoTime();
      txs.terminateTransaction();

      NodeScopeStatisticCollector dest = transactionalClassesStatsMap.get(txs.getTransactionalClass());
      if (dest != null) {
         dest.merge(txs);
      } else {
         log.debug("Statistics not merged for transaction class not found on transactionalClassStatsMap");
      }
      thread.remove();
      TransactionTS lastTS = lastTransactionTS.get();
      if (lastTS != null)
         lastTS.setEndLastTxTs(System.nanoTime());
      TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(ExposedStatistic.TERMINATION_COST, System.nanoTime() - init, txs instanceof LocalTransactionStatistics);
   }

   public static Object getAttribute(ExposedStatistic param, String className) {
      if (className == null) {
         return transactionalClassesStatsMap.get(DEFAULT_ISPN_CLASS).getAttribute(param);
      } else {
         if (transactionalClassesStatsMap.get(className) != null)
            return transactionalClassesStatsMap.get(className).getAttribute(param);
         else
            return null;
      }
   }

   public static Object getPercentile(ExposedStatistic param, int percentile, String className) {
      if (configuration == null) {
         return null;
      }
      if (className == null) {
         return transactionalClassesStatsMap.get(DEFAULT_ISPN_CLASS).getPercentile(param, percentile);
      } else {
         if (transactionalClassesStatsMap.get(className) != null)
            return transactionalClassesStatsMap.get(className).getPercentile(param, percentile);
         else
            return null;
      }
   }

   public static Object getAttribute(ExposedStatistic param) {
      Object ret = transactionalClassesStatsMap.get(DEFAULT_ISPN_CLASS).getAttribute(param);
      if (log.isTraceEnabled()) {
         log.tracef("Attribute %s == %s", param, ret);
      }
      return ret;
   }

   public static void flushPendingRemoteLocksIfNeeded(GlobalTransaction id) {
      if (!active) {
         return;
      }
      if (pendingLocks.containsKey(id.globalId())) {
         if (log.isTraceEnabled())
            log.trace("Going to flush locks for " + (id.isRemote() ? "local " : "remote ") + "xact " + id.getId());
         immediateRemoteLockingTimeSampling(pendingLocks.get(id.globalId()));
         pendingLocks.remove(id.globalId());
      }
   }

   //This is synchronized because depending on the local/remote nature, a different object is created
   //Now, remote transactionStatistics get initialized at InboundInvocationHandler level
   public static TransactionStatistics initTransactionIfNecessary(TxInvocationContext tctx) {
      if (!active) {
         return null;
      }
      boolean isLocal = tctx.isOriginLocal();
      if (isLocal) {
         return initLocalTransaction();
      }
      return thread.get();
   }

   public static TransactionStatistics attachRemoteTransactionStatistic(GlobalTransaction globalTransaction, boolean createIfAbsent) {
      if (!active) {
         return null;
      }
      RemoteTransactionStatistics rts = remoteTransactionStatistics.get(globalTransaction);
      if (rts == null && createIfAbsent && configuration != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Create a new remote transaction statistic for transaction %s", globalTransaction.globalId());
         }
         rts = new RemoteTransactionStatistics(configuration);
         remoteTransactionStatistics.put(globalTransaction, rts);
         final String transactionClass = globalTransaction.getTransactionClass();
         if (transactionClass != null) {
            rts.setTransactionalClass(transactionClass);
            registerTransactionalClassIfNeeded(transactionClass);
         }
      } else if (configuration == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Trying to create a remote transaction statistics in a not initialized Transaction Statistics Registry");
         }
         return rts;
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Using the remote transaction statistic %s for transaction %s", rts, globalTransaction.globalId());
         }
      }
      thread.set(rts);
      return rts;
   }

   public static void detachRemoteTransactionStatistic(GlobalTransaction globalTransaction, boolean finished) {
      if (!active || thread.get() == null) {
         return;
      }
      //DIE: is finished == true either both for CommitCommand and RollbackCommand?
      if (finished) {
         if (log.isTraceEnabled()) {
            log.tracef("Detach remote transaction statistic and finish transaction %s", globalTransaction.globalId());
         }
         terminateTransaction(null);
         remoteTransactionStatistics.remove(globalTransaction);
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Detach remote transaction statistic for transaction %s", globalTransaction.globalId());
         }
         thread.remove();
      }
   }

   public static void reset() {
      for (NodeScopeStatisticCollector nsc : transactionalClassesStatsMap.values()) {
         nsc.reset();
      }
   }

   public static void setTransactionalClass(String className, TransactionStatistics transactionStatistics) {
      if (!active) {
         return;
      }
      TransactionStatistics txs = transactionStatistics == null ? thread.get() : transactionStatistics;
      if (txs == null) {
         log.debug("Trying to invoke setUpdateTransaction() but no transaction is associated to the thread");
         return;
      }
      txs.setTransactionalClass(className);
      registerTransactionalClassIfNeeded(className);
   }

   public static boolean isGmuWaitingActive() {
      return gmuWaitingActive && active;
   }

   public static void setGmuWaitingActive(boolean enabled) {
      gmuWaitingActive = enabled;
   }

   private static void appendLocks(Map<Object, Long> locks, String id) {
      if (locks != null && !locks.isEmpty()) {
         //log.trace("Appending locks for " + id);
         pendingLocks.put(id, locks);
      }
   }

   private static long computeCumulativeLockHoldTime(Map<Object, Long> takenLocks, int numLocks, long currentTime) {
      long ret = numLocks * currentTime;
      Set<Map.Entry<Object, Long>> keySet = takenLocks.entrySet();
      for (Map.Entry<Object, Long> e : keySet)
         ret -= takenLocks.get(e.getValue());
      return ret;
   }

   /**
    * NB: I assume here that *only* remote transactions can have pending locks
    */
   private static void immediateRemoteLockingTimeSampling(Map<Object, Long> locks) {
      int size = locks.size();
      double cumulativeLockHoldTime = computeCumulativeLockHoldTime(locks, size, System.nanoTime());
      addValueAndFlushIfNeeded(ExposedStatistic.LOCK_HOLD_TIME, cumulativeLockHoldTime, false);
      addValueAndFlushIfNeeded(ExposedStatistic.NUM_HELD_LOCKS, size, false);
   }

   private static void registerTransactionalClassIfNeeded(String className) {
      transactionalClassesStatsMap.putIfAbsent(className, new NodeScopeStatisticCollector());
   }

   private static TransactionStatistics initLocalTransaction() {
      //Not overriding the InitialValue method leads me to have "null" at the first invocation of get()
      TransactionStatistics lts = thread.get();
      if (lts == null && configuration != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Init a new local transaction statistics");
         }
         lts = new LocalTransactionStatistics(configuration);
         thread.set(lts);
         //Here only when transaction starts
         TransactionTS lastTS = lastTransactionTS.get();
         if (lastTS == null) {
            if (log.isTraceEnabled())
               log.tracef("Init a new local transaction statistics for Timestamp");
            lastTransactionTS.set(new TransactionTS());
         } else {
            lts.addValue(ExposedStatistic.NTBC_EXECUTION_TIME, System.nanoTime() - lastTS.getEndLastTxTs());
            lts.incrementValue(ExposedStatistic.NTBC_COUNT);
         }
      } else if (configuration == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Trying to create a local transaction statistics in a not initialized Transaction Statistics Registry");
         }
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Local transaction statistic is already initialized: %s", lts);
         }
      }
      return lts;
   }


}
