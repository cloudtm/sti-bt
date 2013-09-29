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
package org.infinispan.stats.container;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.stats.ExposedStatistic;
import org.infinispan.stats.InfinispanStat;
import org.infinispan.stats.LockRelatedStatsHelper;
import org.infinispan.stats.NoIspnStatException;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.infinispan.stats.ExposedStatistic.*;


/**
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class TransactionStatistics implements InfinispanStat {

   private final StatisticsContainer statisticsContainer;
   //Here the elements which are common for local and remote transactions
   protected long initTime;
   protected long initCpuTime;
   protected long endLocalTime;
   protected long endLocalCpuTime;
   protected Configuration configuration;
   protected String id;
   private boolean isReadOnly;
   private boolean isCommit;
   private String transactionalClass;
   private Map<Object, Long> takenLocks = new HashMap<Object, Long>();
   private long lastOpTimestamp;
   private long performedReads;
   protected long readsBeforeFirstWrite = -1;
   private boolean prepareSent = false;


   public void setReadsBeforeFirstWrite() {
      //I do not use isReadOnly just in case, in the future, we'll be able to tag as update a xact upon start
      if (readsBeforeFirstWrite == -1)
         this.readsBeforeFirstWrite = performedReads;
   }

   public long getReadsBeforeFirstWrite() {
      return readsBeforeFirstWrite;
   }

   public boolean isPrepareSent() {
      return prepareSent;
   }

   public void markPrepareSent() {
      this.prepareSent = true;
   }

   public void notifyRead() {
      performedReads++;
   }

   public TransactionStatistics(int size, Configuration configuration) {
      this.initTime = System.nanoTime();
      this.isReadOnly = true; //as far as it does not tries to perform a put operation
      this.takenLocks = new HashMap<Object, Long>();
      this.transactionalClass = TransactionsStatisticsRegistry.DEFAULT_ISPN_CLASS;
      this.statisticsContainer = new StatisticsContainerImpl(size);
      this.configuration = configuration;
      if (getLog().isTraceEnabled()) {
         getLog().tracef("Created transaction statistics. Class is %s. Start time is %s",
                         transactionalClass, initTime);
      }
      if (TransactionsStatisticsRegistry.isSampleServiceTime()) {
         getLog().tracef("Transaction statistics is sampling cpuTime");
         this.initCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
      }
   }

   public final Map<Object, Long> getTakenLocks() {
      return takenLocks;
   }

   public final void attachId(GlobalTransaction id) {
      this.id = id.globalId();
   }

   public final String getId() {
      return id;
   }

   public final String getTransactionalClass() {
      return this.transactionalClass;
   }

   public final void setTransactionalClass(String className) {
      this.transactionalClass = className;
   }

   public final boolean isCommit() {
      return this.isCommit;
   }

   public final void setTransactionOutcome(boolean commit) {
      isCommit = commit;
   }

   public final boolean isReadOnly() {
      return this.isReadOnly;
   }

   public final void setUpdateTransaction() {
      this.isReadOnly = false;
   }

   /*TODO: in 2PL it does not really matter *which* are the taken locks if, in the end, we are going to take and average holdTime So we could just use an array of long, without storing the lock itself
     TODO: I could need the actual lock just to be sure it has not already been locked, but I can easily use a hash function for that maybe
  */
   public final void addTakenLock(Object lock) {
      if (!this.takenLocks.containsKey(lock)) {
         long now = System.nanoTime();
         this.takenLocks.put(lock, now);
         if (getLog().isTraceEnabled())
            getLog().trace("TID " + Thread.currentThread().getId() + " Added " + lock + " at " + now);
      }
   }

   public final void addValue(ExposedStatistic param, double value) {
      try {
         int index = this.getIndex(param);
         this.statisticsContainer.addValue(index, value);
         if (getLog().isTraceEnabled()) {
            getLog().tracef("Add %s to %s", value, param);
         }
      } catch (NoIspnStatException e) {
         getLog().warnf(e, "Exception caught when trying to add the value %s to %s.", value, param);
      }
   }

   public final long getValue(ExposedStatistic param) {
      int index = this.getIndex(param);
      long value = this.statisticsContainer.getValue(index);
      if (getLog().isTraceEnabled()) {
         getLog().tracef("Value of %s is %s", param, value);
      }
      return value;
   }

   public final void incrementValue(ExposedStatistic param) {
      this.addValue(param, 1);
   }

   /**
    * Finalizes statistics of the transaction. This is either called at prepare(1PC)/commit/rollbackCommand visit time
    * for local transactions or upon invocation from InboundInvocationHandler for remote xact
    * (commit/rollback/txCompletionNotification). The remote xact stats container is first "attached" to the thread
    * running the remote transaction and then terminateTransaction is invoked to sample statistics.
    */
   public final void terminateTransaction() {
      if (getLog().isTraceEnabled()) {
         getLog().tracef("Terminating transaction. Is read only? %s. Is commit? %s", isReadOnly, isCommit);
      }

       /*
         In case of aborts, locks are *always* released after receiving a RollbackCommand from the coordinator (even if the acquisition fails during the prepare phase)
         This is good, since end of transaction and release of the locks coincide.
         In case of commit we have two cases:
            In some cases we have that the end of xact and release of the locks coincide
            In another case we have that the end of the xact and the release of the locks don't coincide, and the lock holding time has to be "injected" upon release
            this is the case of GMU with commit async, for example, or if locks are actually released upon receiving the TxCompletionNotificationCommand
         */
      int heldLocks = this.takenLocks.size();
      if (heldLocks > 0) {
         boolean remote = !(this instanceof LocalTransactionStatistics);
         if (!LockRelatedStatsHelper.shouldAppendLocks(configuration, isCommit, remote)) {
            if (getLog().isTraceEnabled())
               getLog().trace("TID " + Thread.currentThread().getId() + "Sampling locks for " + (remote ? "remote " : "local ") + " transaction " + this.id + " commit? " + isCommit);
            immediateLockingTimeSampling(heldLocks, isCommit);
         } else {
            if (getLog().isTraceEnabled())
               getLog().trace("NOT sampling locks for " + (remote ? "remote " : "local ") + " transaction " + this.id);
         }
      }

      double execTime = System.nanoTime() - this.initTime;
      if (this.isReadOnly) {
         if (isCommit) {
            this.incrementValue(NUM_COMMITTED_RO_TX);
            this.addValue(RO_TX_SUCCESSFUL_EXECUTION_TIME, execTime);
            this.addValue(NUM_SUCCESSFUL_GETS_RO_TX, this.getValue(NUM_GET));
            this.addValue(NUM_SUCCESSFUL_REMOTE_GETS_RO_TX, this.getValue(NUM_LOCAL_REMOTE_GET));
         } else {
            this.incrementValue(NUM_ABORTED_RO_TX);
            this.addValue(RO_TX_ABORTED_EXECUTION_TIME, execTime);
         }
      } else {
         if (isCommit) {
            this.incrementValue(NUM_COMMITTED_WR_TX);
            this.addValue(WR_TX_SUCCESSFUL_EXECUTION_TIME, execTime);
            this.addValue(NUM_SUCCESSFUL_GETS_WR_TX, this.getValue(NUM_GET));
            this.addValue(NUM_SUCCESSFUL_REMOTE_GETS_WR_TX, this.getValue(NUM_LOCAL_REMOTE_GET));
            this.addValue(NUM_SUCCESSFUL_PUTS_WR_TX, this.getValue(NUM_PUT));
            this.addValue(NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX, this.getValue(NUM_REMOTE_PUT));
         } else {
            this.incrementValue(NUM_ABORTED_WR_TX);
            this.addValue(WR_TX_ABORTED_EXECUTION_TIME, execTime);
         }
      }
      terminate();
   }

   public abstract boolean stillLocalExecution();

   protected abstract void immediateLockingTimeSampling(int heldLocks, boolean isCommit);

   public final void flush(ConcurrentGlobalContainer globalStatistics) {
      if (getLog().isTraceEnabled()) {
         getLog().tracef("Flush this [%s] to %s", this, globalStatistics);
      }
      this.statisticsContainer.mergeTo(globalStatistics, isLocal());
   }

   public final void dump() {
      this.statisticsContainer.dump();
   }

   @Override
   public String toString() {
      return "initTime=" + initTime +
            ", isReadOnly=" + isReadOnly +
            ", isCommit=" + isCommit +
            ", transactionalClass=" + transactionalClass +
            '}';
   }

   public abstract void onPrepareCommand();

   public final long getLastOpTimestamp() {
      return lastOpTimestamp;
   }

   public final void setLastOpTimestamp(long lastOpTimestamp) {
      this.lastOpTimestamp = lastOpTimestamp;
   }

   public final void addNTBCValue(long currentTime) {
      if (getLastOpTimestamp() != 0) {
         addValue(TBC_EXECUTION_TIME, currentTime - getLastOpTimestamp());
      }
   }

   public abstract boolean isLocal();

   protected final void immediateLockingTimeSampling(int heldLocks) {
      double cumulativeLockHoldTime = this.computeCumulativeLockHoldTime(heldLocks, System.nanoTime());
      this.addValue(NUM_HELD_LOCKS, heldLocks);
      this.addValue(LOCK_HOLD_TIME, cumulativeLockHoldTime);
   }

   protected abstract int getIndex(ExposedStatistic param);

   protected abstract void terminate();

   protected abstract Log getLog();

   /*
   private void appendLocks(Map<Object,Long> locks, Long id){
      TransactionsStatisticsRegistry.shouldAppendLocks(locks,id);
   }
   */
   /*
     Upon completion of a xact (i.e., CommitCommand or RollbackCommand) I know that I have released the locks if
     a. I am a local xact
     b. I am a remote xact and
     b.1. It is a RollbackCommand   OR
     b.2  We are not running GMU protocol   OR
     b.3  It is a CommitCommand and it is synchronous
    */
   //I have to save locks if I am remote committing with GMU (in async mode!!)
   private boolean haveLocksAlreadyBeenReleased(boolean isCommit, Configuration configuration) {
      boolean isGmu = configuration.versioning().scheme().equals(VersioningScheme.GMU);
      boolean isSyncCommit = configuration.transaction().syncCommitPhase();
      return !(this instanceof RemoteTransactionStatistics && isGmu && isCommit && !isSyncCommit);

      // boolean isSyncCommit = configuration.transaction().syncCommitPhase();
      // return ( this instanceof LocalTransactionStatistics) || !isCommit || !isGmu || isSyncCommit;          //Either is a RollbackCommand, or we are not using GMU, or we are using GMU and the commitCommand is sync
   }

   protected long computeCumulativeLockHoldTime(int numLocks, long currentTime) {
      Set<Map.Entry<Object, Long>> keySet = this.takenLocks.entrySet();
      final boolean trace = (getLog().isTraceEnabled());
      if (trace)
         getLog().trace("Held locks from param " + numLocks + "numLocks in entryset " + keySet.size());      long ret = numLocks * currentTime;
      if (trace)
         getLog().trace("Now is " + currentTime + "total is " + ret);
      for (Map.Entry<Object, Long> e : keySet) {
         ret -= e.getValue();
         if (trace)
            getLog().trace("TID " + Thread.currentThread().getId() + " " + e.getKey() + " " + e.getValue());
      }
      if (trace)
         getLog().trace("TID " + Thread.currentThread().getId() + " " + "Avg lock hold time is " + (ret / (long) numLocks) * 1e-3);
      return ret;
   }
}

