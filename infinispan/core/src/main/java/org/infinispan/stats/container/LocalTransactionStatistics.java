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
import org.infinispan.stats.ExposedStatistic;
import org.infinispan.stats.NoIspnStatException;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.stats.ExposedStatistic.*;

/**
 * Websiste: www.cloudtm.eu Date: 20/04/12
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class LocalTransactionStatistics extends TransactionStatistics {

   private static final Log log = LogFactory.getLog(LocalTransactionStatistics.class);
   private static final int SIZE = getLocalStatsSize();
   private boolean stillLocalExecution;

   public LocalTransactionStatistics(Configuration configuration) {
      super(SIZE, configuration);
      this.stillLocalExecution = true;
   }

   @Override
   public final void onPrepareCommand() {
      this.terminateLocalExecution();
   }

   @Override
   public final boolean isLocal() {
      return true;
   }

   @Override
   public final String toString() {
      return "LocalTransactionStatistics{" +
            "stillLocalExecution=" + stillLocalExecution +
            ", " + super.toString();
   }

   /*
   I take local execution times only if the xact commits, otherwise if I have some kind of transactions which remotely abort more frequently,
   they will bias the accuracy of the statistics, just because they are re-run more often!
    */
   @Override
   protected final void terminate() {
      if (!isCommit())
         return;
      final boolean sampleServiceTime = TransactionsStatisticsRegistry.isSampleServiceTime();
      long cpuTime = sampleServiceTime ? TransactionsStatisticsRegistry.getThreadCPUTime() : 0;
      long now = System.nanoTime();
      if (!isReadOnly()) {
         long numPuts = this.getValue(NUM_PUT);
         this.addValue(FIRST_WRITE_INDEX, this.readsBeforeFirstWrite);
         this.addValue(NUM_SUCCESSFUL_PUTS, numPuts);
         this.addValue(NUM_HELD_LOCKS_SUCCESS_TX, getValue(NUM_HELD_LOCKS));
         this.addValue(UPDATE_TX_LOCAL_R, this.endLocalTime - this.initTime);
         this.addValue(UPDATE_TX_TOTAL_R, now - this.initTime);
         if (sampleServiceTime) {
            addValue(UPDATE_TX_LOCAL_S, this.endLocalCpuTime - this.initCpuTime);
            addValue(UPDATE_TX_TOTAL_S, cpuTime - this.initCpuTime);
         }
         if (configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC) {
            this.addValue(LOCAL_EXEC_NO_CONT, this.getValue(UPDATE_TX_LOCAL_R));
         } else {
            long localLockAcquisitionTime = getValue(LOCK_WAITING_TIME);
            long totalLocalDuration = this.getValue(UPDATE_TX_LOCAL_R);
            this.addValue(LOCAL_EXEC_NO_CONT, (totalLocalDuration - localLockAcquisitionTime));
         }
      } else {
         addValue(READ_ONLY_TX_TOTAL_R, now - initTime);
         if (sampleServiceTime)
            addValue(READ_ONLY_TX_TOTAL_S, cpuTime - initCpuTime);
      }
   }

   @Override
   protected final Log getLog() {
      return log;
   }

   protected final int getIndex(ExposedStatistic stat) throws NoIspnStatException {
      int ret = stat.getLocalIndex();
      if (ret == NO_INDEX) {
         throw new NoIspnStatException("ExposedStatistic " + stat + " not found!");
      }
      return ret;
   }

   private void terminateLocalExecution() {
      this.stillLocalExecution = false;
      final boolean sampleServiceTime = TransactionsStatisticsRegistry.isSampleServiceTime();
      long cpuTime = sampleServiceTime ? TransactionsStatisticsRegistry.getThreadCPUTime() : 0;
      long now = System.nanoTime();
      this.endLocalCpuTime = cpuTime;
      this.endLocalTime = now;
      if (!isReadOnly()) {
         incrementValue(NUM_UPDATE_TX_GOT_TO_PREPARE);
      }
      //RO can never abort :)
      else {
         addValue(READ_ONLY_TX_LOCAL_R, now - this.initTime);
         if (sampleServiceTime) {
            addValue(READ_ONLY_TX_LOCAL_S, cpuTime - this.initCpuTime);
            //I do not update the number of prepares, because no readOnly transaction fails
         }
      }
      this.incrementValue(NUM_PREPARES);
   }

   @Override
   public final boolean stillLocalExecution() {
      return stillLocalExecution;
   }

   protected void immediateLockingTimeSampling(int heldLocks, boolean isCommit) {
      double cumulativeLockHoldTime = this.computeCumulativeLockHoldTime(heldLocks, System.nanoTime());
      this.addValue(NUM_HELD_LOCKS, heldLocks);
      this.addValue(LOCK_HOLD_TIME, cumulativeLockHoldTime);
      ExposedStatistic counter, type;
      if (isCommit) {
         counter = NUM_SUX_LOCKS;
         type = SUX_LOCK_HOLD_TIME;
      } else {
         if (!isPrepareSent()) {
            counter = NUM_LOCAL_ABORTED_LOCKS;
            type = LOCAL_ABORT_LOCK_HOLD_TIME;
         } else {
            counter = NUM_REMOTE_ABORTED_LOCKS;
            type = REMOTE_ABORT_LOCK_HOLD_TIME;
         }
      }
      addValue(counter, heldLocks);
      addValue(type, cumulativeLockHoldTime);
   }
}
