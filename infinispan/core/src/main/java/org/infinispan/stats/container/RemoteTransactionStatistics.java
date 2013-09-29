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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.stats.ExposedStatistic.NO_INDEX;
import static org.infinispan.stats.ExposedStatistic.getRemoteStatsSize;

/**
 * Websiste: www.cloudtm.eu Date: 20/04/12
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public class RemoteTransactionStatistics extends TransactionStatistics {

   private static final Log log = LogFactory.getLog(RemoteTransactionStatistics.class);
   private static final int SIZE = getRemoteStatsSize();

   public RemoteTransactionStatistics(Configuration configuration) {
      super(SIZE, configuration);
   }

   @Override
   public final void onPrepareCommand() {
      //nop
   }

   @Override
   public final boolean isLocal() {
      return false;
   }

   @Override
   public final String toString() {
      return "RemoteTransactionStatistics{" + super.toString();
   }

   @Override
   public final boolean stillLocalExecution() {
      return false;
   }

   protected void immediateLockingTimeSampling(int heldLocks, boolean isCommit) {
      double cumulativeLockHoldTime = this.computeCumulativeLockHoldTime(heldLocks, System.nanoTime());
      this.addValue(ExposedStatistic.NUM_HELD_LOCKS, heldLocks);
      this.addValue(ExposedStatistic.LOCK_HOLD_TIME, cumulativeLockHoldTime);
      ExposedStatistic counter, type;
      if (isCommit) {
         counter = ExposedStatistic.NUM_SUX_LOCKS;
         type = ExposedStatistic.SUX_LOCK_HOLD_TIME;
      } else {
         counter = ExposedStatistic.NUM_REMOTE_ABORTED_LOCKS;
         type = ExposedStatistic.REMOTE_ABORT_LOCK_HOLD_TIME;

      }
      addValue(counter, heldLocks);
      addValue(type, cumulativeLockHoldTime);
   }

   @Override
   protected final void terminate() {
      //nop
   }

   @Override
   protected final Log getLog() {
      return log;
   }

   protected final int getIndex(ExposedStatistic stat) throws NoIspnStatException {
      int ret = stat.getRemoteIndex();
      if (ret == NO_INDEX) {
         throw new NoIspnStatException("Statistic " + stat + " is not available!");
      }
      return ret;
   }
}
