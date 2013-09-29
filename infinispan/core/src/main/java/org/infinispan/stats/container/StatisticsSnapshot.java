package org.infinispan.stats.container;

import org.infinispan.stats.ExposedStatistic;

/**
 * A Statistic Snapshot;
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StatisticsSnapshot {

   private final long[] snapshot;

   public StatisticsSnapshot(long[] snapshot) {
      this.snapshot = snapshot;
   }

   public final long getRemote(ExposedStatistic stat) {
      return snapshot[ConcurrentGlobalContainer.getRemoteIndex(stat)];
   }

   public final long getLocal(ExposedStatistic stat) {
      return snapshot[ConcurrentGlobalContainer.getLocalIndex(stat)];
   }

   public final long getLastResetTime() {
      return snapshot[0];
   }
}
