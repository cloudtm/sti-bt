package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;

/**
 * This stage reset the stats in this cache wrapper on all slaves
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class ResetStatsStage extends AbstractDistStage {

   @Override
   public DistStageAck executeOnSlave() {
      DefaultDistStageAck defaultDistStageAck = newDefaultStageAck();
      CacheWrapper cacheWrapper = slaveState.getCacheWrapper();

      if (cacheWrapper == null) {
         log.info("Not resetting stats on this slave as the wrapper hasn't been configured.");
         return defaultDistStageAck;
      }

      long start = System.currentTimeMillis();
      cacheWrapper.resetAdditionalStats();
      long duration = System.currentTimeMillis() - start;
      defaultDistStageAck.setDuration(duration);
      return defaultDistStageAck;
   }

   @Override
   public String toString() {
      return "ResetStatsStage{" + super.toString();
   }
}
