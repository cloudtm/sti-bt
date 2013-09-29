package org.radargun.utils;

import org.radargun.tpcc.TpccTools;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class ThreadTpccToolsManager {

   private final long initialSeed;
   private final long multiplier;
   private TpccTools[] threadRandom;

   public ThreadTpccToolsManager(long initialSeed) {
      this.initialSeed = initialSeed;
      multiplier = 3;
      threadRandom = new TpccTools[16];
   }

   public synchronized final TpccTools getTpccTools(int threadId) {
      if (threadId >= threadRandom.length) {
         TpccTools[] old = threadRandom;
         threadRandom = new TpccTools[threadId * 2];
         System.arraycopy(old, 0, threadRandom, 0, old.length);
      }
      if (threadRandom[threadId] == null) {
         long seed = initialSeed;
         for (int i = 0; i < threadId; ++i) {
            seed *= multiplier;
         }
         threadRandom[threadId] = TpccTools.newInstance(seed);
      }
      return threadRandom[threadId];
   }
}
