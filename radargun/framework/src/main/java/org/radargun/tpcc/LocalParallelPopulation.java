package org.radargun.tpcc;

import org.radargun.CacheWrapper;
import org.radargun.utils.ThreadTpccToolsManager;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class LocalParallelPopulation extends ThreadParallelTpccPopulation {   
   
   public LocalParallelPopulation(CacheWrapper wrapper, int numWarehouses, int slaveIndex, long cLastMask, 
                                  long olIdMask, long cIdMask, int parallelThreads, int seed) {
      super(wrapper, numWarehouses, slaveIndex, 1, cLastMask, olIdMask, cIdMask, parallelThreads, 1,
            true, new ThreadTpccToolsManager(seed));
      
   }
         
}
