
package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.stressors.TpccPopulationStressor;

import java.util.List;

/**
 * This stage shuld be run before the <b>TpccBenchmarkStage</b>. It will perform the population of
 * <b>numWarehouses</b> warehouses in cache. Note: this stage won't clear the added data from
 * slave.
 * <pre>
 * Params:
 *       - numWarehouses : the number of warehouses to be populated.
 *       - cLastMask : the mask used to generate non-uniformly distributed random customer last names.
 *       - olIdMask : mask used to generate non-uniformly distributed random item numbers.
 *       - cIdMask : mask used to generate non-uniformly distributed random customer numbers.
 *       - threadParallelLoad: enable/disable the parallelLoading
 *       - numLoaderThreads: the number of populating threads per node 
 *       - batchLevel: the size of a transaction in population (i.e., the number of items per transaction) 
 * </pre>
 *
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Diego Didona, didona@gsd.inesc-id.pt
 * @author Pedro Ruivo
 */
public class TpccPopulationStage extends AbstractDistStage{

   /**
    * number of Warehouses
    */
   private int numWarehouses = 1;

   /**
    * mask used to generate non-uniformly distributed random customer last names
    */
   private long cLastMask = 255;

   /**
    * mask used to generate non-uniformly distributed random item numbers
    */
   private long olIdMask = 8191;

   /**
    * mask used to generate non-uniformly distributed random customer numbers
    */
   private long cIdMask = 1023;

   /**
    * enable/disable the parallelLoading 
    */
   private boolean threadParallelLoad = false;

   /**
    * the number of populating threads per node 
    */
   private int numLoaderThreads = 4;

   /**
    * the size of a transaction in population (i.e., the number of item per transaction) 
    */
   private int batchLevel = 100;

   /**
    * if true, it means that the cache was already preloaded from a DataBase. So no population is needed
    */
   private boolean preloadedFromDB = false;

   public DistStageAck executeOnSlave() {
      DefaultDistStageAck ack = newDefaultStageAck();
      CacheWrapper wrapper = slaveState.getCacheWrapper();
      if (wrapper == null) {
         log.info("Not executing any test as the wrapper is not set up on this slave ");
         return ack;
      }
      long startTime = System.currentTimeMillis();
      populate(wrapper);
      long duration = System.currentTimeMillis() - startTime;
      log.info("The population took: " + (duration / 1000) + " seconds.");
      ack.setPayload(duration);
      return ack;
   }

   private void populate(CacheWrapper wrapper) {
      TpccPopulationStressor populationStressor = new TpccPopulationStressor();
      populationStressor.setNumWarehouses(numWarehouses);
      populationStressor.setSlaveIndex(wrapper.getMyNode());
      populationStressor.setNumSlaves(getActiveSlaveCount());
      populationStressor.setCLastMask(this.cLastMask);
      populationStressor.setOlIdMask(this.olIdMask);
      populationStressor.setCIdMask(this.cIdMask);
      populationStressor.setThreadParallelLoad(threadParallelLoad);
      populationStressor.setNumLoadersThread(numLoaderThreads);
      populationStressor.setBatchLevel(batchLevel);
      populationStressor.setPreloadedFromDB(preloadedFromDB);
      populationStressor.stress(wrapper);
   }

   public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
      logDurationInfo(acks);
      for (DistStageAck ack : acks) {
         DefaultDistStageAck dAck = (DefaultDistStageAck) ack;
         if (log.isTraceEnabled()) {
            log.trace("Tpcc population on slave " + dAck.getSlaveIndex() + " finished in " + dAck.getPayload() + " millis.");
         }
      }
      return true;
   }

   public void setNumWarehouses(int numWarehouses) {
      this.numWarehouses = numWarehouses;
   }

   public void setCLastMask(long cLastMask) {
      this.cLastMask = cLastMask;
   }

   public void setOlIdMask(long olIdMask) {
      this.olIdMask = olIdMask;
   }

   public void setCIdMask(long cIdMask) {
      this.cIdMask = cIdMask;
   }

   public void setThreadParallelLoad(boolean threadParallelLoad) {
      this.threadParallelLoad = threadParallelLoad;
   }

   public void setNumLoaderThreads(int numLoaderThreads) {
      this.numLoaderThreads = numLoaderThreads;
   }

   public void setBatchLevel(int batchLevel) {
      this.batchLevel = batchLevel;
   }

   public void setPreloadedFromDB(boolean preloadedFromDB) {
      this.preloadedFromDB = preloadedFromDB;
   }

   @Override
   public String toString() {
      return "TpccPopulationStage {" +
            "numWarehouses=" + numWarehouses +
            ", cLastMask=" + cLastMask +
            ", olIdMask=" + olIdMask +
            ", cIdMask=" + cIdMask +
            ", threadParallelLoad=" + threadParallelLoad +
            ", numLoaderThreads=" + numLoaderThreads +
            ", batchLevel=" + batchLevel +
            ", preloadedFromDB=" + preloadedFromDB +
            ", " + super.toString();
   }
}
