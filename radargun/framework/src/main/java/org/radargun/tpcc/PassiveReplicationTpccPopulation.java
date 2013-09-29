package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.utils.ThreadTpccToolsManager;

/**
 * This population is used when passive replication is enabled. Only the primary can perform the population.
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class PassiveReplicationTpccPopulation extends ThreadParallelTpccPopulation {

   private static Log log = LogFactory.getLog(PassiveReplicationTpccPopulation.class);


   public PassiveReplicationTpccPopulation(CacheWrapper wrapper, int numWarehouses, int slaveIndex, int numSlaves,
                                           long cLastMask, long olIdMask, long cIdMask, int parallelThreads,
                                           int elementsPerBlock) {
      super(wrapper, numWarehouses, slaveIndex, numSlaves, cLastMask, olIdMask, cIdMask, parallelThreads, 
            elementsPerBlock, false, new ThreadTpccToolsManager(System.nanoTime()));
   }

   @Override
   public void performPopulation() {
      if (wrapper.isTheMaster()) {
         log.info("I am the primary and I am going to perform the population");
         super.performPopulation();
      } else {
         initTpccTools();
         log.info("I am not allowed to perform the population.");
      }
   }

   @Override
   protected void initializeToolsParameters() {
      initTpccTools();

      long c_c_last = tpccTools.get().randomNumber(0, TpccTools.A_C_LAST);
      long c_c_id = tpccTools.get().randomNumber(0, TpccTools.A_C_ID);
      long c_ol_i_id = tpccTools.get().randomNumber(0, TpccTools.A_OL_I_ID);

      boolean successful = false;
      do {
         try {
            wrapper.put(null, "C_C_LAST", c_c_last);
            successful = true;
         } catch (Throwable e) {
            log.warn(e);
         }
      } while (!successful);

      successful = false;
      do {
         try {
            wrapper.put(null, "C_C_ID", c_c_id);
            successful = true;
         } catch (Throwable e) {
            log.warn(e);
         }
      } while (!successful);

      successful = false;
      do {
         try {
            wrapper.put(null, "C_OL_ID", c_ol_i_id);
            successful = true;
         } catch (Throwable e) {
            log.warn(e);
         }
      } while (!successful);
   }

   @Override
   protected void populateWarehouses() {
      log.trace("Populate warehouses");

      for (int warehouseId = 1; warehouseId <= this.numWarehouses; warehouseId++) {
         log.info("Populate Warehouse " + warehouseId);

         txAwarePut(createWarehouse(warehouseId));

         populateStock(warehouseId);

         populateDistricts(warehouseId);

         printMemoryInfo();
      }
   }

   @Override
   protected void populateDistricts(int warehouseId) {
      if (warehouseId < 0) {
         log.warn("Trying to populate Districts for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating District for warehouse " + warehouseId);

      logDistrictPopulation(warehouseId, 1, TpccTools.NB_MAX_DISTRICT);
      for (int districtId = 1; districtId <= TpccTools.NB_MAX_DISTRICT; districtId++) {
         txAwarePut(createDistrict(districtId, warehouseId));

         populateCustomers(warehouseId, districtId);

         populateOrders(warehouseId, districtId);
      }
   }

   @Override
   protected void populateItem() {
      log.trace("Populating Items");

      performMultiThreadPopulation(1, TpccTools.NB_MAX_ITEM, new ThreadCreator() {
         @Override
         public Thread createThread(int threadIdx, long lowerBound, long upperBound) {
            return new PopulateItemThread(threadIdx, lowerBound, upperBound);
         }
      });
   }

   @Override
   protected void populateStock(final int warehouseId) {
      if (warehouseId < 0) {
         log.warn("Trying to populate Stock for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating Stock for warehouse " + warehouseId);

      performMultiThreadPopulation(1, TpccTools.NB_MAX_ITEM, new ThreadCreator() {
         @Override
         public Thread createThread(int threadIdx, long lowerBound, long upperBound) {
            return new PopulateStockThread(threadIdx, lowerBound, upperBound, warehouseId);
         }
      });
   }
}
