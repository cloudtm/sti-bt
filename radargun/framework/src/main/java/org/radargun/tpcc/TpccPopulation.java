package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.tpcc.domain.*;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Date;

import static org.radargun.utils.Utils.memString;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Diego Didona, didona@gsd.inesc-id.pt
 * @author Pedro Ruivo
 */
public class TpccPopulation {

   private static Log log = LogFactory.getLog(TpccPopulation.class);

   private long POP_C_LAST = TpccTools.NULL_NUMBER;

   private long POP_C_ID = TpccTools.NULL_NUMBER;

   private long POP_OL_I_ID = TpccTools.NULL_NUMBER;

   protected boolean _new_order = false;

   private final int _seqIdCustomer[];

   protected final CacheWrapper wrapper;

   private final MemoryMXBean memoryBean;

   protected final int numWarehouses;

   protected final int slaveIndex;
   protected final int numSlaves;

   protected final long cLastMask;
   protected final long olIdMask;
   protected final long cIdMask;

   protected final ThreadLocal<TpccTools> tpccTools;

   private final boolean populateLocalOnly;

   public TpccPopulation(CacheWrapper wrapper, int numWarehouses, int slaveIndex, int numSlaves, long cLastMask,
                         long olIdMask, long cIdMask) {
      this(wrapper, numWarehouses, slaveIndex, numSlaves, cLastMask, olIdMask, cIdMask, false);            
   }
   
   public TpccPopulation(CacheWrapper wrapper, int numWarehouses, int slaveIndex, int numSlaves, long cLastMask,
                         long olIdMask, long cIdMask, boolean populateLocalOnly) {
      this.wrapper = wrapper;
      this._seqIdCustomer = new int[TpccTools.NB_MAX_CUSTOMER];
      this.memoryBean = ManagementFactory.getMemoryMXBean();
      this.numWarehouses = numWarehouses;
      this.slaveIndex = slaveIndex;
      this.numSlaves = numSlaves;
      this.cLastMask = cLastMask;
      this.olIdMask = olIdMask;
      this.cIdMask = cIdMask;
      tpccTools = new ThreadLocal<TpccTools>();
      tpccTools.set(TpccTools.newInstance());
      this.populateLocalOnly = populateLocalOnly;
   }

   public final void initTpccTools() {
      TpccTools.NB_WAREHOUSES = this.numWarehouses;
      TpccTools.A_C_LAST = this.cLastMask;
      TpccTools.A_OL_I_ID = this.olIdMask;
      TpccTools.A_C_ID = this.cIdMask;
   }

   public void performPopulation(){
      initializeToolsParameters();

      populateItem();

      populateWarehouses();

      System.gc();
   }


   protected void initializeToolsParameters() {
      initTpccTools();

      if (this.slaveIndex == 0) {//Only one slave
         long c_c_last = tpccTools.get().randomNumber(0, TpccTools.A_C_LAST);
         long c_c_id = tpccTools.get().randomNumber(0, TpccTools.A_C_ID);
         long c_ol_i_id = tpccTools.get().randomNumber(0, TpccTools.A_OL_I_ID);

         boolean successful = false;
         while (!successful) {
            try {
               wrapper.put(null, "C_C_LAST", c_c_last);
               successful = true;
            } catch (Throwable e) {
               log.warn(e);
            }
         }

         successful = false;
         while (!successful) {
            try {
               wrapper.put(null, "C_C_ID", c_c_id);
               successful = true;
            } catch (Throwable e) {
               log.warn(e);
            }
         }

         successful = false;
         while (!successful) {
            try {
               wrapper.put(null, "C_OL_ID", c_ol_i_id);
               successful = true;
            } catch (Throwable e) {
               log.warn(e);
            }
         }

      }
   }

   public String c_last() {
      String c_last = "";
      long number = tpccTools.get().nonUniformRandom(getC_LAST(), TpccTools.A_C_LAST, TpccTools.MIN_C_LAST, TpccTools.MAX_C_LAST);
      String alea = String.valueOf(number);
      while (alea.length() < 3) {
         alea = "0" + alea;
      }
      for (int i = 0; i < 3; i++) {
         c_last += TpccTools.C_LAST[(Integer.parseInt(alea.substring(i, i + 1))) % TpccTools.C_LAST.length];
      }
      return c_last;
   }

   public long getC_LAST() {
      if (POP_C_LAST == TpccTools.NULL_NUMBER) {
         POP_C_LAST = tpccTools.get().randomNumber(TpccTools.MIN_C_LAST, TpccTools.A_C_LAST);
      }
      return POP_C_LAST;
   }

   public long getC_ID() {
      if (POP_C_ID == TpccTools.NULL_NUMBER) {
         POP_C_ID = tpccTools.get().randomNumber(0, TpccTools.A_C_ID);
      }
      return POP_C_ID;
   }

   public long getOL_I_ID() {
      if (POP_OL_I_ID == TpccTools.NULL_NUMBER) {
         POP_OL_I_ID = tpccTools.get().randomNumber(0, TpccTools.A_OL_I_ID);
      }
      return POP_OL_I_ID;
   }

   protected void populateItem() {
      log.trace("Populate Items");

      long init_id_item = 1;
      long num_of_items = TpccTools.NB_MAX_ITEM;

      if (numSlaves > 1) {
         num_of_items = TpccTools.NB_MAX_ITEM / numSlaves;
         long remainder = TpccTools.NB_MAX_ITEM % numSlaves;

         init_id_item = (slaveIndex * num_of_items) + 1;

         if (slaveIndex == numSlaves - 1) {
            num_of_items += remainder;
         }


      }
      logItemsPopulation(init_id_item, init_id_item - 1 + num_of_items);
      for (long itemId = init_id_item; itemId <= (init_id_item - 1 + num_of_items); itemId++) {
         txAwarePut(createItem(itemId));
      }
      printMemoryInfo();
   }

   protected void populateWarehouses() {
      log.trace("Populate warehouses");
      log.info("Populate Warehouse " + (slaveIndex + 1));
      txAwarePut(createWarehouse(slaveIndex + 1));
      populateStock(slaveIndex + 1);
      populateDistricts(slaveIndex + 1);

      printMemoryInfo();
   }

   protected void populateStock(int warehouseId) {
      if (warehouseId < 0) {
         log.warn("Trying to populate Stock for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating Stock for warehouse " + warehouseId);

      long init_id_item = 1;
      long num_of_items = TpccTools.NB_MAX_ITEM;

      logStockPopulation(warehouseId, init_id_item, init_id_item - 1 + num_of_items);
      for (long stockId = init_id_item; stockId <= (init_id_item - 1 + num_of_items); stockId++) {
         txAwarePut(createStock(stockId, warehouseId));
      }
   }

   protected void populateDistricts(int warehouseId) {
      if (warehouseId < 0) {
         log.warn("Trying to populate Districts for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating District for warehouse " + warehouseId);

      int init_districtId = 1;
      int num_of_districts = TpccTools.NB_MAX_DISTRICT;

      logDistrictPopulation(warehouseId, init_districtId, init_districtId - 1 + num_of_districts);
      for (int districtId = init_districtId; districtId <= (init_districtId - 1 + num_of_districts); districtId++) {
         txAwarePut(createDistrict(districtId, warehouseId));
         populateCustomers(warehouseId, districtId);
         populateOrders(warehouseId, districtId);
      }
   }

   protected void populateCustomers(int warehouseId, int districtId) {
      if (warehouseId < 0 || districtId < 0) {
         log.warn("Trying to populate Customer with a negative warehouse or district ID. skipping...");
         return;
      }

      logCustomerPopulation(warehouseId, districtId, 1, TpccTools.NB_MAX_CUSTOMER);
      for (int customerId = 1; customerId <= TpccTools.NB_MAX_CUSTOMER; customerId++) {
         String c_last = c_last();

         txAwarePut(createCustomer(warehouseId, districtId, customerId, c_last));

         CustomerLookup customerLookup = new CustomerLookup(c_last, warehouseId, districtId);
         txAwareLoad(customerLookup);

         customerLookup.addId(customerId);
         txAwarePut(customerLookup);

         populateHistory(customerId, warehouseId, districtId);
      }
   }

   protected void populateHistory(int customerId, int warehouseId, int districtId) {
      if (warehouseId < 0 || districtId < 0 || customerId < 0) {
         log.warn("Trying to populate Customer with a negative warehouse or district or customer ID. skipping...");
         return;
      }

      log.trace("Populating History for warehouse " + warehouseId + ", district " + districtId + " and customer " +
                      customerId);

      txAwarePut(createHistory(customerId, districtId, warehouseId));
   }


   protected void populateOrders(int warehouseId, int districtId) {
      if (warehouseId < 0 || districtId < 0) {
         log.warn("Trying to populate Order with a negative warehouse or district ID. skipping...");
         return;
      }

      logOrderPopulation(warehouseId, districtId, 1, TpccTools.NB_MAX_ORDER);
      this._new_order = false;
      for (int orderId = 1; orderId <= TpccTools.NB_MAX_ORDER; orderId++) {

         int o_ol_cnt = tpccTools.get().aleaNumber(5, 15);
         Date aDate = new Date((new java.util.Date()).getTime());

         txAwarePut(createOrder(orderId, districtId, warehouseId, aDate, o_ol_cnt,
                                generateSeqAlea(0, TpccTools.NB_MAX_CUSTOMER - 1)));

         populateOrderLines(warehouseId, districtId, orderId, o_ol_cnt, aDate);

         if (orderId >= TpccTools.LIMIT_ORDER) {
            populateNewOrder(warehouseId, districtId, orderId);
         }
      }
   }

   protected void populateOrderLines(int warehouseId, int districtId, int orderId, int o_ol_cnt, Date aDate) {
      if (warehouseId < 0 || districtId < 0) {
         log.warn("Trying to populate Order Lines with a negative warehouse or district ID. skipping...");
         return;
      }

      log.trace("Populating Orders Lines for warehouse " + warehouseId + ", district " + districtId + " and order " +
                      orderId);
      for (int orderLineId = 0; orderLineId < o_ol_cnt; orderLineId++) {

         double amount;
         Date delivery_date;

         if (orderId >= TpccTools.LIMIT_ORDER) {
            amount = tpccTools.get().aleaDouble(0.01, 9999.99, 2);
            delivery_date = null;
         } else {
            amount = 0.0;
            delivery_date = aDate;
         }

         txAwarePut(createOrderLine(orderId, districtId, warehouseId, orderLineId, delivery_date, amount));
      }
   }

   protected void populateNewOrder(int warehouseId, int districtId, int orderId) {
      if (warehouseId < 0 || districtId < 0) {
         log.warn("Trying to populate New Order with a negative warehouse or district ID. skipping...");
         return;
      }

      log.trace("Populating New Order for warehouse " + warehouseId + ", district " + districtId + " and order " +
                      orderId);

      txAwarePut(createNewOrder(orderId, districtId, warehouseId));
   }

   protected final boolean txAwarePut(DomainObject domainObject) {
      if (wrapper.isInTransaction()) {
         try {
            domainObject.storeToPopulate(wrapper, slaveIndex, populateLocalOnly);
         } catch (Throwable throwable) {
            return false;
         }
      } else {
         boolean putDone = false;
         do {
            try {
               domainObject.storeToPopulate(wrapper, slaveIndex, populateLocalOnly);
               putDone = true;
            } catch (Throwable e) {
               logErrorWhilePut(domainObject, e);
            }
         } while (!putDone);
      }
      return true;
   }

   protected final boolean txAwareLoad(DomainObject domainObject) {
      if (wrapper.isInTransaction()) {
         try {
            domainObject.load(wrapper, slaveIndex);
         } catch (Throwable throwable) {
            return false;
         }
      } else {
         boolean loadDone = false;
         do {
            try {
               domainObject.load(wrapper, slaveIndex);
               loadDone = true;
            } catch (Throwable e) {
               logErrorWhileGet(domainObject, e);
            }
         } while(!loadDone);
      }
      return true;
   }

   protected int generateSeqAlea(int deb, int fin) {
      if (!this._new_order) {
         for (int i = deb; i <= fin; i++) {
            this._seqIdCustomer[i] = i + 1;
         }
         this._new_order = true;
      }
      int rand;
      int alea;
      do {
         rand = (int) tpccTools.get().nonUniformRandom(getC_ID(), TpccTools.A_C_ID, deb, fin);
         alea = this._seqIdCustomer[rand];
      } while (alea == TpccTools.NULL_NUMBER);
      _seqIdCustomer[rand] = TpccTools.NULL_NUMBER;
      return alea;
   }

   protected void printMemoryInfo() {
      MemoryUsage u1 = this.memoryBean.getHeapMemoryUsage();
      log.info("Memory Statistics (Heap) - used=" + memString(u1.getUsed()) +
                     "; committed=" + memString(u1.getCommitted()));
      MemoryUsage u2 = this.memoryBean.getNonHeapMemoryUsage();
      log.info("Memory Statistics (NonHeap) - used=" + memString(u2.getUsed()) +
                     "; committed=" + memString(u2.getCommitted()));
   }

   protected void logStockPopulation(int warehouseID, long initID, long finishID) {
      log.debug("Populating Stock for Warehouse " + warehouseID + ", Items from " + initID + " to " + finishID);
   }

   protected void logOrderPopulation(int warehouseID, int districtID, long initID, long finishID) {
      log.debug("Populating Order for Warehouse " + warehouseID + " and District " + districtID +
                      " , Orders from " + initID + " to " + finishID);
   }

   protected void logCustomerPopulation(int warehouseID, int districtID, long initID, long finishID) {
      log.debug("Populating Customer for Warehouse " + warehouseID + " and District " + districtID +
                      " , Customer from " + initID + " to " + finishID);
   }

   protected void logItemsPopulation(long initID, long finishID) {
      log.debug("Populate Items from " + initID + " to " + finishID);
   }

   protected void logDistrictPopulation(int warehouse, long initId, long finishID) {
      log.debug("Populate Districts for Warehouse " + warehouse + ". Districts from " + initId + " to " + finishID);
   }

   private void logErrorWhilePut(Object object, Throwable throwable) {
      log.error("Error while trying to perform a put operation. Object is " + object +
                      ". Error is " + throwable.getLocalizedMessage() + ". Retrying...", throwable);
   }

   private void logErrorWhileGet(Object object, Throwable throwable) {
      log.error("Error while trying to perform a Get operation. Object is " + object +
                      ". Error is " + throwable.getLocalizedMessage() + ". Retrying...", throwable);
   }

   protected final Warehouse createWarehouse(int warehouseId) {
      return new Warehouse(wrapper, slaveIndex, warehouseId,
                           tpccTools.get().aleaChainec(6, 10),
                           tpccTools.get().aleaChainec(10, 20),
                           tpccTools.get().aleaChainec(10, 20),
                           tpccTools.get().aleaChainec(10, 20),
                           tpccTools.get().aleaChainel(2, 2),
                           tpccTools.get().aleaChainen(4, 4) + TpccTools.CHAINE_5_1,
                           tpccTools.get().aleaFloat(Float.valueOf("0.0000"), Float.valueOf("0.2000"), 4),
                           TpccTools.WAREHOUSE_YTD);
   }

   protected final Item createItem(long itemId) {
      return new Item(itemId,
                      tpccTools.get().aleaNumber(1, 10000),
                      tpccTools.get().aleaChainec(14, 24),
                      tpccTools.get().aleaFloat(1, 100, 2),
                      tpccTools.get().sData());
   }

   protected final Stock createStock(long stockId, int warehouseId) {
      return new Stock(stockId,
                       warehouseId,
                       tpccTools.get().aleaNumber(10, 100),
                       tpccTools.get().aleaChainel(24, 24),
                       tpccTools.get().aleaChainel(24, 24),
                       tpccTools.get().aleaChainel(24, 24),
                       tpccTools.get().aleaChainel(24, 24),
                       tpccTools.get().aleaChainel(24, 24),
                       tpccTools.get().aleaChainel(24, 24),
                       tpccTools.get().aleaChainel(24, 24),
                       tpccTools.get().aleaChainel(24, 24),
                       tpccTools.get().aleaChainel(24, 24),
                       tpccTools.get().aleaChainel(24, 24),
                       0,
                       0,
                       0,
                       tpccTools.get().sData());
   }

   protected final District createDistrict(int districtId, int warehouseId) {
      return new District(wrapper, slaveIndex, warehouseId,
                          districtId,
                          tpccTools.get().aleaChainec(6, 10),
                          tpccTools.get().aleaChainec(10, 20),
                          tpccTools.get().aleaChainec(10, 20),
                          tpccTools.get().aleaChainec(10, 20),
                          tpccTools.get().aleaChainel(2, 2),
                          tpccTools.get().aleaChainen(4, 4) + TpccTools.CHAINE_5_1,
                          tpccTools.get().aleaFloat(Float.valueOf("0.0000"), Float.valueOf("0.2000"), 4),
                          TpccTools.WAREHOUSE_YTD,
                          3001);
   }

   protected final Customer createCustomer(int warehouseId, long districtId, long customerId, String customerLastName) {
      return new Customer(wrapper, slaveIndex, warehouseId,
                          districtId,
                          customerId,
                          tpccTools.get().aleaChainec(8, 16),
                          "OE",
                          customerLastName,
                          tpccTools.get().aleaChainec(10, 20),
                          tpccTools.get().aleaChainec(10, 20),
                          tpccTools.get().aleaChainec(10, 20),
                          tpccTools.get().aleaChainel(2, 2),
                          tpccTools.get().aleaChainen(4, 4) + TpccTools.CHAINE_5_1,
                          tpccTools.get().aleaChainen(16, 16),
                          new Date(System.currentTimeMillis()),
                          (tpccTools.get().aleaNumber(1, 10) == 1) ? "BC" : "GC",
                          500000.0,
                          tpccTools.get().aleaDouble(0., 0.5, 4),
                          -10.0,
                          10.0,
                          1,
                          0,
                          tpccTools.get().aleaChainec(300, 500));
   }

   protected final History createHistory(long customerId, long districtId, long warehouseId) {
      return new History(customerId,
                         districtId,
                         warehouseId,
                         districtId,
                         warehouseId,
                         new Date(System.currentTimeMillis()), 10, tpccTools.get().aleaChainec(12, 24));
   }

   protected final Order createOrder(long orderId, long districtId, long warehouseId, Date aDate, int o_ol_cnt,
                                     int seqAlea) {
      return new Order(orderId,
                       districtId,
                       warehouseId,
                       seqAlea,
                       aDate,
                       (orderId < TpccTools.LIMIT_ORDER) ? tpccTools.get().aleaNumber(1, 10) : 0,
                       o_ol_cnt,
                       1);
   }

   protected final OrderLine createOrderLine(long orderId, long districtId, long warehouseId, long orderLineId,
                                             Date delivery_date, double amount) {
      return new OrderLine(orderId,
                           districtId,
                           warehouseId,
                           orderLineId,
                           tpccTools.get().nonUniformRandom(getOL_I_ID(), TpccTools.A_OL_I_ID, 1L, TpccTools.NB_MAX_ITEM),
                           warehouseId,
                           delivery_date,
                           5,
                           amount,
                           tpccTools.get().aleaChainel(12, 24));
   }

   protected final NewOrder createNewOrder(long orderId, long districtId, long warehouseId) {
      return new NewOrder(orderId,
                          districtId,
                          warehouseId);
   }
}


