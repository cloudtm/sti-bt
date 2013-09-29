package org.radargun.tpcc.transaction;

import org.radargun.CacheWrapper;
import org.radargun.tpcc.ElementNotFoundException;
import org.radargun.tpcc.TpccTools;
import org.radargun.tpcc.domain.Customer;
import org.radargun.tpcc.domain.District;
import org.radargun.tpcc.domain.Item;
import org.radargun.tpcc.domain.NewOrder;
import org.radargun.tpcc.domain.Order;
import org.radargun.tpcc.domain.OrderLine;
import org.radargun.tpcc.domain.Stock;
import org.radargun.tpcc.domain.Warehouse;

import java.util.Date;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Pedro Ruivo
 */
public class NewOrderTransaction implements TpccTransaction {

   private final long warehouseID;
   private final long districtID;
   private final long customerID;
   private final int numItems;
   private int allLocal;

   private final long[] itemIDs;
   private final long[] supplierWarehouseIDs;
   private final long[] orderQuantities;

   public NewOrderTransaction(TpccTools tpccTools, int warehouseID) {

      if (warehouseID <= 0) {
         this.warehouseID = tpccTools.randomNumber(1, TpccTools.NB_WAREHOUSES);
      } else {
         this.warehouseID = warehouseID;
      }

      this.districtID = tpccTools.randomNumber(1, TpccTools.NB_MAX_DISTRICT);
      this.customerID = tpccTools.nonUniformRandom(TpccTools.C_C_ID, TpccTools.A_C_ID, 1, TpccTools.NB_MAX_CUSTOMER);

      this.numItems = (int) tpccTools.randomNumber(TpccTools.NUMBER_OF_ITEMS_INTERVAL[0],
                                                   TpccTools.NUMBER_OF_ITEMS_INTERVAL[1]); // o_ol_cnt
      this.itemIDs = new long[numItems];
      this.supplierWarehouseIDs = new long[numItems];
      this.orderQuantities = new long[numItems];
      this.allLocal = 1; // see clause 2.4.2.2 (dot 6)
      for (int i = 0; i < numItems; i++) // clause 2.4.1.5
      {
         itemIDs[i] = tpccTools.nonUniformRandom(TpccTools.C_OL_I_ID, TpccTools.A_OL_I_ID, 1, TpccTools.NB_MAX_ITEM);
         if (tpccTools.randomNumber(1, 100) > 1) {
            supplierWarehouseIDs[i] = this.warehouseID;
         } else //see clause 2.4.1.5 (dot 2)
         {
            do {
               supplierWarehouseIDs[i] = tpccTools.randomNumber(1, TpccTools.NB_WAREHOUSES);
            }
            while (supplierWarehouseIDs[i] == this.warehouseID && TpccTools.NB_WAREHOUSES > 1);
            allLocal = 0;// see clause 2.4.2.2 (dot 6)
         }
         orderQuantities[i] = tpccTools.randomNumber(1, TpccTools.NB_MAX_DISTRICT); //see clause 2.4.1.5 (dot 6)
      }
      // clause 2.4.1.5 (dot 1)
      if (tpccTools.randomNumber(1, 100) == 1)
         this.itemIDs[this.numItems - 1] = -12345;

   }

   @Override
   public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
      newOrderTransaction(cacheWrapper);
   }

   @Override
   public boolean isReadOnly() {
      return false;
   }

   private void newOrderTransaction(CacheWrapper cacheWrapper) throws Throwable {
      long o_id = -1, s_quantity;
      String i_data, s_data;

      String ol_dist_info = null;
      double[] itemPrices = new double[numItems];
      double[] orderLineAmounts = new double[numItems];
      String[] itemNames = new String[numItems];
      long[] stockQuantities = new long[numItems];
      char[] brandGeneric = new char[numItems];
      long ol_supply_w_id, ol_i_id, ol_quantity;
      int s_remote_cnt_increment;
      double ol_amount, total_amount = 0;


      Customer c = new Customer();
      Warehouse w = new Warehouse();

      c.setC_id(customerID);
      c.setC_d_id(districtID);
      c.setC_w_id(warehouseID);

      boolean found = c.load(cacheWrapper, ((int) warehouseID - 1));

      if (!found)
         throw new ElementNotFoundException("W_ID=" + warehouseID + " C_D_ID=" + districtID + " C_ID=" + customerID + " not found!");

      w.setW_id(warehouseID);

      found = w.load(cacheWrapper, ((int) warehouseID - 1));
      if (!found) throw new ElementNotFoundException("W_ID=" + warehouseID + " not found!");


      District d = new District();
      // see clause 2.4.2.2 (dot 4)


      d.setD_id(districtID);
      d.setD_w_id(warehouseID);
      found = d.load(cacheWrapper, ((int) warehouseID - 1));
      if (!found) throw new ElementNotFoundException("D_ID=" + districtID + " D_W_ID=" + warehouseID + " not found!");


      o_id = d.getD_next_o_id();


      NewOrder no = new NewOrder(o_id, districtID, warehouseID);

      no.store(cacheWrapper, ((int) warehouseID - 1));

      d.setD_next_o_id(d.getD_next_o_id() + 1);

      d.store(cacheWrapper, ((int) warehouseID - 1));


      Order o = new Order(o_id, districtID, warehouseID, customerID, new Date(), -1, numItems, allLocal);

      o.store(cacheWrapper, ((int) warehouseID - 1));


      // see clause 2.4.2.2 (dot 8)
      for (int ol_number = 1; ol_number <= numItems; ol_number++) {
         ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
         ol_i_id = itemIDs[ol_number - 1];
         ol_quantity = orderQuantities[ol_number - 1];

         // clause 2.4.2.2 (dot 8.1)
         Item i = new Item();
         i.setI_id(ol_i_id);
         found = i.load(cacheWrapper, ((int) ol_supply_w_id - 1));
         if (!found) throw new ElementNotFoundException("I_ID=" + ol_i_id + " not found!");


         itemPrices[ol_number - 1] = i.getI_price();
         itemNames[ol_number - 1] = i.getI_name();
         // clause 2.4.2.2 (dot 8.2)

         Stock s = new Stock();
         s.setS_i_id(ol_i_id);
         s.setS_w_id(ol_supply_w_id);
         found = s.load(cacheWrapper, ((int) ol_supply_w_id - 1));
         if (!found) throw new ElementNotFoundException("I_ID=" + ol_i_id + " not found!");


         s_quantity = s.getS_quantity();
         stockQuantities[ol_number - 1] = s_quantity;
         // clause 2.4.2.2 (dot 8.2)
         if (s_quantity - ol_quantity >= 10) {
            s_quantity -= ol_quantity;
         } else {
            s_quantity += -ol_quantity + 91;
         }

         if (ol_supply_w_id == warehouseID) {
            s_remote_cnt_increment = 0;
         } else {
            s_remote_cnt_increment = 1;
         }
         // clause 2.4.2.2 (dot 8.2)
         s.setS_quantity(s_quantity);
         s.setS_ytd(s.getS_ytd() + ol_quantity);
         s.setS_remote_cnt(s.getS_remote_cnt() + s_remote_cnt_increment);
         s.setS_order_cnt(s.getS_order_cnt() + 1);
         s.store(cacheWrapper, ((int) ol_supply_w_id - 1));


         // clause 2.4.2.2 (dot 8.3)
         ol_amount = ol_quantity * i.getI_price();
         orderLineAmounts[ol_number - 1] = ol_amount;
         total_amount += ol_amount;
         // clause 2.4.2.2 (dot 8.4)
         i_data = i.getI_data();
         s_data = s.getS_data();
         if (i_data.contains(TpccTools.ORIGINAL) && s_data.contains(TpccTools.ORIGINAL)) {
            brandGeneric[ol_number - 1] = 'B';
         } else {
            brandGeneric[ol_number - 1] = 'G';
         }

         switch ((int) districtID) {
            case 1:
               ol_dist_info = s.getS_dist_01();
               break;
            case 2:
               ol_dist_info = s.getS_dist_02();
               break;
            case 3:
               ol_dist_info = s.getS_dist_03();
               break;
            case 4:
               ol_dist_info = s.getS_dist_04();
               break;
            case 5:
               ol_dist_info = s.getS_dist_05();
               break;
            case 6:
               ol_dist_info = s.getS_dist_06();
               break;
            case 7:
               ol_dist_info = s.getS_dist_07();
               break;
            case 8:
               ol_dist_info = s.getS_dist_08();
               break;
            case 9:
               ol_dist_info = s.getS_dist_09();
               break;
            case 10:
               ol_dist_info = s.getS_dist_10();
               break;
         }
         // clause 2.4.2.2 (dot 8.5)

         OrderLine ol = new OrderLine(o_id, districtID, warehouseID, ol_number, ol_i_id, ol_supply_w_id, null,
                                      ol_quantity, ol_amount, ol_dist_info);
         ol.store(cacheWrapper, ((int) warehouseID - 1));

      }

   }


}
