package org.radargun.tpcc.transaction;

import org.radargun.CacheWrapper;
import org.radargun.tpcc.ElementNotFoundException;
import org.radargun.tpcc.TpccTerminal;
import org.radargun.tpcc.TpccTools;
import org.radargun.tpcc.dac.CustomerDAC;
import org.radargun.tpcc.dac.OrderDAC;
import org.radargun.tpcc.dac.OrderLineDAC;
import org.radargun.tpcc.domain.Customer;
import org.radargun.tpcc.domain.Order;
import org.radargun.tpcc.domain.OrderLine;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Pedro Ruivo
 */
public class OrderStatusTransaction implements TpccTransaction {

   private final long terminalWarehouseID;

   private final long districtID;

   private final String customerLastName;

   private final long customerID;

   private final boolean customerByName;

   public OrderStatusTransaction(TpccTools tpccTools, int warehouseID) {

      if (warehouseID <= 0) {
         this.terminalWarehouseID = tpccTools.randomNumber(1, TpccTools.NB_WAREHOUSES);
      } else {
         this.terminalWarehouseID = warehouseID;
      }

      // clause 2.6.1.2
      this.districtID = tpccTools.randomNumber(1, TpccTools.NB_MAX_DISTRICT);

      long y = tpccTools.randomNumber(1, 100);

      if (y <= 60) {
         // clause 2.6.1.2 (dot 1)
         this.customerByName = true;
         this.customerLastName = lastName((int) tpccTools.nonUniformRandom(TpccTools.C_C_LAST, TpccTools.A_C_LAST, 0, TpccTools.MAX_C_LAST));
         this.customerID = -1;
      } else {
         // clause 2.6.1.2 (dot 2)
         customerByName = false;
         customerID = tpccTools.nonUniformRandom(TpccTools.C_C_ID, TpccTools.A_C_ID, 1, TpccTools.NB_MAX_CUSTOMER);
         this.customerLastName = null;
      }

   }

   @Override
   public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
      orderStatusTransaction(cacheWrapper);
   }

   @Override
   public boolean isReadOnly() {
      return true;
   }

   private String lastName(int num) {
      return TpccTerminal.nameTokens[(num / 100) % TpccTerminal.nameTokens.length] + TpccTerminal.nameTokens[(num / 10) % TpccTerminal.nameTokens.length] + TpccTerminal.nameTokens[num % TpccTerminal.nameTokens.length];
   }

   private void orderStatusTransaction(CacheWrapper cacheWrapper) throws Throwable {
      long nameCnt;

      boolean found;
      Customer c;
      if (customerByName) {
         List<Customer> cList = CustomerDAC.loadByCLast(cacheWrapper, terminalWarehouseID, districtID, customerLastName);
         if (cList == null || cList.isEmpty())
            throw new ElementNotFoundException("C_LAST=" + customerLastName + " C_D_ID=" + districtID + " C_W_ID=" + terminalWarehouseID + " not found!");
         Collections.sort(cList);


         nameCnt = cList.size();


         if (nameCnt % 2 == 1) nameCnt++;
         Iterator<Customer> itr = cList.iterator();

         for (int i = 1; i <= nameCnt / 2; i++) {
            c = itr.next();
         }

      } else {
         // clause 2.6.2.2 (dot 3, Case 1)
         c = new Customer();
         c.setC_id(customerID);
         c.setC_d_id(districtID);
         c.setC_w_id(terminalWarehouseID);
         found = c.load(cacheWrapper, ((int) terminalWarehouseID - 1));
         if (!found)
            throw new ElementNotFoundException("C_ID=" + customerID + " C_D_ID=" + districtID + " C_W_ID=" + terminalWarehouseID + " not found!");

      }

      // clause 2.6.2.2 (dot 4)
      Order o = OrderDAC.loadByGreatestId(cacheWrapper, terminalWarehouseID, districtID, customerID);

      // clause 2.6.2.2 (dot 5)
      List<OrderLine> o_lines = OrderLineDAC.loadByOrder(cacheWrapper, o);
   }


}
