package org.radargun.tpcc.dac;

import org.radargun.CacheWrapper;
import org.radargun.tpcc.domain.Order;
import org.radargun.tpcc.domain.OrderLine;

import java.util.ArrayList;
import java.util.List;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 */
public final class OrderLineDAC {

   private OrderLineDAC() {
   }

   public static List<OrderLine> loadByOrder(CacheWrapper cacheWrapper, Order order) throws Throwable {

      List<OrderLine> list = new ArrayList<OrderLine>();

      if (order == null) return list;

      int numLines = order.getO_ol_cnt();

      OrderLine current = null;
      boolean found = false;

      for (int i = 0; i < numLines; i++) {

         current = new OrderLine();
         current.setOl_w_id(order.getO_w_id());
         current.setOl_d_id(order.getO_d_id());
         current.setOl_o_id(order.getO_id());
         current.setOl_number(i);

         found = current.load(cacheWrapper, ((int) order.getO_w_id() - 1));

         if (found) list.add(current);

      }

      return list;

   }


}
