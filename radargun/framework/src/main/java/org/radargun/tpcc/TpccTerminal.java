package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.tpcc.transaction.NewOrderTransaction;
import org.radargun.tpcc.transaction.OrderStatusTransaction;
import org.radargun.tpcc.transaction.PaymentTransaction;
import org.radargun.tpcc.transaction.TpccTransaction;


/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Pedro Ruivo
 */
public class TpccTerminal {

   private static Log log = LogFactory.getLog(TpccTerminal.class);

   public final static int NEW_ORDER = 1, PAYMENT = 2, ORDER_STATUS = 3, DELIVERY = 4, STOCK_LEVEL = 5;

   public final static String[] nameTokens = {"BAR", "OUGHT"/*, "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"*/};

   private double paymentWeight;

   private double orderStatusWeight;

   private final int indexNode;

   private int localWarehouseID;

   private final TpccTools tpccTools;


   public TpccTerminal(double paymentWeight, double orderStatusWeight, int indexNode, int localWarehouseID) {
      this.paymentWeight = paymentWeight;
      this.orderStatusWeight = orderStatusWeight;
      this.indexNode = indexNode;
      this.localWarehouseID = localWarehouseID;
      tpccTools = TpccTools.newInstance();
   }

   public synchronized final TpccTransaction createTransaction(int type) {
      switch (type) {
         case PAYMENT:
            return new PaymentTransaction(tpccTools, indexNode, localWarehouseID);
         case ORDER_STATUS:
            return new OrderStatusTransaction(tpccTools, localWarehouseID);
         case NEW_ORDER:
            return new NewOrderTransaction(tpccTools, localWarehouseID);
         case DELIVERY:
         case STOCK_LEVEL:
         default:
            return null;
      }
   }

   public synchronized final TpccTransaction choiceTransaction(boolean isPassiveReplication, boolean isTheMaster) {
      return createTransaction(chooseTransactionType(isPassiveReplication, isTheMaster));
   }

   public synchronized final int chooseTransactionType(boolean isPassiveReplication, boolean isTheMaster) {
      double transactionType = Math.min(tpccTools.doubleRandomNumber(1, 100), 100.0);

      double realPaymentWeight = paymentWeight, realOrderStatusWeight = orderStatusWeight;

      if (isPassiveReplication) {
         if (isTheMaster) {
            realPaymentWeight = paymentWeight + (orderStatusWeight / 2);
            realOrderStatusWeight = 0;
         } else {
            realPaymentWeight = 0;
            realOrderStatusWeight = 100;
         }
      }

      if (log.isDebugEnabled()) {
         log.debug("Choose transaction " + transactionType +
                         ". Payment Weight=" + realPaymentWeight + "(" + paymentWeight + ")" +
                         ", Order Status Weight=" + realOrderStatusWeight + "(" + orderStatusWeight + ")");
      }

      if (transactionType <= realPaymentWeight) {
         return PAYMENT;
      } else if (transactionType <= realPaymentWeight + realOrderStatusWeight) {
         return ORDER_STATUS;
      } else {
         return NEW_ORDER;
      }
   }

   public synchronized void change(int localWarehouseID, double paymentWeight, double orderStatusWeight) {
      setLocalWarehouseID(localWarehouseID);
      setPercentages(paymentWeight, orderStatusWeight);
   }

   public synchronized void setPercentages(double paymentWeight, double orderStatusWeight) {
      this.paymentWeight = paymentWeight;
      this.orderStatusWeight = orderStatusWeight;
   }

   public synchronized void setLocalWarehouseID(int localWarehouseID) {
      this.localWarehouseID = localWarehouseID;
   }

   @Override
   public String toString() {
      return "TpccTerminal{" +
            "paymentWeight=" + paymentWeight +
            ", orderStatusWeight=" + orderStatusWeight +            
            ", localWarehouseID=" + (localWarehouseID == -1 ? "random" : localWarehouseID) +
            '}';
   }
}
