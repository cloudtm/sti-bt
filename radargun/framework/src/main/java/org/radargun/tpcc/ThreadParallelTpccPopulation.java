package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.tpcc.domain.CustomerLookup;
import org.radargun.utils.ThreadTpccToolsManager;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Note: the code is not fully-engineered as it lacks some basic checks (for example on the number
 *  of threads).
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo      
 */
public class ThreadParallelTpccPopulation extends TpccPopulation{

   private static Log log = LogFactory.getLog(ThreadParallelTpccPopulation.class);
   private static final long MAX_SLEEP_BEFORE_RETRY = 30000; //30 seconds

   private int parallelThreads = 4;
   private int elementsPerBlock = 100;  //items loaded per transaction
   private AtomicLong waitingPeriod;
   private final ThreadTpccToolsManager threadTpccToolsManager;

   public ThreadParallelTpccPopulation(CacheWrapper wrapper, int numWarehouses, int slaveIndex, int numSlaves,
                                       long cLastMask, long olIdMask, long cIdMask,
                                       int parallelThreads, int elementsPerBlock) {
      this(wrapper, numWarehouses, slaveIndex, numSlaves, cLastMask, olIdMask, cIdMask, parallelThreads, elementsPerBlock,
            false, new ThreadTpccToolsManager(System.nanoTime()));
   }
   
   public ThreadParallelTpccPopulation(CacheWrapper wrapper, int numWarehouses, int slaveIndex, int numSlaves,
                                       long cLastMask, long olIdMask, long cIdMask,
                                       int parallelThreads, int elementsPerBlock, boolean populateLocalOnly, 
                                       ThreadTpccToolsManager threadTpccToolsManager) {
      super(wrapper, numWarehouses, slaveIndex, numSlaves, cLastMask, olIdMask, cIdMask, populateLocalOnly);
      this.parallelThreads = parallelThreads;
      this.elementsPerBlock = elementsPerBlock;

      if (this.parallelThreads <= 0) {
         log.warn("Parallel threads must be greater than zero. disabling parallel population");
         this.parallelThreads = 1;
      }
      if (this.elementsPerBlock <= 0) {
         log.warn("Batch level must be greater than zero. disabling batching level");
         this.elementsPerBlock = 1;
      }

      this.waitingPeriod = new AtomicLong(0);
      this.threadTpccToolsManager = threadTpccToolsManager;
   }

   @Override
   protected void populateItem(){
      log.trace("Populating Items");

      long init_id_item=1;
      long num_of_items=TpccTools.NB_MAX_ITEM;

      if(numSlaves>1){
         long remainder=TpccTools.NB_MAX_ITEM % numSlaves;
         num_of_items=(TpccTools.NB_MAX_ITEM-remainder)/numSlaves;

         init_id_item=(slaveIndex*num_of_items)+1;

         if(slaveIndex==numSlaves-1){
            num_of_items+=remainder;
         }
      }

      performMultiThreadPopulation(init_id_item, num_of_items, new ThreadCreator() {
         @Override
         public Thread createThread(int threadIdx, long lowerBound, long upperBound) {
            return new PopulateItemThread(threadIdx, lowerBound, upperBound);
         }
      });
   }

   @Override
   protected void populateStock(final int warehouseId){
      if (warehouseId < 0) {
         log.warn("Trying to populate Stock for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating Stock for warehouse " + warehouseId);

      long init_id_item=1;
      long num_of_items=TpccTools.NB_MAX_ITEM;

      if(numSlaves>1){
         long remainder=TpccTools.NB_MAX_ITEM % numSlaves;
         num_of_items=(TpccTools.NB_MAX_ITEM-remainder)/numSlaves;

         init_id_item=(slaveIndex*num_of_items)+1;

         if(slaveIndex==numSlaves-1){
            num_of_items+=remainder;
         }
      }

      performMultiThreadPopulation(init_id_item, num_of_items, new ThreadCreator() {
         @Override
         public Thread createThread(int threadIdx, long lowerBound, long upperBound) {
            return new PopulateStockThread(threadIdx, lowerBound, upperBound, warehouseId);
         }
      });
   }

   @Override
   protected void populateCustomers(final int warehouseId, final int districtId){
      if (warehouseId < 0 || districtId < 0) {
         log.warn("Trying to populate Customer with a negative warehouse or district ID. skipping...");
         return;
      }

      log.trace("Populating Customers for warehouse " + warehouseId + " and district " + districtId);

      final ConcurrentHashMap<CustomerLookupQuadruple,Integer> lookupContentionAvoidance =
            new ConcurrentHashMap<CustomerLookupQuadruple, Integer>();

      performMultiThreadPopulation(1, TpccTools.NB_MAX_CUSTOMER, new ThreadCreator() {
         @Override
         public Thread createThread(int threadIdx, long lowerBound, long upperBound) {
            return new PopulateCustomerThread(threadIdx, lowerBound, upperBound, warehouseId, districtId, lookupContentionAvoidance);
         }
      });

      if(isBatchingEnabled()){
         populateCustomerLookup(lookupContentionAvoidance);
      }
   }

   protected void populateCustomerLookup(ConcurrentHashMap<CustomerLookupQuadruple, Integer> map){
      log.trace("Populating customer lookup ");

      final Vector<CustomerLookupQuadruple> vec_map = new Vector<CustomerLookupQuadruple>(map.keySet());
      long totalEntries = vec_map.size();

      log.trace("Populating customer lookup. Size is " + totalEntries);

      performMultiThreadPopulation(0, totalEntries, new ThreadCreator() {
         @Override
         public Thread createThread(int threadIdx, long lowerBound, long upperBound) {
            return new PopulateCustomerLookupThread(threadIdx, lowerBound, upperBound, vec_map);
         }
      });
   }

   @Override
   protected void populateOrders(final int warehouseId, final int districtId){
      if (warehouseId < 0 || districtId < 0) {
         log.warn("Trying to populate Order with a negative warehouse or district ID. skipping...");
         return;
      }

      log.trace("Populating Orders for warehouse " + warehouseId + " and district " + districtId);
      this._new_order = false;

      performMultiThreadPopulation(1, TpccTools.NB_MAX_ORDER, new ThreadCreator() {
         @Override
         public Thread createThread(int threadIdx, long lowerBound, long upperBound) {
            return new PopulateOrderThread(threadIdx, lowerBound, upperBound, warehouseId, districtId);
         }
      });
   }

   /*
    * ######################################### POPULATING THREADS ################################
    */

   protected class PopulateOrderThread extends Thread{
      private long lowerBound;
      private long upperBound;
      private int warehouseId;
      private int districtId;
      private final int threadIdx;

      @Override
      public String toString() {
         return "PopulateOrderThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               ", warehouseId=" + warehouseId +
               ", districtId=" + districtId +
               '}';
      }

      public PopulateOrderThread(int threadIdx, long l, long u, int w, int d){
         this.lowerBound = l;
         this.upperBound = u;
         this.districtId = d;
         this.warehouseId = w;
         this.threadIdx = threadIdx;
      }

      public void run(){
         tpccTools.set(threadTpccToolsManager.getTpccTools(threadIdx));
         logStart(toString());

         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder) / elementsPerBlock;
         long base = lowerBound;

         for(int batch = 1; batch <= numBatches; batch++){
            logBatch(toString(), batch, numBatches);
            executeTransaction(base, base + elementsPerBlock);
            base += elementsPerBlock;
         }

         logRemainder(toString());
         executeTransaction(base, upperBound + 1);

         logFinish(toString());
      }

      private void executeTransaction(long start, long end) {
         logOrderPopulation(warehouseId, districtId, start, end - 1);
         LinkedList<Integer> seqAleaList = new LinkedList<Integer>();
         boolean useList = false;

         do {
            startTransactionIfNeeded();
            Iterator<Integer> iterator = seqAleaList.iterator();

            for(long orderId=start; orderId < end; orderId++){

               int generatedSeqAlea;

               if (useList && iterator.hasNext()) {
                  generatedSeqAlea = iterator.next();
               } else {
                  generatedSeqAlea = generateSeqAlea(0, TpccTools.NB_MAX_CUSTOMER-1);
                  seqAleaList.add(generatedSeqAlea);
               }

               int o_ol_cnt = tpccTools.get().aleaNumber(5, 15);
               Date aDate = new Date((new java.util.Date()).getTime());

               if (!txAwarePut(createOrder(orderId, districtId, warehouseId, aDate, o_ol_cnt, generatedSeqAlea))) {
                  break; // rollback tx
               }
               populateOrderLines(warehouseId, districtId, (int)orderId, o_ol_cnt, aDate);

               if (orderId >= TpccTools.LIMIT_ORDER){
                  populateNewOrder(warehouseId, districtId, (int)orderId);
               }
            }
            useList = true;
         } while (!endTransactionIfNeeded());
      }
   }

   protected class PopulateCustomerThread extends Thread{
      private long lowerBound;
      private long upperBound;
      private int warehouseId;
      private int districtId;
      private ConcurrentHashMap<CustomerLookupQuadruple,Integer> lookupContentionAvoidance;
      private final int threadIdx;

      @Override
      public String toString() {
         return "PopulateCustomerThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               ", warehouseId=" + warehouseId +
               ", districtId=" + districtId +
               '}';
      }

      @SuppressWarnings("unchecked")
      public PopulateCustomerThread(int threadIdx, long lowerBound, long upperBound, int warehouseId, int districtId,
                                    ConcurrentHashMap c){
         this.lowerBound = lowerBound;
         this.upperBound = upperBound;
         this.districtId = districtId;
         this.warehouseId = warehouseId;
         this.lookupContentionAvoidance = c;
         this.threadIdx = threadIdx;
      }

      public void run(){
         tpccTools.set(threadTpccToolsManager.getTpccTools(threadIdx));
         logStart(toString());

         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder)  / elementsPerBlock;
         long base = lowerBound;

         for(int batch =1; batch <= numBatches; batch++){
            logBatch(toString(), batch, numBatches);
            executeTransaction(base, base + elementsPerBlock);
            base += elementsPerBlock;
         }

         logRemainder(toString());
         executeTransaction(base, upperBound + 1);

         logFinish(toString());
      }

      private void executeTransaction(long start, long end) {
         logCustomerPopulation(warehouseId, districtId, start, end - 1);
         do {
            startTransactionIfNeeded();
            for(long customerId = start; customerId < end; customerId++) {
               String c_last = c_last();

               if (!txAwarePut(createCustomer(warehouseId, districtId, customerId, c_last))) {
                  break; // rollback tx
               }

               if(isBatchingEnabled()){
                  CustomerLookupQuadruple clt = new CustomerLookupQuadruple(c_last,warehouseId,districtId, customerId);
                  if(!this.lookupContentionAvoidance.containsKey(clt)){
                     this.lookupContentionAvoidance.put(clt,1);
                  }
               } else{
                  CustomerLookup customerLookup = new CustomerLookup(c_last, warehouseId, districtId);
                  if (!txAwareLoad(customerLookup)) {
                     break; // rollback tx
                  }
                  customerLookup.addId(customerId);

                  if (!txAwarePut(customerLookup)) {
                     break; // rollback tx
                  }
               }

               populateHistory((int)customerId, warehouseId, districtId);
            }
         } while (!endTransactionIfNeeded());
      }
   }

   protected class PopulateItemThread extends Thread{

      private long lowerBound;
      private long upperBound;
      private final int threadIdx;

      @Override
      public String toString() {
         return "PopulateItemThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               '}';
      }

      public PopulateItemThread(int threadIdx, long low, long up){
         this.lowerBound = low;
         this.upperBound = up;
         this.threadIdx = threadIdx;
      }

      public void run(){
         tpccTools.set(threadTpccToolsManager.getTpccTools(threadIdx));
         logStart(toString());

         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder ) / elementsPerBlock;
         long base = lowerBound;

         for(long batch = 1; batch <=numBatches; batch++){
            logBatch(toString(), batch, numBatches);
            executeTransaction(base, base + elementsPerBlock);
            base += elementsPerBlock;
         }

         logRemainder(toString());
         executeTransaction(base, upperBound + 1);

         logFinish(toString());
      }

      private void executeTransaction(long start, long end) {
         logItemsPopulation(start, end - 1);
         do {
            startTransactionIfNeeded();
            for(long itemId = start; itemId < end; itemId++){
               if (!txAwarePut(createItem(itemId))) {
                  break; //rollback tx;
               }
            }
         } while (!endTransactionIfNeeded());
      }
   }

   protected class PopulateStockThread extends Thread{
      private long lowerBound;
      private long upperBound;
      private int warehouseId;
      private final int threadIdx;

      @Override
      public String toString() {
         return "PopulateStockThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               ", warehouseId=" + warehouseId +
               '}';
      }

      public PopulateStockThread(int threadIdx, long low, long up, int warehouseId){
         this.lowerBound = low;
         this.upperBound = up;
         this.warehouseId = warehouseId;
         this.threadIdx = threadIdx;
      }

      public void run(){
         tpccTools.set(threadTpccToolsManager.getTpccTools(threadIdx));
         logStart(toString());

         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder ) / elementsPerBlock;
         long base = lowerBound;

         for(long batch = 1; batch <=numBatches; batch++){
            logBatch(toString(), batch, numBatches);
            executeTransaction(base, base + elementsPerBlock);
            base += elementsPerBlock;
         }

         logRemainder(toString());
         executeTransaction(base, upperBound + 1);

         logFinish(toString());
      }

      private void executeTransaction(long start, long end) {
         logStockPopulation(warehouseId, start, end - 1);
         do {
            startTransactionIfNeeded();
            for(long stockId = start; stockId < end; stockId++){
               if (!txAwarePut(createStock(stockId, warehouseId))) {
                  break;
               }
            }
         } while (!endTransactionIfNeeded());
      }
   }

   protected class PopulateCustomerLookupThread extends Thread{
      private Vector<CustomerLookupQuadruple> vector;
      private long lowerBound;
      private long upperBound;
      private final int threadIdx;

      @Override
      public String toString() {
         return "PopulateCustomerLookupThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               '}';
      }

      @SuppressWarnings("unchecked")
      public PopulateCustomerLookupThread(int threadIdx, long l, long u, Vector v){
         this.vector = v;
         this.lowerBound = l;
         this.upperBound = u;
         this.threadIdx = threadIdx;
      }

      public void run(){
         tpccTools.set(threadTpccToolsManager.getTpccTools(threadIdx));
         logStart(toString());

         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder ) / elementsPerBlock;
         long base = lowerBound;

         for(long batch = 1; batch <= numBatches; batch++){
            logBatch(toString(), batch, numBatches);
            executeTransaction(base, base + elementsPerBlock);
            base += elementsPerBlock;
         }

         logRemainder(toString());
         executeTransaction(base, upperBound + 1);

         logFinish(toString());
      }

      private void executeTransaction(long start, long end) {
         logCustomerLookupPopulation(start, end - 1);
         do {
            startTransactionIfNeeded();
            for(long idx = start; idx < end; idx++){

               CustomerLookupQuadruple clq = this.vector.get((int)idx);
               CustomerLookup customerLookup = new CustomerLookup(clq.c_last, clq.warehouseId, clq.districtId);

               if (!txAwareLoad(customerLookup)) {
                  break; //rollback tx
               }

               customerLookup.addId(clq.customerId);

               if (!txAwarePut(customerLookup)) {
                  break; //rollback tx
               }
            }
         } while (!endTransactionIfNeeded());
      }
   }

   protected class CustomerLookupQuadruple {
      private String c_last;
      private int warehouseId;
      private int districtId;
      private long customerId;

      public CustomerLookupQuadruple(String c, int w, int d, long i){
         this.c_last = c;
         this.warehouseId = w;
         this.districtId = d;
         this.customerId = i;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         CustomerLookupQuadruple that = (CustomerLookupQuadruple) o;
         //The customer id does not count!!! it's not part of the key
         //if (customerId != that.customerId) return false;
         return districtId == that.districtId &&
               warehouseId == that.warehouseId &&
               !(c_last != null ? !c_last.equals(that.c_last) : that.c_last != null);

      }

      @Override
      public int hashCode() {
         int result = c_last != null ? c_last.hashCode() : 0;
         result = 31 * result + warehouseId;
         result = 31 * result + districtId;
         //I don't need customerId since it's not part of a customerLookup's key
         //result = 31 * result + (int)customerId;
         return result;
      }

      @Override
      public String toString() {
         return "CustomerLookupQuadruple{" +
               "c_last='" + c_last + '\'' +
               ", warehouseId=" + warehouseId +
               ", districtId=" + districtId +
               ", customerId=" + customerId +
               '}';
      }
   }

   protected final boolean isBatchingEnabled(){
      return this.elementsPerBlock != 1;
   }

   private void startTransactionIfNeeded() {
      if (isBatchingEnabled()) {
         //Pedro: this is experimental. I want to avoid the overloading of the network. 
         // So, instead of starting immediately the transaction, it waits a while
         long sleepFor = waitingPeriod.get();

         if (sleepFor > 0) {
            sleepFor(sleepFor);
         }
         wrapper.startTransaction(false);
      }
   }

   private boolean endTransactionIfNeeded() {
      if (!isBatchingEnabled()) {
         return true;
      }

      long start = System.currentTimeMillis();
      try {
         wrapper.endTransaction(true);
      } catch (Throwable t) {
         log.warn("Error committing transaction. Error is " + t.getMessage(), t);
         try {
            wrapper.endTransaction(false);
         } catch (Throwable t2) {
            //just ignore
         }
         sleepRandomly();
         log.warn("Retrying transaction...");
         return false;
      } finally {
         calculateNextWaitingTime(System.currentTimeMillis() - start);
      }
      return true;
   }

   private void calculateNextWaitingTime(long duration) {
      if (duration <= 10) {
         long old = waitingPeriod.get();
         waitingPeriod.set(old / 2);
         return ;
      }
      int counter = 0;
      while (duration > 0) {
         counter++;
         duration /= 10;
      }
      waitingPeriod.addAndGet(counter);
   }

   private void sleepRandomly() {
      Random r = new Random();
      long sleepFor;
      do {
         sleepFor = r.nextLong();
      } while (sleepFor <= 0);
      sleepFor(sleepFor % MAX_SLEEP_BEFORE_RETRY);
   }

   private void sleepFor(long milliseconds) {
      try {
         Thread.sleep(milliseconds);
      } catch (InterruptedException e) {
         //no-op
      }
   }

   private void logStart(String thread) {
      log.debug("Starting " + thread);
   }

   private void logFinish(String thread) {
      log.debug("Ended " + thread);
   }

   private void logBatch(String thread, long batch, long numberOfBatches) {
      log.debug(thread + " is populating the " + batch + " batch out of " + numberOfBatches);
   }

   private void logRemainder(String thread) {
      log.debug(thread + " is populating the remainder");
   }

   private void logCustomerLookupPopulation(long init, long end) {
      log.debug("Populate Customer Lookup from index " + init + " to " + end);
   }

   protected void performMultiThreadPopulation(long initValue, long numberOfItems, ThreadCreator threadCreator) {
      Thread[] threads = new Thread[parallelThreads];

      //compute the number of item per thread
      long threadRemainder = numberOfItems % parallelThreads;
      long itemsPerThread = (numberOfItems - threadRemainder) / parallelThreads;

      long lowerBound = initValue;
      long itemsToAdd;

      for(int i = 1; i <= parallelThreads; i++){
         itemsToAdd = itemsPerThread + (i == parallelThreads ? threadRemainder:0);
         Thread thread = threadCreator.createThread(i, lowerBound, lowerBound + itemsToAdd - 1);
         threads[i-1] = thread;
         thread.start();
         lowerBound += (itemsToAdd);
      }

      //wait until all thread are finished
      try{
         for(Thread thread : threads){
            log.trace("Waiting for the end of " + thread);
            thread.join();
         }
         log.trace("All threads have finished! Movin' on");
      }
      catch(InterruptedException ie){
         ie.printStackTrace();
         System.exit(-1);
      }
   }

   protected interface ThreadCreator {
      Thread createThread(int threadIdx, long lowerBound, long upperBound);
   }
}
