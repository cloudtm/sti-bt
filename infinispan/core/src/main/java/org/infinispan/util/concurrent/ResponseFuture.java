package org.infinispan.util.concurrent;

import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.ExposedStatistic;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.container.TransactionStatistics;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ResponseFuture implements Future<Map<Address, Response>> {

   private static final Log log = LogFactory.getLog(ResponseFuture.class);
   private final Future<RspList<Object>> realFuture;
   private final long timeout;
   private final boolean hasResponseFilter;
   private final ResponseParser parser;
   private ExposedStatistic durationStat;
   private ExposedStatistic counterStat;
   private ExposedStatistic recipientSizeStat;
   private ExposedStatistic commandSizeStat;
   private TransactionStatistics transactionStatistics;
   private long initTime = -1;
   private int size;
   private int numberOfNodes;

   public ResponseFuture(Future<RspList<Object>> realFuture, long timeout, boolean hasResponseFilter, ResponseParser parser) {
      this.realFuture = realFuture;
      this.timeout = timeout;
      this.hasResponseFilter = hasResponseFilter;
      this.parser = parser;
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      return realFuture.cancel(mayInterruptIfRunning);
   }

   @Override
   public boolean isCancelled() {
      return realFuture.isCancelled();
   }

   @Override
   public boolean isDone() {
      return realFuture.isDone();
   }

   @Override
   public Map<Address, Response> get() throws InterruptedException, ExecutionException {
      return innerGet(timeout, TimeUnit.MILLISECONDS);
   }

   @Override
   public Map<Address, Response> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
      return innerGet(timeout, unit);
   }

   public final void setUpdateStats(TransactionStatistics transactionStatistics, long initTime, ExposedStatistic durationStat,
                                    ExposedStatistic counterStat, ExposedStatistic recipientSizeStat, ExposedStatistic commandSizeStat, int size,
                                    int numberOfNodes) {
      this.initTime = initTime;
      this.transactionStatistics = transactionStatistics;
      this.durationStat = durationStat;
      this.counterStat = counterStat;
      this.recipientSizeStat = recipientSizeStat;
      this.commandSizeStat = commandSizeStat;
      this.size = size;
      this.numberOfNodes = numberOfNodes;
   }

   private Map<Address, Response> innerGet(long timeout, TimeUnit unit) throws org.infinispan.util.concurrent.TimeoutException, InterruptedException, ExecutionException {
      try {
         RspList<Object> rspList = realFuture.get(timeout, unit);
         if (log.isTraceEnabled()) {
            log.tracef("Response list is " + rspList);
         }
         if (rspList == null) {
            updateStats();
            return InfinispanCollections.emptyMap();
         } else {
            if (rspList.isEmpty() || containsOnlyNulls(rspList)) {
               return null;
            }
            Map<Address, Response> retval = new HashMap<Address, Response>(rspList.size());

            boolean noValidResponses = true;
            for (Rsp<Object> rsp : rspList.values()) {
               noValidResponses &= parser.parseResponse(rsp, retval, hasResponseFilter, false);
            }

            if (noValidResponses)
               throw new org.infinispan.util.concurrent.TimeoutException("Timed out waiting for valid responses!");
            updateStats();
            return retval;
         }
      } catch (java.util.concurrent.TimeoutException e) {
         throw new org.infinispan.util.concurrent.TimeoutException("Timed out waiting for valid responses!", e);
      } catch (org.infinispan.util.concurrent.TimeoutException e) {
         throw e;
      } catch (Exception e) {
         throw new RpcException(e);
      }
   }

   private void updateStats() {
      if (initTime == -1) {
         return;
      }
      if (transactionStatistics != null) {
         transactionStatistics.addValue(durationStat, System.nanoTime() - initTime);
         transactionStatistics.incrementValue(counterStat);
         transactionStatistics.addValue(recipientSizeStat, numberOfNodes);
         if (commandSizeStat != null) {
            transactionStatistics.addValue(commandSizeStat, size);
         }
      } else {
         TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(durationStat, System.nanoTime() - initTime, true);
         TransactionsStatisticsRegistry.incrementValueAndFlushIfNeeded(counterStat, true);
         TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(recipientSizeStat, numberOfNodes, true);
         if (commandSizeStat != null) {
            TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(commandSizeStat, size, true);
         }
      }

   }

   private boolean containsOnlyNulls(RspList<Object> l) {
      for (Rsp<Object> r : l.values()) {
         if (r.getValue() != null || !r.wasReceived() || r.wasSuspected()) return false;
      }
      return true;
   }

   public static interface ResponseParser {

      boolean parseResponse(Rsp<Object> rsp, Map<Address, Response> map, boolean hasFilter, boolean ignoreLeavers) throws Exception;

   }
}
