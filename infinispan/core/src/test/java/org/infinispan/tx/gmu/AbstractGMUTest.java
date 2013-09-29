/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.tx.gmu;

import org.infinispan.Cache;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.DataContainer;
import org.infinispan.container.gmu.GMUDataContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.gmu.manager.SortedTransactionQueue;
import org.infinispan.transaction.gmu.manager.TransactionCommitManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.distribution.DistributionTestHelper.isOwner;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class AbstractGMUTest extends MultipleCacheManagersTest {

   protected static final String KEY_1 = "key_1";
   protected static final String KEY_2 = "key_2";
   protected static final String KEY_3 = "key_3";
   protected static final String VALUE_1 = "value_1";
   protected static final String VALUE_2 = "value_2";
   protected static final String VALUE_3 = "value_3";
   private static final AtomicInteger KEY_ID = new AtomicInteger(0);

   @Override
   protected final void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = defaultGMUConfiguration();
      decorate(dcc);
      createCluster(dcc, initialClusterSize());
      waitForClusterToForm();
   }

   protected abstract void decorate(ConfigurationBuilder builder);

   protected abstract int initialClusterSize();

   protected abstract boolean syncCommitPhase();

   protected abstract CacheMode cacheMode();

   protected final void assertCachesValue(int executedOn, Object key, Object value) {
      for (int i = 0; i < cacheManagers.size(); ++i) {
         if (i == executedOn || syncCommitPhase()) {
            assertEquals("Wrong value for cache " + address(cache(i)) + " and key " + key + ".", value, cache(i).get(key));
         } else {
            assertEventuallyEquals(i, key, value);
         }
      }
   }

   protected final void assertCacheValuesNull(Object... keys) {
      for (int i = 0; i < cacheManagers.size(); ++i) {
         for (Object key : keys) {
            assertNull(cache(i).get(key));
         }
      }
   }

   protected final void assertAtLeastCaches(int size) {
      assert cacheManagers.size() >= size;
   }

   protected final void printDataContainer() {
      if (log.isDebugEnabled()) {
         StringBuilder stringBuilder = new StringBuilder("\n\n===================\n");
         for (int i = 0; i < cacheManagers.size(); ++i) {
            DataContainer dataContainer = cache(i).getAdvancedCache().getDataContainer();
            if (dataContainer instanceof GMUDataContainer) {
               stringBuilder.append(dataContainerToString((GMUDataContainer) dataContainer))
                     .append("\n")
                     .append("===================\n");
            } else {
               return;
            }
         }
         log.debugf(stringBuilder.toString());
      }
   }

   protected final void put(int cacheIndex, Object key, Object value, Object returnValue) {
      txPut(cacheIndex, key, value, returnValue);
      assertCachesValue(cacheIndex, key, value);
   }

   protected final void txPut(int cacheIndex, Object key, Object value, Object returnValue) {
      Object oldValue = cache(cacheIndex).put(key, value);
      assertEquals(returnValue, oldValue);
   }

   protected final void txRemove(int cacheIndex, Object key, Object returnValue) {
      Object oldValue = cache(cacheIndex).remove(key);
      assertEquals(returnValue, oldValue);
   }

   protected final void putIfAbsent(int cacheIndex, Object key, Object value, Object returnValue, Object expectedValue) {
      Object oldValue = cache(cacheIndex).putIfAbsent(key, value);
      assertCachesValue(cacheIndex, key, expectedValue);
      assertEquals(returnValue, oldValue);
   }

   protected final void putAll(int cacheIndex, Map<Object, Object> map) {
      cache(cacheIndex).putAll(map);
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
         assertCachesValue(cacheIndex, entry.getKey(), entry.getValue());
      }
   }

   protected final void remove(int cacheIndex, Object key, Object returnValue) {
      Object oldValue = cache(cacheIndex).remove(key);
      assertCachesValue(cacheIndex, key, null);
      assertEquals(returnValue, oldValue);
   }

   protected final void replace(int cacheIndex, Object key, Object value, Object returnValue) {
      Object oldValue = cache(cacheIndex).replace(key, value);
      assertCachesValue(cacheIndex, key, value);
      assertEquals(returnValue, oldValue);
   }

   protected final void replaceIf(int cacheIndex, Object key, Object value, Object ifValue, Object returnValue, boolean success) {
      boolean result = cache(cacheIndex).replace(key, ifValue, value);
      assertCachesValue(cacheIndex, key, returnValue);
      assertEquals(result, success);
   }

   protected final void removeIf(int cacheIndex, Object key, Object ifValue, Object returnValue, boolean success) {
      boolean result = cache(cacheIndex).remove(key, ifValue);
      assertCachesValue(cacheIndex, key, returnValue);
      assertEquals(result, success);
   }

   protected final void safeRollback(int cacheIndex) {
      safeRollback(tm(cacheIndex));
   }

   protected final void safeRollback(TransactionManager transactionManager) {
      try {
         transactionManager.rollback();
      } catch (Exception e) {
         log.warn("Exception suppressed when rollback: " + e.getMessage());
      }
   }

   protected final Object newKey(int mapTo, int notMapTo) {
      return newKey(Collections.singletonList(mapTo), Collections.singletonList(notMapTo));
   }

   protected final Object newKey(int mapTo, List<Integer> notMapTo) {
      return newKey(Collections.singletonList(mapTo), notMapTo);
   }

   protected final Object newKey(List<Integer> mapTo, List<Integer> notMapTo) {
      if (cacheMode().isReplicated()) {
         return "KEY_" + KEY_ID.incrementAndGet();
      }

      int numberOfCaches = cacheManagers.size();
      int numberOfOwners = cacheManagers.get(0).getCache().getCacheConfiguration().clustering().hash().numOwners();
      if (numberOfCaches == numberOfOwners) {
         Assert.assertEquals(notMapTo.size(), 0, "Number of caches equals to the number of owners. NotMapTo must be" +
               "empty");
         return "KEY_" + KEY_ID.incrementAndGet();
      }
      if (mapTo.size() + notMapTo.size() > numberOfCaches) {
         Assert.fail("MapTo and NotMapTo sizes should be lower or equals than the number of caches");
      }
      Cache<?,?>[] owners = new Cache[numberOfOwners];
      int insertIndex = 0;
      for (int cacheIndex : mapTo) {
         owners[insertIndex++] = cache(cacheIndex);
         if (insertIndex == numberOfOwners) {
            break;
         }
      }
      for (int i = 0; i < numberOfCaches && insertIndex < numberOfOwners; ++i) {
         if (mapTo.contains(i) || notMapTo.contains(i)) {
            continue; //already added or not to add
         }
         owners[insertIndex++] = cache(i);
      }
      return new GMUMagicKey("KEY_" + KEY_ID.incrementAndGet(), owners);
   }

   protected final Object newKey(int mapTo) {
      return newKey(Arrays.asList(mapTo), Collections.<Integer>emptyList());
   }

   protected final void assertKeyOwners(Object key, int mapTo, int notMapTo) {
      assertKeyOwners(key, Collections.singleton(mapTo), Collections.singleton(notMapTo));
   }

   protected final void assertKeyOwners(Object key, Collection<Integer> mapTo, Collection<Integer> notMapTo) {
      if (cacheMode().isReplicated()) {
         return;
      }
      if (mapTo != null) {
         for (int index : mapTo) {
            if (cache(index).getAdvancedCache().getDistributionManager() != null) {
               assert isOwner(cache(index), key) : key + " does not belong to " + addressOf(cache(index));
            }
         }
      }
      if (notMapTo != null) {
         for (int index : notMapTo) {
            if (cache(index).getAdvancedCache().getDistributionManager() != null) {
               assert !isOwner(cache(index), key) : key + " belong to " + addressOf(cache(index));
            }
         }
      }
   }

   protected final <T> T getComponent(int cacheIndex, Class<T> tClass) {
      return TestingUtil.extractComponent(cache(cacheIndex), tClass);
   }

   protected final void logKeysUsedInTest(String testName, Object... keys) {
      log.debugf("Test [%s] in class [%s] will use %s", testName, getClass().getSimpleName(), Arrays.asList(keys));
   }

   protected final Collection<Cache> caches(Collection<Integer> cacheIndexes) {
      if (cacheIndexes == null || cacheIndexes.isEmpty()) {
         return Collections.emptyList();
      }
      List<Cache> list = new LinkedList<Cache>();
      for (int index : cacheIndexes) {
         list.add(cache(index));
      }
      return list;
   }

   protected final GlobalTransaction globalTransaction(int cacheIndex) {
      return globalTransaction(cacheIndex, tx(cacheIndex));
   }

   protected final GlobalTransaction globalTransaction(int cacheIndex, Transaction transaction) {
      TransactionTable transactionTable = advancedCache(cacheIndex).getComponentRegistry()
            .getComponent(TransactionTable.class);
      LocalTransaction localTransaction = transactionTable.getLocalTransaction(transaction);
      return localTransaction == null ? null : localTransaction.getGlobalTransaction();
   }

   protected final Thread prepareInAllNodes(final Transaction tx, final DelayCommit delayCommit, final int cacheIndex)
         throws InterruptedException {
      final GlobalTransaction globalTransaction = globalTransaction(cacheIndex, tx);
      Thread thread = new Thread("PrepareOnly," + address(cacheIndex) + "," + globalTransaction.globalId()) {
         @Override
         public void run() {
            try {
               tm(cacheIndex).resume(tx);
               delayCommit.blockTransaction(globalTransaction);
               tm(cacheIndex).commit();
            } catch (Exception e) {
               e.printStackTrace();
               delayCommit.setCommitBlocked(true);
            }
         }
      };
      thread.start();
      delayCommit.awaitUntilCommitIsBlocked();
      return thread;
   }

   protected final DelayCommit addDelayCommit(int cacheIndex, int delay) {
      DelayCommit delayCommit = new DelayCommit(delay);
      advancedCache(cacheIndex).removeInterceptor(DelayCommit.class);
      advancedCache(cacheIndex).addInterceptorAfter(delayCommit, TxInterceptor.class);
      return delayCommit;
   }

   protected final DelayCommit addDelayCommit(int cacheIndex, int delay, Class<? extends CommandInterceptor> after) {
      DelayCommit delayCommit = new DelayCommit(delay);
      advancedCache(cacheIndex).removeInterceptor(DelayCommit.class);
      advancedCache(cacheIndex).addInterceptorAfter(delayCommit, after);
      return delayCommit;
   }

   private ConfigurationBuilder defaultGMUConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode(), true);
      builder.locking().isolationLevel(IsolationLevel.SERIALIZABLE);
      builder.versioning().enable().scheme(VersioningScheme.GMU);
      builder.transaction().syncCommitPhase(syncCommitPhase());
      builder.clustering().l1().disable();
      return builder;
   }

   private String dataContainerToString(GMUDataContainer dataContainer) {
      return dataContainer.stateToString();
   }

   protected class DelayCommit extends CommandInterceptor {
      private final long delay;
      private final Object commitLock = new Object();
      private final Object hasCommitLock = new Object();
      private volatile GlobalTransaction transactionToBlock;
      private boolean hasCommitBlocked;

      private DelayCommit(long delay) {
         this.delay = delay;
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         if (transactionToBlock != null && transactionToBlock.equals(command.getGlobalTransaction())) {
            blockCommit();
         }
         return invokeNextInterceptor(ctx, command);
      }

      public void blockTransaction(GlobalTransaction globalTransaction) {
         this.transactionToBlock = globalTransaction;
      }

      public void awaitUntilCommitIsBlocked() throws InterruptedException {
         synchronized (hasCommitLock) {
            while (!hasCommitBlocked) {
               hasCommitLock.wait();
            }
         }
      }

      public void blockCommit() {
         synchronized (commitLock) {
            setCommitBlocked(true);
            try {
               if (delay <= 0) {
                  commitLock.wait();
               } else {
                  commitLock.wait(delay);
               }
            } catch (Exception e) {
               //ignore
            }
            transactionToBlock = null;
            setCommitBlocked(false);
         }
      }

      public void unblock() {
         synchronized (commitLock) {
            commitLock.notify();
         }
      }

      private void setCommitBlocked(boolean value) {
         synchronized (hasCommitLock) {
            hasCommitBlocked = value;
            hasCommitLock.notifyAll();
         }
      }
   }

   protected class ObtainTransactionEntry extends BaseCustomInterceptor {
      private final TransactionCommitManager transactionCommitManager;
      private SortedTransactionQueue.TransactionEntry transactionEntry;
      private Thread expectedThread;

      public ObtainTransactionEntry(Cache<?, ?> cache) {
         this.transactionCommitManager = cache.getAdvancedCache().getComponentRegistry()
               .getComponent(TransactionCommitManager.class);
         cache.getAdvancedCache().addInterceptorAfter(this, TxInterceptor.class);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         setTransactionEntry(transactionCommitManager.getTransactionEntry(command.getGlobalTransaction()));
         return invokeNextInterceptor(ctx, command);
      }

      public synchronized SortedTransactionQueue.TransactionEntry getTransactionEntry() throws InterruptedException {
         while (transactionEntry == null) {
            wait();
         }
         return transactionEntry;
      }

      private synchronized void setTransactionEntry(SortedTransactionQueue.TransactionEntry transactionEntry) {
         if (Thread.currentThread().equals(expectedThread)) {
            log.debugf("Setting transactions entry: %s", transactionEntry);
            this.transactionEntry = transactionEntry;
            notifyAll();
         } else {
            log.debugf("Not setting transaction entry. Thread does not match %s and %s.", expectedThread, Thread.currentThread());
         }
      }

      public synchronized void expectedThisThread() {
         this.expectedThread = Thread.currentThread();
      }

      public synchronized void reset() {
         transactionEntry = null;
      }
   }

}
