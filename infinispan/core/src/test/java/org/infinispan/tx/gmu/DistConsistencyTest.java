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

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;
import static org.infinispan.transaction.gmu.manager.SortedTransactionQueue.TransactionEntry;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.DistConsistencyTest")
public class DistConsistencyTest extends ConsistencyTest {

   public void testWaitingForLocalCommit() throws Exception {
      if (cacheManagers.size() > 2) {
         //Note: this test is not determinist with more than 2 caches
         return;
      }
      assertAtLeastCaches(2);

      final DelayCommit delayCommit = addDelayCommit(0, 5000);
      final ObtainTransactionEntry obtainTransactionEntry = new ObtainTransactionEntry(cache(1));

      final Object cache0Key = newKey(0, 1);
      final Object cache1Key = newKey(1, 0);

      logKeysUsedInTest("testWaitingForLocalCommit", cache0Key, cache1Key);

      assertKeyOwners(cache0Key, 0, 1);
      assertKeyOwners(cache1Key, 1, 0);
      assertCacheValuesNull(cache0Key, cache1Key);

      tm(0).begin();
      txPut(0, cache0Key, VALUE_1, null);
      txPut(0, cache1Key, VALUE_1, null);
      tm(0).commit();

      Thread otherThread = new Thread("TestWaitingForLocalCommit-Thread") {
         @Override
         public void run() {
            try {
               tm(1).begin();
               txPut(1, cache0Key, VALUE_2, VALUE_1);
               txPut(1, cache1Key, VALUE_2, VALUE_1);
               obtainTransactionEntry.expectedThisThread();
               delayCommit.blockTransaction(globalTransaction(1));
               tm(1).commit();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      };
      obtainTransactionEntry.reset();
      otherThread.start();
      TransactionEntry transactionEntry = obtainTransactionEntry.getTransactionEntry();
      transactionEntry.awaitUntilCommitted();

      tm(0).begin();
      assertEquals(VALUE_2, cache(0).get(cache1Key));
      assertEquals(VALUE_2, cache(0).get(cache0Key));
      tm(0).commit();

      delayCommit.unblock();
      otherThread.join();

      printDataContainer();
      assertNoTransactions();
      cache(0).getAdvancedCache().removeInterceptor(DelayCommit.class);
      cache(1).getAdvancedCache().removeInterceptor(ObtainTransactionEntry.class);
   }

   public void testWaitInRemoteNode() throws Exception {
      if (cacheManagers.size() > 2) {
         //Note: this test is not determinist with more than 2 caches
         return;
      }
      assertAtLeastCaches(2);

      final DelayCommit delayCommit = addDelayCommit(0, 5000);

      final ObtainTransactionEntry obtainTransactionEntry = new ObtainTransactionEntry(cache(1));

      final Object cache0Key = newKey(0, 1);
      final Object cache1Key = newKey(1, 0);

      logKeysUsedInTest("testWaitInRemoteNode", cache0Key, cache1Key);

      assertKeyOwners(cache0Key, 0, 1);
      assertKeyOwners(cache1Key, 1, 0);
      assertCacheValuesNull(cache0Key, cache1Key);

      tm(0).begin();
      txPut(0, cache0Key, VALUE_1, null);
      txPut(0, cache1Key, VALUE_1, null);
      tm(0).commit();

      Thread otherThread = new Thread("TestWaitingForLocalCommit-Thread") {
         @Override
         public void run() {
            try {
               tm(1).begin();
               txPut(1, cache0Key, VALUE_2, VALUE_1);
               txPut(1, cache1Key, VALUE_2, VALUE_1);
               delayCommit.blockTransaction(globalTransaction(1));
               obtainTransactionEntry.expectedThisThread();
               tm(1).commit();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      };
      obtainTransactionEntry.reset();
      otherThread.start();
      TransactionEntry transactionEntry = obtainTransactionEntry.getTransactionEntry();
      transactionEntry.awaitUntilCommitted();

      //tx already committed in cache(1). start a read only on cache(1) reading the local key and them the remote key.
      // the remote get should wait until the transaction is committed
      tm(1).begin();
      assertEquals(VALUE_2, cache(1).get(cache1Key));
      assertEquals(VALUE_2, cache(1).get(cache0Key));
      tm(1).commit();

      delayCommit.unblock();
      otherThread.join();

      printDataContainer();
      assertNoTransactions();
      cache(0).getAdvancedCache().removeInterceptor(DelayCommit.class);
      cache(1).getAdvancedCache().removeInterceptor(ObtainTransactionEntry.class);
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().hash().numOwners(1);
   }

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
