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
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import java.util.concurrent.CountDownLatch;

import static junit.framework.Assert.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.ConsistencyTest")
public class ConsistencyTest extends AbstractGMUTest {

   public void testGetSnapshot() throws Exception {
      assertAtLeastCaches(2);

      Object cache0Key0 = newKey(0, 1);
      Object cache1Key1 = newKey(1, 0);
      Object cache1Key2 = newKey(1, 0);

      logKeysUsedInTest("testGetSnapshot", cache0Key0, cache1Key1, cache1Key2);

      assertKeyOwners(cache0Key0, 0, 1);
      assertKeyOwners(cache1Key1, 1, 0);
      assertKeyOwners(cache1Key2, 1, 0);
      assertCacheValuesNull(cache0Key0, cache1Key1, cache1Key2);

      tm(0).begin();
      txPut(0, cache0Key0, VALUE_1, null);
      txPut(0, cache1Key1, VALUE_1, null);
      txPut(0, cache1Key2, VALUE_1, null);
      tm(0).commit();

      assertCachesValue(0, cache0Key0, VALUE_1);
      assertCachesValue(0, cache1Key1, VALUE_1);
      assertCachesValue(0, cache1Key2, VALUE_1);

      tm(0).begin();
      assert VALUE_1.equals(cache(0).get(cache0Key0));
      Transaction tx0 = tm(0).suspend();

      tm(1).begin();
      cache(1).put(cache0Key0, VALUE_2);
      cache(1).put(cache1Key1, VALUE_2);
      cache(1).put(cache1Key2, VALUE_2);
      tm(1).commit();

      assertCachesValue(1, cache0Key0, VALUE_2);
      assertCachesValue(1, cache1Key1, VALUE_2);
      assertCachesValue(1, cache1Key2, VALUE_2);

      tm(0).resume(tx0);
      assert VALUE_1.equals(cache(0).get(cache0Key0));
      assert VALUE_1.equals(cache(0).get(cache1Key1));
      assert VALUE_1.equals(cache(0).get(cache1Key2));
      tm(0).commit();

      assertNoTransactions();
      printDataContainer();
   }

   public void testPrematureAbort() throws Exception {
      assertAtLeastCaches(2);

      Object cache0Key = newKey(0, 1);
      Object cache1Key = newKey(1, 0);

      logKeysUsedInTest("testPrematureAbort", cache0Key, cache1Key);

      assertKeyOwners(cache0Key, 0, 1);
      assertKeyOwners(cache1Key, 1, 0);
      assertCacheValuesNull(cache0Key, cache1Key);

      tm(0).begin();
      txPut(0, cache0Key, VALUE_1, null);
      txPut(0, cache1Key, VALUE_1, null);
      tm(0).commit();


      tm(0).begin();
      Object value = cache(0).get(cache0Key);
      assertEquals(VALUE_1, value);
      Transaction tx0 = tm(0).suspend();

      tm(1).begin();
      txPut(1, cache0Key, VALUE_2, VALUE_1);
      txPut(1, cache1Key, VALUE_2, VALUE_1);
      tm(1).commit();

      tm(0).resume(tx0);
      txPut(0, cache0Key, VALUE_3, VALUE_1);
      try {
         cache(0).get(cache1Key);
         assert false : "Expected to abort read write transaction";
      } catch (Exception e) {
         safeRollback(0);
      }

      printDataContainer();
      assertNoTransactions();
   }

   public void testConflictingTxs() throws Exception {
      assertAtLeastCaches(2);

      Object cache0Key = newKey(0, 1);
      Object cache1Key = newKey(1, 0);

      logKeysUsedInTest("testConflictingTxs", cache0Key, cache1Key);

      assertKeyOwners(cache0Key, 0, 1);
      assertKeyOwners(cache1Key, 1, 0);
      assertCacheValuesNull(cache0Key, cache1Key);

      tm(0).begin();
      txPut(0, cache0Key, VALUE_1, null);
      txPut(0, cache1Key, VALUE_1, null);
      tm(0).commit();


      tm(0).begin();
      Object value = cache(0).get(cache0Key);
      assertEquals(VALUE_1, value);
      Transaction tx0 = tm(0).suspend();

      tm(1).begin();
      txPut(1, cache0Key, VALUE_2, VALUE_1);
      txPut(1, cache1Key, VALUE_2, VALUE_1);
      tm(1).commit();

      tm(0).resume(tx0);
      try {
         txPut(0, cache0Key, VALUE_3, VALUE_1);
         txPut(0, cache1Key, VALUE_3, VALUE_1);
         tm(0).commit();
         assert false : "Expected to abort conflicting transaction";
      } catch (Exception e) {
         safeRollback(0);
      }

      printDataContainer();
      assertNoTransactions();
   }

   public void testTimeoutCleanup() throws Exception {
      assertAtLeastCaches(2);
      final CountDownLatch block = new CountDownLatch(1);
      final CommandInterceptor interceptor = new BaseCustomInterceptor() {
         @Override
         public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
            block.await();
            return invokeNextInterceptor(ctx, command);
         }
      };
      final InterceptorChain chain = TestingUtil.extractComponent(cache(1), InterceptorChain.class);
      final Object key1 = newKey(1);
      final Object key2 = initialClusterSize() > 2 ? newKey(2) : newKey(0);
      try {
         chain.addInterceptor(interceptor, 0);
         tm(0).begin();
         cache(0).put(key1, VALUE_1);
         cache(0).put(key2, VALUE_1);
         tm(0).commit();
         fail("Rollback expected!");
      } catch (RollbackException e) {
         //expected
      } finally {
         block.countDown();
         chain.removeInterceptor(0);
      }

      cache(0).put(key1, VALUE_2);
      cache(0).put(key2, VALUE_2);

      assertCachesValue(0, key1, VALUE_2);
      assertCachesValue(0, key2, VALUE_2);

      assertNoTransactions();
      assertNoLocks();
   }

   public void testRemove() throws Exception {
      assertAtLeastCaches(2);

      Object key1 = newKey(1, 0);

      logKeysUsedInTest("testRemove", key1);

      assertKeyOwners(key1, 1, 0);
      assertCacheValuesNull(key1);

      tm(0).begin();
      txPut(0, key1, VALUE_1, null);
      tm(0).commit();

      assertCachesValue(0, key1, VALUE_1);

      tm(0).begin();
      assertEquals("Wrong value for key.", VALUE_1, cache(0).get(key1));
      txRemove(0, key1, VALUE_1);
      assertNull("Expected key to be removed.", cache(0).get(key1));
      tm(0).commit();

      assertCachesValue(0, key1, null);

      assertNoTransactions();
      printDataContainer();
   }

   public void testRemoveOnOwner() throws Exception {
      assertAtLeastCaches(2);

      Object key1 = newKey(0, 1);

      logKeysUsedInTest("testRemoveOnOwner", key1);

      assertKeyOwners(key1, 0, 1);
      assertCacheValuesNull(key1);

      tm(0).begin();
      txPut(0, key1, VALUE_1, null);
      tm(0).commit();

      assertCachesValue(0, key1, VALUE_1);

      tm(0).begin();
      assertEquals("Wrong value for key.", VALUE_1, cache(0).get(key1));
      txRemove(0, key1, VALUE_1);
      assertNull("Expected key to be removed.", cache(0).get(key1));
      tm(0).commit();

      assertCachesValue(0, key1, null);

      assertNoTransactions();
      printDataContainer();
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      //no-op
   }

   @Override
   protected int initialClusterSize() {
      return 2;
   }

   @Override
   protected boolean syncCommitPhase() {
      return true;
   }

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.REPL_SYNC;
   }

   protected void assertNoLocks() {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (Cache cache : caches()) {
               LockManager lockManager = TestingUtil.extractLockManager(cache);
               if (lockManager.getNumberOfLocksHeld() != 0) {
                  return false;
               }
            }
            return true;
         }
      });
   }
}
