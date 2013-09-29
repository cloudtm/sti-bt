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
package org.infinispan.tx.gmu.totalorder;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.totalorder.GMUTotalOrderManager;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.tx.gmu.DistSimpleTest2;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.totalorder.TotalOrderDistSimpleTest2")
public class TotalOrderDistSimpleTest2 extends DistSimpleTest2 {

   public void testTotalOrderManager() {
      TotalOrderManager totalOrderManager = TestingUtil.extractComponent(cache(0), TotalOrderManager.class);
      Assert.assertTrue(totalOrderManager instanceof GMUTotalOrderManager, "Wrong total order manager");
   }

   public void testReadReadDependency() throws Exception {
      final Object key01 = newKey(0);
      final Object key02 = newKey(0);
      final Object key11 = newKey(1);
      final Object key12 = newKey(1);

      //init the keys
      tm(0).begin();
      cache(0).put(key01, VALUE_1);
      cache(0).put(key02, VALUE_1);
      cache(0).put(key11, VALUE_1);
      cache(0).put(key12, VALUE_1);
      tm(0).commit();

      try {
         final CommandBlocker blocker = addCommandBlocker(cache(0));
         blocker.reset();

         tm(0).begin();
         //RS={01,02,12}, WS={01}
         Assert.assertEquals(cache(0).get(key01), VALUE_1);
         Assert.assertEquals(cache(0).get(key02), VALUE_1);
         Assert.assertEquals(cache(0).get(key12), VALUE_1);
         final GlobalTransaction gtx1 = globalTransaction(0);
         final Transaction tx1 = tm(0).suspend();

         tm(0).begin();
         //RS={02,11,12}, WS={11}
         Assert.assertEquals(cache(0).get(key02), VALUE_1);
         Assert.assertEquals(cache(0).get(key11), VALUE_1);
         Assert.assertEquals(cache(0).get(key12), VALUE_1);
         final GlobalTransaction gtx2 = globalTransaction(0);
         final Transaction tx2 = tm(0).suspend();

         blocker.block(gtx1);
         blocker.block(gtx2);

         final Future<Boolean> ftx1 = fork(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
               tm(0).resume(tx1);
               cache(0).put(key01, VALUE_2);
               tm(0).commit();
               return Boolean.TRUE;
            }
         });

         final Future<Boolean> ftx2 = fork(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
               tm(0).resume(tx2);
               cache(0).put(key11, VALUE_2);
               tm(0).commit();
               return Boolean.TRUE;
            }
         });

         Assert.assertTrue(blocker.await(gtx1, 10000), gtx1 + " was never received!");
         Assert.assertTrue(blocker.await(gtx2, 10000), gtx2 + " was never received!");

         blocker.unblock(gtx1);
         blocker.unblock(gtx2);

         Assert.assertTrue(ftx1.get());
         Assert.assertTrue(ftx2.get());

         assertCachesValue(0, key01, VALUE_2);
         assertCachesValue(0, key02, VALUE_1);
         assertCachesValue(0, key11, VALUE_2);
         assertCachesValue(0, key12, VALUE_1);

         assertNoTransactions();
         assertNoLocks();
      } finally {
         removeCommandBlocked(cache(0));
      }
   }

   public void testReadWriteDependency() throws Exception {
      final Object key01 = newKey(0);
      final Object key02 = newKey(0);
      final Object key11 = newKey(1);
      final Object key12 = newKey(1);

      //init the keys
      tm(0).begin();
      cache(0).put(key01, VALUE_1);
      cache(0).put(key02, VALUE_1);
      cache(0).put(key11, VALUE_1);
      cache(0).put(key12, VALUE_1);
      tm(0).commit();

      try {
         final CommandBlocker blocker = addCommandBlocker(cache(0));
         blocker.reset();

         tm(0).begin();
         //RS={01,11,02,12}, WS={01}
         Assert.assertEquals(cache(0).get(key01), VALUE_1);
         Assert.assertEquals(cache(0).get(key11), VALUE_1);
         Assert.assertEquals(cache(0).get(key02), VALUE_1);
         Assert.assertEquals(cache(0).get(key12), VALUE_1);
         final GlobalTransaction gtx1 = globalTransaction(0);
         final Transaction tx1 = tm(0).suspend();

         tm(0).begin();
         //RS={02,11,12}, WS={11} //writes on a key read by the other
         Assert.assertEquals(cache(0).get(key02), VALUE_1);
         Assert.assertEquals(cache(0).get(key11), VALUE_1);
         Assert.assertEquals(cache(0).get(key12), VALUE_1);
         final GlobalTransaction gtx2 = globalTransaction(0);
         final Transaction tx2 = tm(0).suspend();

         blocker.block(gtx1);
         blocker.block(gtx2);

         final Future<Boolean> ftx1 = fork(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
               tm(0).resume(tx1);
               cache(0).put(key01, VALUE_2);
               tm(0).commit();
               return Boolean.TRUE;
            }
         });

         Assert.assertTrue(blocker.await(gtx1, 10000), gtx1 + " was never received!");

         final Future<Boolean> ftx2 = fork(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
               tm(0).resume(tx2);
               cache(0).put(key11, VALUE_2);
               tm(0).commit();
               return Boolean.TRUE;
            }
         });

         Assert.assertFalse(blocker.await(gtx2, 5000), gtx2 + " was received and it should block");

         blocker.unblock(gtx1);
         blocker.unblock(gtx2);

         Assert.assertTrue(ftx1.get());
         Assert.assertTrue(ftx2.get());

         assertCachesValue(0, key01, VALUE_2);
         assertCachesValue(0, key02, VALUE_1);
         assertCachesValue(0, key11, VALUE_2);
         assertCachesValue(0, key12, VALUE_1);

         assertNoTransactions();
         assertNoLocks();
      } finally {
         removeCommandBlocked(cache(0));
      }
   }

   public void testWriteWriteDependency() throws Exception {
      final Object key01 = newKey(0);
      final Object key02 = newKey(0);
      final Object key11 = newKey(1);
      final Object key12 = newKey(1);

      //init the keys
      tm(0).begin();
      cache(0).put(key01, VALUE_1);
      cache(0).put(key02, VALUE_1);
      cache(0).put(key11, VALUE_1);
      cache(0).put(key12, VALUE_1);
      tm(0).commit();

      try {
         final CommandBlocker blocker = addCommandBlocker(cache(0));
         blocker.reset();

         tm(0).begin();
         //RS={01,11,12}, WS={01}
         Assert.assertEquals(cache(0).get(key02), VALUE_1);
         Assert.assertEquals(cache(0).get(key11), VALUE_1);
         Assert.assertEquals(cache(0).get(key12), VALUE_1);
         final GlobalTransaction gtx1 = globalTransaction(0);
         final Transaction tx1 = tm(0).suspend();

         tm(0).begin();
         //RS={02,11,12}, WS={01} //writes on a key write by the other
         Assert.assertEquals(cache(0).get(key02), VALUE_1);
         Assert.assertEquals(cache(0).get(key11), VALUE_1);
         Assert.assertEquals(cache(0).get(key12), VALUE_1);
         final GlobalTransaction gtx2 = globalTransaction(0);
         final Transaction tx2 = tm(0).suspend();

         blocker.block(gtx1);
         blocker.block(gtx2);

         final Future<Boolean> ftx1 = fork(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
               tm(0).resume(tx1);
               cache(0).put(key01, VALUE_2);
               tm(0).commit();
               return Boolean.TRUE;
            }
         });

         Assert.assertTrue(blocker.await(gtx1, 10000), gtx1 + " was never received!");

         final Future<Boolean> ftx2 = fork(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
               tm(0).resume(tx2);
               cache(0).put(key01, VALUE_2);
               tm(0).commit();
               return Boolean.TRUE;
            }
         });

         Assert.assertFalse(blocker.await(gtx2, 5000), gtx2 + " was received and it should block");

         blocker.unblock(gtx1);
         blocker.unblock(gtx2);

         Assert.assertTrue(ftx1.get());
         try {
            ftx2.get();
            Assert.fail("Transaction should fail!");
         } catch (Exception e) {
            //expected
         }

         assertCachesValue(0, key01, VALUE_2);
         assertCachesValue(0, key02, VALUE_1);
         assertCachesValue(0, key11, VALUE_1);
         assertCachesValue(0, key12, VALUE_1);

         assertNoTransactions();
         assertNoLocks();
      } finally {
         removeCommandBlocked(cache(0));
      }
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      super.decorate(builder);
      builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER)
            .recovery().disable();
   }

   protected final void assertNoLocks() {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (Cache cache : caches()) {
               if (TestingUtil.extractComponent(cache, TotalOrderManager.class).hasAnyLockAcquired()) {
                  return false;
               }
            }
            return true;
         }
      });
   }

   private CommandBlocker addCommandBlocker(Cache cache) {
      InterceptorChain chain = TestingUtil.extractComponent(cache, InterceptorChain.class);
      CommandBlocker blocker = new CommandBlocker();
      chain.addInterceptor(blocker, 0);
      return blocker;
   }

   private void removeCommandBlocked(Cache cache) {
      InterceptorChain chain = TestingUtil.extractComponent(cache, InterceptorChain.class);
      chain.removeInterceptor(CommandBlocker.class);
   }
}
