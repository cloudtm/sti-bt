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
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.gmu.GMUDistributionInterceptor;
import org.infinispan.interceptors.gmu.GMUEntryWrappingInterceptor;
import org.infinispan.interceptors.gmu.GMUReplicationInterceptor;
import org.infinispan.interceptors.gmu.TotalOrderGMUDistributionInterceptor;
import org.infinispan.interceptors.gmu.TotalOrderGMUEntryWrappingInterceptor;
import org.infinispan.interceptors.gmu.TotalOrderGMUReplicationInterceptor;
import org.infinispan.interceptors.locking.OptimisticReadWriteLockingInterceptor;
import org.infinispan.reconfigurableprotocol.protocol.PassiveReplicationCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TotalOrderCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TwoPhaseCommitProtocol;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.SimpleTest")
public class SimpleTest extends AbstractGMUTest {

   public void testPut() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1, KEY_2, KEY_3);

      put(0, KEY_1, VALUE_1, null);

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(KEY_2, VALUE_2);
      map.put(KEY_3, VALUE_3);

      putAll(1, map);

      assertNoTransactions();
      printDataContainer();
   }

   public void testPut2() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      put(0, KEY_1, VALUE_1, null);
      put(0, KEY_1, VALUE_2, VALUE_1);
      put(0, KEY_1, VALUE_3, VALUE_2);
      put(0, KEY_1, VALUE_3, VALUE_3);

      assertNoTransactions();
      printDataContainer();
   }

   public void removeTest() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      put(1, KEY_1, VALUE_1, null);
      remove(0, KEY_1, VALUE_1);
      put(0, KEY_1, VALUE_2, null);
      remove(1, KEY_1, VALUE_2);

      printDataContainer();
      assertNoTransactions();
   }

   public void testPutIfAbsent() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1, KEY_2);

      put(1, KEY_1, VALUE_1, null);
      putIfAbsent(0, KEY_1, VALUE_2, VALUE_1, VALUE_1);
      put(1, KEY_1, VALUE_2, VALUE_1);
      putIfAbsent(0, KEY_1, VALUE_3, VALUE_2, VALUE_2);      
      putIfAbsent(0, KEY_2, VALUE_3, null, VALUE_3);

      printDataContainer();
      assertNoTransactions();
   }

   public void testRemoveIfPresent() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      put(0, KEY_1, VALUE_1, null);
      put(1, KEY_1, VALUE_2, VALUE_1);
      removeIf(0, KEY_1, VALUE_1, VALUE_2, false);
      removeIf(0, KEY_1, VALUE_2, null, true);

      printDataContainer();
      assertNoTransactions();
   }

   public void testClear() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      put(0, KEY_1, VALUE_1, null);

      cache(0).clear();
      assertCachesValue(0, KEY_1, null);

      printDataContainer();
      assertNoTransactions();
   }

   public void testReplace() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      put(1, KEY_1, VALUE_1, null);
      replace(0, KEY_1, VALUE_2, VALUE_1);
      put(0, KEY_1, VALUE_3, VALUE_2);
      replace(0, KEY_1, VALUE_3, VALUE_3);

      printDataContainer();
      assertNoTransactions();
   }

   public void testReplaceWithOldVal() {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1);

      put(1, KEY_1, VALUE_2, null);
      put(0, KEY_1, VALUE_3, VALUE_2);
      replaceIf(0, KEY_1, VALUE_1, VALUE_2, VALUE_3, false);
      replaceIf(0, KEY_1, VALUE_1, VALUE_3, VALUE_1, true);

      printDataContainer();
      assertNoTransactions();
   }

   public void testRemoveUnexistingEntry() {
      assertAtLeastCaches(1);
      assertCacheValuesNull(KEY_1);

      remove(0, KEY_1, null);

      assertNoTransactions();
   }

   public void testInterceptorChain() {
      InterceptorChain ic = TestingUtil.extractComponent(cache(0), InterceptorChain.class);

      if (cacheMode().isReplicated()) {
         assertTrue(ic.containsInterceptorType(GMUReplicationInterceptor.class, TwoPhaseCommitProtocol.UID));
         assertTrue(ic.containsInterceptorType(GMUReplicationInterceptor.class, PassiveReplicationCommitProtocol.UID));
         assertTrue(ic.containsInterceptorType(TotalOrderGMUReplicationInterceptor.class, TotalOrderCommitProtocol.UID));
      } else {
         assertTrue(ic.containsInterceptorType(GMUDistributionInterceptor.class, TwoPhaseCommitProtocol.UID));
         assertTrue(ic.containsInterceptorType(GMUDistributionInterceptor.class, PassiveReplicationCommitProtocol.UID));
         assertTrue(ic.containsInterceptorType(TotalOrderGMUDistributionInterceptor.class, TotalOrderCommitProtocol.UID));
      }
      assertTrue(ic.containsInterceptorType(GMUEntryWrappingInterceptor.class, TwoPhaseCommitProtocol.UID));
      assertTrue(ic.containsInterceptorType(GMUEntryWrappingInterceptor.class, PassiveReplicationCommitProtocol.UID));
      assertTrue(ic.containsInterceptorType(TotalOrderGMUEntryWrappingInterceptor.class, TotalOrderCommitProtocol.UID));

      assertTrue(ic.containsInterceptorType(OptimisticReadWriteLockingInterceptor.class, TwoPhaseCommitProtocol.UID));
      assertTrue(ic.containsInterceptorType(OptimisticReadWriteLockingInterceptor.class, PassiveReplicationCommitProtocol.UID));
      assertFalse(ic.containsInterceptorType(OptimisticReadWriteLockingInterceptor.class, TotalOrderCommitProtocol.UID));
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
}
