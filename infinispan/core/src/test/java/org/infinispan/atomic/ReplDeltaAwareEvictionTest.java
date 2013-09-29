/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
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
package org.infinispan.atomic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
@Test(groups = "functional", testName = "atomic.ReplDeltaAwareEvictionTest")
@CleanupAfterMethod
public class ReplDeltaAwareEvictionTest extends LocalDeltaAwareEvictionTest {

   public ReplDeltaAwareEvictionTest() {
      txEnabled = true;
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true, true);
      builder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.PESSIMISTIC)
            .transactionManagerLookup(new JBossStandaloneJTAManagerLookup())
            .eviction().maxEntries(1).strategy(EvictionStrategy.LRU)
            .loaders()
            .addStore().cacheStore(new DummyInMemoryCacheStore())
            .fetchPersistentState(false);

      addClusterEnabledCacheManager(builder);

      builder.loaders().clearCacheLoaders()
            .addStore().cacheStore(new DummyInMemoryCacheStore()).fetchPersistentState(false);

      addClusterEnabledCacheManager(builder);

      waitForClusterToForm();
   }

   public void testDeltaAware() throws Exception {
      test(createDeltaAwareAccessor(), 0, 1);
   }

   public void testDeltaAware2() throws Exception {
      test(createDeltaAwareAccessor(), 1, 0);
   }

   public void testAtomicMap() throws Exception {
      test(createAtomicMapAccessor(), 0, 1);
   }

   public void testAtomicMap2() throws Exception {
      test(createAtomicMapAccessor(), 1, 0);
   }

   public void testFineGrainedAtomicMap() throws Exception {
      test(createFineGrainedAtomicMapAccessor(), 0, 1);
   }

   public void testFineGrainedAtomicMap2() throws Exception {
      test(createFineGrainedAtomicMapAccessor(), 1, 0);
   }
}
