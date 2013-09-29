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
package org.infinispan.tx.gmu.gc;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.gmu.GMUDataContainer;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.transaction.gmu.manager.GarbageCollectorManager;
import org.infinispan.tx.gmu.AbstractGMUTest;
import org.testng.annotations.Test;

import javax.transaction.Transaction;

import static junit.framework.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.gc.DistSimpleGarbageCollectorTest")
public class DistSimpleGarbageCollectorTest extends AbstractGMUTest {

   public DistSimpleGarbageCollectorTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testKeepMostRecent() {
      assertAtLeastCaches(2);
      final Object key = newKey(0, 1);
      assertKeyOwners(key, 0, 1);
      assertCacheValuesNull(key);
      logKeysUsedInTest("testKeepMostRecent", key);

      GarbageCollectorManager gcManager = getComponent(0, GarbageCollectorManager.class);
      final GMUDataContainer gmuDataContainer = (GMUDataContainer) getComponent(0, DataContainer.class);

      put(0, key, VALUE_1, null);
      put(0, key, VALUE_2, VALUE_1);
      put(0, key, VALUE_3, VALUE_2);

      assertNoTransactions();
      assert gmuDataContainer.getVersionChain(key).numberOfVersion() == 3 : "Wrong number of versions in version chain";
      //no transactions are running... so the garbage collect should only keep the most recent one
      gcManager.triggerVersionGarbageCollection();
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return gmuDataContainer.getVersionChain(key).numberOfVersion() == 1;
         }
      });

      assertEquals(VALUE_3, cache(0).get(key));
      assertEquals(VALUE_3, cache(1).get(key));

      assertNoTransactions();
   }

   public void testKeepAll() throws Exception {
      assertAtLeastCaches(2);
      final Object key1 = newKey(1, 0);
      final Object key2 = newKey(1, 0);
      final Object key3 = newKey(1, 0);

      assertKeyOwners(key1, 1, 0);
      assertKeyOwners(key2, 1, 0);
      assertKeyOwners(key3, 1, 0);

      logKeysUsedInTest("testKeepAll", key1, key2, key3);

      assertCacheValuesNull(key1, key2, key3);

      GarbageCollectorManager gcManager = getComponent(1, GarbageCollectorManager.class);
      final GMUDataContainer gmuDataContainer = (GMUDataContainer) getComponent(1, DataContainer.class);

      put(0, key1, VALUE_1, null);
      put(0, key2, VALUE_1, null);
      put(0, key3, VALUE_1, null);

      put(0, key1, VALUE_2, VALUE_1);
      put(0, key2, VALUE_2, VALUE_1);
      put(0, key3, VALUE_2, VALUE_1);

      tm(0).begin();
      assertEquals(VALUE_2, cache(0).get(key3));
      Transaction readOnlyTx = tm(0).suspend();

      put(0, key1, VALUE_3, VALUE_2);
      put(0, key2, VALUE_3, VALUE_2);
      put(0, key3, VALUE_3, VALUE_2);

      put(0, key1, VALUE_1, VALUE_3);
      put(0, key2, VALUE_1, VALUE_3);
      put(0, key3, VALUE_1, VALUE_3);

      assert gmuDataContainer.getVersionChain(key1).numberOfVersion() == 4 : "Wrong Number Of Versions for " + key1;
      assert gmuDataContainer.getVersionChain(key2).numberOfVersion() == 4 : "Wrong Number Of Versions for " + key2;
      assert gmuDataContainer.getVersionChain(key3).numberOfVersion() == 4 : "Wrong Number Of Versions for " + key3;

      gcManager.triggerVersionGarbageCollection();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return gmuDataContainer.getVersionChain(key1).numberOfVersion() == 3 &&
                  gmuDataContainer.getVersionChain(key2).numberOfVersion() == 3 &&
                  gmuDataContainer.getVersionChain(key3).numberOfVersion() == 3;
         }
      });

      tm(0).resume(readOnlyTx);
      assertEquals(VALUE_2, cache(0).get(key1));
      assertEquals(VALUE_2, cache(0).get(key2));
      assertEquals(VALUE_2, cache(0).get(key3));
      tm(0).commit();

      assertNoTransactions();

      gcManager.triggerVersionGarbageCollection();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return gmuDataContainer.getVersionChain(key1).numberOfVersion() == 1 &&
                  gmuDataContainer.getVersionChain(key2).numberOfVersion() == 1 &&
                  gmuDataContainer.getVersionChain(key3).numberOfVersion() == 1;
         }
      });

      assertNoTransactions();
   }

   //TODO fix the test.... it has no tx executed so the min view is the -1... no garbage collection is performed
   @Test(enabled = false)
   public void testViewIdGC() {
      final GarbageCollectorManager garbageCollectorManager = getComponent(0, GarbageCollectorManager.class);
      final ClusterTopologyManager clusterTopologyManager = getComponent(0, ClusterTopologyManager.class);
      final VersionGenerator versionGenerator = getComponent(0, VersionGenerator.class);

      assert clusterTopologyManager.getCacheHistorySize(cache(0).getName()) == initialClusterSize();
      //they add the view -1 that only contains himself (before received the cache view)
      assert versionGenerator.getViewHistorySize() == initialClusterSize() + 1;

      garbageCollectorManager.triggerViewGarbageCollection();

      try {
         log.info("Sleeping 10 seconds (view back-off time)");
         Thread.sleep(10000);
      } catch (InterruptedException e) {
         return;
      }

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return clusterTopologyManager.getCacheHistorySize(cache(0).getName()) == 1 &&
                  versionGenerator.getViewHistorySize() == 1;
         }
      });
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.garbageCollector().enabled(true).viewGCBackOff(10);
      builder.clustering().l1().disable();
      builder.clustering().hash().numOwners(1);
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
      return CacheMode.DIST_SYNC;
   }
}
