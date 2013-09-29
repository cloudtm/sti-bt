/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.tx.gmu.cacheloader;

import com.arjuna.ats.jta.exceptions.RollbackException;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.transaction.Transaction;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional")
public abstract class BaseGMUCacheStoreTest extends MultipleCacheManagersTest {

   protected static final int NUM_KEYS = 100;
   private static final int NUM_NODES = 2;
   protected final CacheStore[] cacheStores = new CacheStore[NUM_NODES];
   protected final Object keys[] = new Object[NUM_KEYS];
   protected final Object values[] = new Object[NUM_KEYS];

   public void testCacheStoreAndLoader() throws Exception {
      for (int i = 0; i < NUM_KEYS; ++i) {
         cache(0).put(getKey(i), getValue(i));
      }
      for (Cache cache : caches()) {
         for (int i = 0; i < NUM_KEYS; ++i) {
            Assert.assertEquals(cache.get(getKey(i)), getValue(i), "Wrong value read in single read.");
         }
      }
      for (int i = 0; i < NUM_NODES; ++i) {
         tm(i).begin();
         for (int j = 0; j < NUM_KEYS; ++j) {
            Assert.assertEquals(cache(i).get(getKey(j)), getValue(j), "Wrong value read in tx read.");
         }
         tm(i).commit();
      }

      tm(0).begin();
      for (int i = 0; i < NUM_KEYS; ++i) {
         Assert.assertEquals(cache(0).get(getKey(i)), getValue(i), "Wrong value read in tx read.");
         Assert.assertEquals(cache(0).put(getKey(i), getValue(i + 1)), getValue(i), "Wrong value returned by put.");
      }
      tm(0).commit();

      tm(1).begin();
      for (int i = 0; i < NUM_KEYS; ++i) {
         Assert.assertEquals(cache(1).get(getKey(i)), getValue(i + 1), "Wrong value read in tx read.");
         Assert.assertEquals(cache(1).put(getKey(i), getValue(i + 2)), getValue(i + 1), "Wrong value returned by put.");
      }
      tm(1).commit();

      for (Cache cache : caches()) {
         for (int i = 0; i < NUM_KEYS; ++i) {
            Assert.assertEquals(cache.get(getKey(i)), getValue(i + 2), "Wrong value read in single read.");
         }
      }
      for (int i = 0; i < NUM_NODES; ++i) {
         tm(i).begin();
         for (int j = 0; j < NUM_KEYS; ++j) {
            Assert.assertEquals(cache(i).get(getKey(j)), getValue(j + 2), "Wrong value read in tx read.");
         }
         tm(i).commit();
      }

   }

   public void testTransactionConsistency() throws Exception {
      for (int i = 0; i < NUM_KEYS; ++i) {
         cache(0).put(getKey(i), getValue(i));
      }

      Object key1 = null, key2 = null;
      Object value1 = null, value2 = null;

      for (int i = 0; i < NUM_KEYS; ++i) {
         if (key1 == null && isOwner(getKey(i), cache(0))) {
            key1 = getKey(i);
            value1 = getValue(i);
         } else if (key2 == null && isOwner(getKey(i), cache(0))) {
            key2 = getKey(i);
            value2 = getValue(i);
         } else if (key1 != null && key2 != null) {
            break;
         }
      }

      Assert.assertNotNull(key1);
      Assert.assertNotNull(key2);

      tm(0).begin();
      Assert.assertEquals(cache(0).get(key1), value1, "Wrong value read.");
      Transaction tx = tm(0).suspend();

      //update the key
      cache(0).put(key2, getValue(0));
      //force the key to be only in cache store.
      cache(0).evict(key2);

      //the tx should abort because the key is not in memory and the value in cache store is not visible.
      try {
         tm(0).resume(tx);
         Assert.assertEquals(cache(0).get(key2), value2, "Wrong value read");
         tm(0).commit();
         Assert.fail("The transaction should abort");
      } catch (RollbackException e) {
         Assert.fail("RollbackException not expected. the transaction should abort before the commit");
      } catch (CacheException e) {
         //expected
         tm(0).rollback();
      }
   }

   protected final Object getKey(int i) {
      return keys[i % NUM_KEYS];
   }

   protected final Object getValue(int i) {
      return values[i % NUM_KEYS];
   }

   @Override
   protected final void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; ++i) {
         ConfigurationBuilder builder = defaultGMUConfiguration();
         cacheStores[i] = new DummyInMemoryCacheStore(getClass().getSimpleName());
         builder.loaders().passivation(passivation()).shared(false)
               .addStore().cacheStore(cacheStores[i]);
         if (maxEntries() > 0) {
            builder.eviction().threadPolicy(EvictionThreadPolicy.DEFAULT).strategy(EvictionStrategy.LRU).maxEntries(maxEntries());
         }
         addClusterEnabledCacheManager(builder);
      }
      //create keys
      for (int i = 0; i < NUM_KEYS; ++i) {
         keys[i] = "KEY_" + i;
         values[i] = "VALUE_" + i;
      }
      waitForClusterToForm();

   }

   protected abstract int maxEntries();

   protected abstract boolean passivation();

   protected abstract CacheMode cacheMode();

   protected abstract boolean isOwner(Object key, Cache cache);

   private ConfigurationBuilder defaultGMUConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode(), true);
      builder.locking().isolationLevel(IsolationLevel.SERIALIZABLE);
      builder.versioning().enable().scheme(VersioningScheme.GMU);
      builder.clustering().l1().disable();
      return builder;
   }

}
