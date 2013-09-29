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

package org.infinispan.tx.gmu;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.IgnoreReturnValueTest")
public class IgnoreReturnValueTest extends MultipleCacheManagersTest {

   private static final int NUM_KEYS = 10;
   private static final Object[] KEYS = new Object[NUM_KEYS];
   private static final Object[] VALUES = new Object[NUM_KEYS];

   static {
      for (int i = 0; i < NUM_KEYS; ++i) {
         KEYS[i] = "KEY_" + i;
         VALUES[i] = "VALUE_" + i;
      }
   }

   public IgnoreReturnValueTest() {
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   public final void testNotInReadSet() throws Exception {
      populate();
      GetReadSetInterceptor readSetInterceptor = injectInCache(cache(0));
      advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).put(KEYS[0], VALUES[1]);
      Assert.assertTrue(readSetInterceptor.lastReadSet.isEmpty());
      assertInAllCaches(KEYS[0], VALUES[1]);

      tm(0).begin();
      advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).put(KEYS[0], VALUES[2]);
      cache(0).get(KEYS[0]);
      tm(0).commit();
      Assert.assertTrue(readSetInterceptor.lastReadSet.isEmpty());
      assertInAllCaches(KEYS[0], VALUES[2]);

      tm(0).begin();
      cache(0).get(KEYS[0]);
      advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).put(KEYS[0], VALUES[3]);
      tm(0).commit();
      Assert.assertTrue(readSetInterceptor.lastReadSet.contains(KEYS[0]));
      assertInAllCaches(KEYS[0], VALUES[3]);
   }

   public final void testNotValidated() throws Exception {
      populate();

      tm(0).begin();
      advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).put(KEYS[0], VALUES[1]);
      final Transaction tx1 = tm(0).suspend();

      tm(0).begin();
      advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).put(KEYS[0], VALUES[2]);
      final Transaction tx2 = tm(0).suspend();

      //this originates a conflict.
      cache(0).put(KEYS[0], VALUES[3]);

      tm(0).resume(tx1);
      //this tx has never keys[0]. So it should never be aborted
      Assert.assertEquals(cache(0).get(KEYS[0]), VALUES[1]);
      tm(0).commit();

      assertInAllCaches(KEYS[0], VALUES[1]);

      tm(0).resume(tx2);
      //this tx has never keys[0]. So it should never be aborted
      Assert.assertEquals(cache(0).get(KEYS[0]), VALUES[2]);
      tm(0).commit();

      assertInAllCaches(KEYS[0], VALUES[2]);
   }

   public final void testMultiplePuts() throws Exception {
      populate();

      tm(0).begin();
      Assert.assertEquals(cache(0).get(KEYS[0]), VALUES[0]);
      advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).put(KEYS[0], VALUES[1]);
      final Transaction tx1 = tm(0).suspend();

      tm(0).begin();
      Assert.assertEquals(cache(0).get(KEYS[0]), VALUES[0]);
      advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).put(KEYS[0], VALUES[2]);
      final Transaction tx2 = tm(0).suspend();

      tm(0).begin();
      advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).put(KEYS[0], VALUES[3]);
      final Transaction tx3 = tm(0).suspend();

      tm(0).resume(tx1);

      tm(0).commit();

      assertInAllCaches(KEYS[0], VALUES[1]);

      try {
         tm(0).resume(tx2);
         tm(0).commit();
         Assert.fail("Rollback expected!");
      } catch (Exception e) {
         //expected
      }

      assertInAllCaches(KEYS[0], VALUES[1]);

      tm(0).resume(tx3);
      tm(0).commit();

      assertInAllCaches(KEYS[0], VALUES[3]);
   }

   public final void testValidation() throws Exception {
      populate();

      tm(0).begin();

      advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).put(KEYS[0], VALUES[1]);
      final Transaction tx1 = tm(0).suspend();

      tm(0).begin();
      advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).put(KEYS[0], VALUES[2]);
      final Transaction tx2 = tm(0).suspend();

      tm(0).begin();
      advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).put(KEYS[0], VALUES[3]);
      final Transaction tx3 = tm(0).suspend();

      tm(0).resume(tx1);

      tm(0).commit();

      assertInAllCaches(KEYS[0], VALUES[1]);

      tm(0).resume(tx2);
      tm(0).commit();

      assertInAllCaches(KEYS[0], VALUES[2]);

      tm(0).resume(tx3);
      tm(0).commit();

      assertInAllCaches(KEYS[0], VALUES[3]);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.locking().isolationLevel(IsolationLevel.SERIALIZABLE);
      builder.versioning().enable().scheme(VersioningScheme.GMU);
      builder.clustering().stateTransfer().fetchInMemoryState(false);
      builder.clustering().hash().numOwners(2).numSegments(25);
      createClusteredCaches(5, builder);
   }

   private GetReadSetInterceptor injectInCache(Cache cache) {
      InterceptorChain chain = TestingUtil.extractComponent(cache, InterceptorChain.class);
      List<CommandInterceptor> interceptors = chain.getInterceptorsWithClass(GetReadSetInterceptor.class);
      if (interceptors.isEmpty()) {
         GetReadSetInterceptor interceptor = new GetReadSetInterceptor();
         chain.addInterceptor(interceptor, 0);
         return interceptor;
      }
      return (GetReadSetInterceptor) interceptors.get(0);
   }

   @AfterMethod(alwaysRun = true)
   private void removeInterceptor() {
      for (Cache cache : caches()) {
         InterceptorChain chain = TestingUtil.extractComponent(cache, InterceptorChain.class);
         chain.removeInterceptor(GetReadSetInterceptor.class);
      }
   }

   private void populate() throws Exception {
      tm(0).begin();
      for (int i = 0; i < NUM_KEYS; ++i) {
         cache(0).put(KEYS[i], VALUES[i]);
      }
      tm(0).commit();
   }

   private void assertInAllCaches(Object key, Object value) {
      for (Cache cache : caches()) {
         Assert.assertEquals(cache.get(key), value, "Wrong value for key [" + key + "] in cache [" + address(cache) + "]");
      }
   }

   private class GetReadSetInterceptor extends BaseCustomInterceptor {

      private final List<Object> lastReadSet = new ArrayList<Object>();

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         synchronized (lastReadSet) {
            lastReadSet.clear();
            lastReadSet.addAll(ctx.getReadSet());
         }
         return invokeNextInterceptor(ctx, command);
      }

   }
}
