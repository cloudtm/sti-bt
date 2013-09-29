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

import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.CallInterceptor;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Set;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.DistSimpleTest")
public class DistSimpleTest extends SimpleTest {

   public void testReadSet() throws Exception {
      try {
         Object read = newKey(1);
         Object write = newKey(0);
         AffectedKeysInterceptor interceptor = new AffectedKeysInterceptor();
         cache(0).getAdvancedCache().addInterceptorBefore(interceptor, CallInterceptor.class);

         tm(0).begin();
         cache(0).get(read);
         cache(0).put(write, VALUE_1);
         tm(0).commit();
         Assert.assertNotNull(interceptor.affectedKeys);
         Assert.assertTrue(interceptor.affectedKeys.containsAll(Arrays.asList(read, write)));
      } finally {
         cache(0).getAdvancedCache().removeInterceptor(AffectedKeysInterceptor.class);
      }
   }

   public void testReadSet2() throws Exception {
      try {
         Object read = newKey(0);
         Object write = newKey(1);
         AffectedKeysInterceptor interceptor = new AffectedKeysInterceptor();
         cache(0).getAdvancedCache().addInterceptorBefore(interceptor, CallInterceptor.class);

         tm(0).begin();
         cache(0).get(read);
         cache(0).put(write, VALUE_1);
         tm(0).commit();
         Assert.assertNotNull(interceptor.affectedKeys);
         Assert.assertTrue(interceptor.affectedKeys.containsAll(Arrays.asList(read, write)));
      } finally {
         cache(0).getAdvancedCache().removeInterceptor(AffectedKeysInterceptor.class);
      }
   }

   @Override
   protected CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().hash().numOwners(1);
   }

   public class AffectedKeysInterceptor extends BaseCustomInterceptor {

      private volatile Set<Object> affectedKeys = null;

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         Assert.assertTrue(command instanceof GMUPrepareCommand);
         if (ctx.isOriginLocal()) {
            affectedKeys = ctx.getAffectedKeys();
         }
         return invokeNextInterceptor(ctx, command);
      }
   }
}
