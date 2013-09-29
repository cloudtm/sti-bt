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

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.gmu.L1GMUContainer;
import org.infinispan.transaction.gmu.manager.GarbageCollectorManager;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.gc.DistL1SimpleGarbageCollectorTest")
public class DistL1SimpleGarbageCollectorTest extends DistSimpleGarbageCollectorTest {

   public void testL1GC() {
      assertAtLeastCaches(2);
      final Object key1 = newKey(1, 0);

      assertKeyOwners(key1, 1, 0);

      final L1GMUContainer l1GMUContainer = getComponent(0, L1GMUContainer.class);
      GarbageCollectorManager garbageCollectorManager = getComponent(0, GarbageCollectorManager.class);

      put(1, key1, VALUE_1, null);
      assertEquals(VALUE_1, cache(0).get(key1));

      put(1, key1, VALUE_2, VALUE_1);
      assertEquals(VALUE_2, cache(0).get(key1));

      put(1, key1, VALUE_3, VALUE_2);
      assertEquals(VALUE_3, cache(0).get(key1));

      assert l1GMUContainer.getVersionChain(key1).numberOfVersion() == 3;

      garbageCollectorManager.triggerL1GarbageCollection();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return l1GMUContainer.getVersionChain(key1).numberOfVersion() == 1;
         }
      });

      assertNoTransactions();
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      super.decorate(builder);
      builder.clustering().l1().enable();
   }
}
