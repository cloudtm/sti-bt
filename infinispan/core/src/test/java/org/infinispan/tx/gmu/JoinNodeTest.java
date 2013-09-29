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

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.JoinNodeTest")
public class JoinNodeTest extends AbstractGMUTest {

   private ConfigurationBuilder builder;

   public void testJoinNode() throws Exception {
      assertAtLeastCaches(2);
      assertCacheValuesNull(KEY_1, KEY_2);

      put(0, KEY_1, VALUE_1, null);
      put(0, KEY_2, VALUE_1, null);

      addClusterEnabledCacheManager(builder);
      waitForClusterToForm();
      assertAtLeastCaches(3);

      tm(2).begin();
      cache(2).put(KEY_1, VALUE_2);
      cache(2).put(KEY_2, VALUE_2);
      tm(2).commit();

      assertNoTransactions();
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      //disable state transfer
      builder.clustering().stateTransfer().fetchInMemoryState(false)
            .hash().numOwners(1);
      this.builder = builder;
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
