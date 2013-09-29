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
package org.infinispan.tx.gmu.l1;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.gmu.L1GMUContainer;
import org.infinispan.tx.gmu.AbstractGMUTest;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class AbstractGMUL1Test extends AbstractGMUTest {

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.clustering().l1().enable().enableOnRehash();
   }

   @Override
   protected final CacheMode cacheMode() {
      return CacheMode.DIST_SYNC;
   }

   protected final void assertInL1Cache(int cacheIndex, Object... keys) {
      assert keys != null : "Cannot check if null keys is in cache";
      L1GMUContainer l1GMUContainer = getComponent(cacheIndex, L1GMUContainer.class);
      assert l1GMUContainer != null : "L1 GMU Container is null";
      for (Object key : keys) {
         assert l1GMUContainer.contains(key) : "Key " + key + " not in L1 GMU container of " + cacheIndex;
      }
   }

   protected final void printL1Container() {
      if (log.isDebugEnabled()) {
         StringBuilder stringBuilder = new StringBuilder("\n\n====== L1 Container ======\n");
         for (int i = 0; i < cacheManagers.size(); ++i) {
            L1GMUContainer l1GMUContainer = getComponent(i, L1GMUContainer.class);
            assert l1GMUContainer != null : "L1 GMU Container is null";
            stringBuilder.append(l1GMUContainer.chainToString())
                  .append("\n")
                  .append("===================\n");
         }
         log.debugf(stringBuilder.toString());
      }
   }
}
