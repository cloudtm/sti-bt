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
package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.dataplacement.DataPlacementManager;
import org.infinispan.dataplacement.hm.HashMapObjectLookupFactory;
import org.infinispan.distribution.DistributionManager;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractComponent;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.DistSelfTunningTest")
public class DistSelfTunningTest extends AbstractSelfTunningTest {

   public DistSelfTunningTest() {
      super(CacheMode.DIST_SYNC);
      builder.clustering().hash().numOwners(1);
      builder.dataPlacement().enabled(true)
            .objectLookupFactory(new HashMapObjectLookupFactory())
            .coolDownTime(1000);
   }

   public void testReplicationDegree() throws Exception {
      populate();
      DistributionManager cache0DM = extractComponent(cache(0), DistributionManager.class);
      DistributionManager cache1DM = extractComponent(cache(1), DistributionManager.class);
      DataPlacementManager dataPlacementManager = extractComponent(cache(0), DataPlacementManager.class);

      assertReplicationDegree(cache0DM, 1);
      assertReplicationDegree(cache1DM, 1);

      triggerTunningReplicationDegree(dataPlacementManager, 2);

      assertReplicationDegree(cache0DM, 2);
      assertReplicationDegree(cache1DM, 2);

      addClusterEnabledCacheManager(builder);
      waitForClusterToForm();

      assertReplicationDegree(cache0DM, 2);
      assertReplicationDegree(cache1DM, 2);
      assertReplicationDegree(extractComponent(cache(2), DistributionManager.class), 2);
      assertKeysValue();
   }
}
