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

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.dataplacement.DataPlacementManager;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.reconfigurableprotocol.manager.ReconfigurableReplicationManager;
import org.infinispan.reconfigurableprotocol.protocol.TotalOrderCommitProtocol;
import org.infinispan.reconfigurableprotocol.protocol.TwoPhaseCommitProtocol;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.sleepThread;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional")
public abstract class AbstractSelfTunningTest extends MultipleCacheManagersTest {

   private static final String KEY_1 = "KEY_1";
   private static final String KEY_2 = "KEY_2";
   private static final String KEY_3 = "KEY_3";
   private static final String VALUE = "VALUE";
   protected final ConfigurationBuilder builder;

   protected AbstractSelfTunningTest(CacheMode cacheMode) {
      builder = getDefaultClusteredCacheConfig(cacheMode, true);
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testSwitch() throws Exception {
      populate();
      ReconfigurableReplicationManager manager0 = extractComponent(cache(0), ReconfigurableReplicationManager.class);
      ReconfigurableReplicationManager manager1 = extractComponent(cache(1), ReconfigurableReplicationManager.class);

      assertProtocol(manager0, TwoPhaseCommitProtocol.UID);
      assertEpoch(manager0, 0);
      assertProtocol(manager1, TwoPhaseCommitProtocol.UID);
      assertEpoch(manager1, 0);

      triggerTunningProtocol(manager0, TotalOrderCommitProtocol.UID);

      assertProtocol(manager0, TotalOrderCommitProtocol.UID);
      assertEpoch(manager0, 1);
      assertProtocol(manager1, TotalOrderCommitProtocol.UID);
      assertEpoch(manager1, 1);

      addClusterEnabledCacheManager(builder);
      waitForClusterToForm();

      ReconfigurableReplicationManager manager2 = extractComponent(cache(2), ReconfigurableReplicationManager.class);

      assertProtocol(manager0, TotalOrderCommitProtocol.UID);
      assertEpoch(manager0, 1);
      assertProtocol(manager1, TotalOrderCommitProtocol.UID);
      assertEpoch(manager1, 1);
      assertProtocol(manager2, TotalOrderCommitProtocol.UID);
      assertEpoch(manager2, 1);
      assertKeysValue();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builder.clustering().stateTransfer().fetchInMemoryState(true);
      createCluster(builder, 2);
      waitForClusterToForm();
   }

   protected void assertReplicationDegree(final DistributionManager distributionManager, final int expectedReplicationDegree) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return distributionManager.getConsistentHash().getNumOwners() == expectedReplicationDegree;
         }
      });
   }

   protected void triggerTunningReplicationDegree(DataPlacementManager dataPlacementManager, int replicationDegree) throws Exception {
      sleepThread(DataPlacementManager.INITIAL_COOL_DOWN_TIME);
      dataPlacementManager.setReplicationDegree(replicationDegree);
   }

   protected void populate() {
      cache(0).put(KEY_1, VALUE);
      cache(0).put(KEY_2, VALUE);
      cache(0).put(KEY_3, VALUE);
   }

   protected void assertKeysValue() {
      for (final Cache cache : caches()) {
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return VALUE.equals(cache.get(KEY_1));
            }
         });
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return VALUE.equals(cache.get(KEY_2));
            }
         });
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return VALUE.equals(cache.get(KEY_3));
            }
         });
      }
   }

   private void assertProtocol(final ReconfigurableReplicationManager manager, final String protocolId) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return manager.getCurrentProtocolId().equals(protocolId);
         }
      });
   }

   private void assertEpoch(final ReconfigurableReplicationManager manager, final long epoch) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return epoch == manager.getCurrentEpoch();
         }
      });
   }

   private void triggerTunningProtocol(ReconfigurableReplicationManager manager, String protocolId) throws Exception {
      sleepThread(manager.getSwitchCoolDownTime() * 1000);
      manager.switchTo(protocolId, false, false);
   }
}
