/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.configuration.cache;

import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;

/**
 * Helper configuration methods.
 *
 * @author Galder Zamarreño
 * @author Pedro Ruivo
 * @since 5.2
 */
public class Configurations {

   // Suppresses default constructor, ensuring non-instantiability.
   private Configurations(){
   }

   public static boolean isSecondPhaseAsync(Configuration cfg) {
      ClusteringConfiguration clusteringCfg = cfg.clustering();
      return !cfg.transaction().syncCommitPhase()
            || clusteringCfg.async().useReplQueue()
            || !clusteringCfg.cacheMode().isSynchronous();
   }

   public static boolean isOnePhaseCommit(Configuration cfg) {
      return !cfg.clustering().cacheMode().isSynchronous() ||
            cfg.transaction().lockingMode() == LockingMode.PESSIMISTIC;
   }

   public static boolean isStateTransferEnabled(Configuration cfg) {
      return cfg.clustering().stateTransfer().fetchInMemoryState()
            || (cfg.loaders().fetchPersistentState());
   }

   public static boolean isOnePhaseTotalOrderCommit(Configuration cfg) {
      return cfg.transaction().transactionProtocol().isTotalOrder() && !isVersioningEnabled(cfg);
   }

   public static boolean isVersioningEnabled(Configuration cfg) {
      return (cfg.locking().writeSkewCheck() &&
            cfg.transaction().lockingMode() == LockingMode.OPTIMISTIC &&
            cfg.versioning().enabled()) || cfg.locking().isolationLevel() == IsolationLevel.SERIALIZABLE;
   }

   public static boolean isOnePhasePassiveReplication(Configuration cfg) {
      return cfg.transaction().transactionProtocol().isPassiveReplication() &&
            !cfg.clustering().cacheMode().isDistributed() &&
            (!isVersioningEnabled(cfg) || cfg.transaction().useSynchronization());
   }
}
