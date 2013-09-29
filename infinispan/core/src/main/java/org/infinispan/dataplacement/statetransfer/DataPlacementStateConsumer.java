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
package org.infinispan.dataplacement.statetransfer;

import org.infinispan.dataplacement.ClusterObjectLookup;
import org.infinispan.dataplacement.ch.DataPlacementConsistentHash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.InboundTransferTask;
import org.infinispan.statetransfer.StateConsumerImpl;
import org.infinispan.statetransfer.TransactionInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * The State Consumer logic that is aware of the Data Placement optimization and the keys that can be moved in each
 * segment
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementStateConsumer extends StateConsumerImpl {

   private void add(InboundTransferTask transferTask) {
      if (isTransactional) {
         List<TransactionInfo> transactions = getTransactions(transferTask.getSource(), null, cacheTopology.getTopologyId());
         if (transactions != null) {
            applyTransactions(transferTask.getSource(), transactions);
         }
      }

      if (isFetchEnabled) {
         List<InboundTransferTask> inboundTransfers = transfersBySource.get(transferTask.getSource());
         if (inboundTransfers == null) {
            inboundTransfers = new ArrayList<InboundTransferTask>();
            transfersBySource.put(transferTask.getSource(), inboundTransfers);
         }
         inboundTransfers.add(transferTask);
         taskQueue.add(transferTask);
         startTransferThread(new HashSet<Address>());
      }


   }

   @Override
   protected void afterStateTransferStarted(ConsistentHash oldCh, ConsistentHash newCh) {
      if (log.isTraceEnabled()) {
         log.trace("Data Placement consumer. Comparing oldCH with new CH");
      }
      super.afterStateTransferStarted(oldCh, newCh);
      if (oldCh instanceof DataPlacementConsistentHash && newCh instanceof DataPlacementConsistentHash) {
         List<ClusterObjectLookup> oldMappings = ((DataPlacementConsistentHash) oldCh).getClusterObjectLookupList();
         List<ClusterObjectLookup> newMappings = ((DataPlacementConsistentHash) newCh).getClusterObjectLookupList();

         if (oldMappings.equals(newMappings)) {
            if (log.isDebugEnabled()) {
               log.debug("Not adding new Inbound State Transfer tasks. The mappings are the same");
            }
            return;
         }
         //we have a new mapping... trigger data placement state transfer
         synchronized (this) {
            for (Address source : cacheTopology.getMembers()) {
               if (source.equals(rpcManager.getAddress())) {
                  continue;
               }
               if (log.isDebugEnabled()) {
                  log.debugf("Adding new Inbound State Transfer for %s", source);
               }
               InboundTransferTask inboundTransfer = new InboundTransferTask(null, source, cacheTopology.getTopologyId(),
                                                                             this, rpcManager, commandsFactory,
                                                                             timeout, cacheName);
               add(inboundTransfer);
            }
         }
      } else {
         if (log.isDebugEnabled()) {
            log.debug("It is not a data Placement Consistent Hash");
         }
      }

   }
}
