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

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutboundTransferTask;
import org.infinispan.statetransfer.StateProviderImpl;
import org.infinispan.topology.CacheTopology;

import java.util.Set;

/**
 * The State Provider logic that is aware of the Data Placement optimization and the keys that can be moved in each
 * segment
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementStateProvider extends StateProviderImpl {

   @Override
   protected OutboundTransferTask createTask(Address destination, Set<Integer> segments, CacheTopology cacheTopology) {
      return new DataPlacementOutboundTransferTask(destination, segments, chunkSize, cacheTopology.getTopologyId(),
                                                   cacheTopology.getReadConsistentHash(), this,
                                                   cacheTopology.getWriteConsistentHash(), dataContainer,
                                                   cacheLoaderManager, rpcManager, commandsFactory, timeout, cacheName);
   }

   @Override
   protected boolean isKeyLocal(Object key, ConsistentHash consistentHash, Set<Integer> segments) {
      //a key is local to this node is it belongs to the segments requested and it has not been moved
      boolean isLocal = consistentHash.isKeyLocalToNode(rpcManager.getAddress(), key);
      boolean belongToSegments = segments == null || segments.contains(consistentHash.getSegment(key));
      return isLocal && belongToSegments;
   }
}
