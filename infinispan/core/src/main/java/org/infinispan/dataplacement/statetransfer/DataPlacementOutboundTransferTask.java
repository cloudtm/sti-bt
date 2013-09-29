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

import org.infinispan.commands.CommandsFactory;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutboundTransferTask;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateProviderImpl;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * The Outbound Transfer Task logic when Data Placement optimization is enabled. This logic is aware of the keys
 * that can be moved in each segment.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementOutboundTransferTask extends OutboundTransferTask {

   public DataPlacementOutboundTransferTask(Address destination, Set<Integer> segments, int stateTransferChunkSize,
                                            int topologyId, ConsistentHash readCh, StateProviderImpl stateProvider,
                                            ConsistentHash writeCh, DataContainer dataContainer,
                                            CacheLoaderManager cacheLoaderManager, RpcManager rpcManager,
                                            CommandsFactory commandsFactory, long timeout, String cacheName) {
      super(destination, segments, stateTransferChunkSize, topologyId, readCh, stateProvider, writeCh, dataContainer,
            cacheLoaderManager, rpcManager, commandsFactory, timeout, cacheName);
   }

   @Override
   protected boolean isKeyMoved(Object key) {
      int segmentId = readCh.getSegment(key);
      if (segments.isEmpty()) {
         //Data Placement
         boolean isSegmentMoved = !readCh.getSegmentsForOwner(destination).contains(segmentId) &&
               writeCh.getSegmentsForOwner(destination).contains(segmentId);
         boolean shouldIPush = readCh.locatePrimaryOwner(key).equals(rpcManager.getAddress());
         boolean destinationIsOwner = writeCh.isKeyLocalToNode(destination, key);
         if (log.isDebugEnabled()) {
            log.debugf("Analyzing key %s. Is the segment moved?=%s, Should I push it?=%s, Destination is an owner?=%s",
                       key, isSegmentMoved, shouldIPush, destinationIsOwner);
         }
         return !isSegmentMoved && shouldIPush && destinationIsOwner;
      } else {
         return segments.contains(segmentId);
      }
   }

   @Override
   protected void sendEntries(boolean isLast) {
      if (segments.isEmpty()) {
         List<InternalCacheEntry> allEntries = new LinkedList<InternalCacheEntry>();

         for (List<InternalCacheEntry> entries : entriesBySegment.values()) {
            if (!entries.isEmpty()) {
               allEntries.addAll(entries);
               entries.clear();
            }
         }
         List<StateChunk> chunks = new LinkedList<StateChunk>();
         chunks.add(new StateChunk(-1, allEntries, isLast));
         sendChunks(chunks, isLast);
      } else {
         super.sendEntries(isLast);
      }

   }
}
