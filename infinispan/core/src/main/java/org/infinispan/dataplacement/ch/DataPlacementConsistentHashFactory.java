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
package org.infinispan.dataplacement.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.dataplacement.ClusterObjectLookup;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Set;

/**
 * Consistent Hash Factory used when Data Placement optimization is enabled
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementConsistentHashFactory<CH extends ConsistentHash>
      implements ConsistentHashFactory<DataPlacementConsistentHash<CH>> {

   private final ConsistentHashFactory<CH> consistentHashFactory;

   public DataPlacementConsistentHashFactory(ConsistentHashFactory<CH> consistentHashFactory) {
      this.consistentHashFactory = consistentHashFactory;
   }

   @Override
   public DataPlacementConsistentHash<CH> create(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
      CH ch = consistentHashFactory.create(hashFunction, numOwners, numSegments, members);
      return new DataPlacementConsistentHash<CH>(ch);
   }

   @Override
   public DataPlacementConsistentHash<CH> updateMembers(DataPlacementConsistentHash<CH> baseCH, List<Address> newMembers) {
      CH ch = consistentHashFactory.updateMembers(baseCH.getConsistentHash(), newMembers);
      return new DataPlacementConsistentHash<CH>(baseCH, ch);
   }

   @Override
   public DataPlacementConsistentHash<CH> rebalance(DataPlacementConsistentHash<CH> baseCH, Object customData) {
      CH ch = consistentHashFactory.rebalance(baseCH.getConsistentHash(), customData);
      if (ch.equals(baseCH.getConsistentHash())) {
         if (customData != null && customData instanceof ConsistentHashChanges) {
            ConsistentHashChanges consistentHashChanges = (ConsistentHashChanges) customData;
            ClusterObjectLookup clusterObjectLookup = consistentHashChanges.getNewMappings();
            if (clusterObjectLookup == null && baseCH.getClusterObjectLookupList().isEmpty()) {
               return baseCH;
            }
            if (baseCH.getClusterObjectLookupList().size() == 1 && baseCH.getClusterObjectLookupList().get(0).equals(clusterObjectLookup)) {
               //same CH and same ClusterObjectLookup
               return baseCH;
            } else {
               return new DataPlacementConsistentHash<CH>(baseCH.getConsistentHash(), clusterObjectLookup);
            }
         } else {
            return baseCH;
         }
      } else {
         if (customData != null && customData instanceof ClusterObjectLookup) {
            ClusterObjectLookup clusterObjectLookup = (ClusterObjectLookup) customData;
            return new DataPlacementConsistentHash<CH>(ch, clusterObjectLookup);
         } else {
            return new DataPlacementConsistentHash<CH>(baseCH, ch);
         }
      }
   }

   @Override
   public DataPlacementConsistentHash<CH> union(DataPlacementConsistentHash<CH> ch1, DataPlacementConsistentHash<CH> ch2) {
      CH union = consistentHashFactory.union(ch1.getConsistentHash(), ch2.getConsistentHash());
      return ch1.union(ch2, union);
   }

   public static class Externalizer extends AbstractExternalizer<DataPlacementConsistentHashFactory> {

      @Override
      public Set<Class<? extends DataPlacementConsistentHashFactory>> getTypeClasses() {
         return Util.<Class<? extends DataPlacementConsistentHashFactory>>asSet(DataPlacementConsistentHashFactory.class);
      }

      @Override
      public void writeObject(ObjectOutput output, DataPlacementConsistentHashFactory object) throws IOException {
         output.writeObject(object.consistentHashFactory);
      }

      @Override
      public DataPlacementConsistentHashFactory readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         ConsistentHashFactory factory = (ConsistentHashFactory) input.readObject();
         return new DataPlacementConsistentHashFactory(factory);
      }

      @Override
      public Integer getId() {
         return Ids.DATA_PLACEMENT_CONSISTENT_HASH_FACTORY;
      }
   }
}
