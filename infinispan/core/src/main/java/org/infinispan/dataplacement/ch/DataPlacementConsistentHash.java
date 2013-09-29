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
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Data Placement Consistent Hash that keep tracks of the new owners per segment
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementConsistentHash<CH extends ConsistentHash> implements ConsistentHash {

   private final CH consistentHash;
   //one object lookup per segment
   private final List<ClusterObjectLookup> clusterObjectLookupList;
   private final boolean union;

   public DataPlacementConsistentHash(CH consistentHash) {
      this.consistentHash = consistentHash;
      this.clusterObjectLookupList = InfinispanCollections.emptyList();
      this.union = false;
   }

   public DataPlacementConsistentHash(CH consistentHash, ClusterObjectLookup clusterObjectLookup) {
      this.consistentHash = consistentHash;
      if (clusterObjectLookup == null) {
         this.clusterObjectLookupList = Collections.emptyList();
      } else {
         this.clusterObjectLookupList = Collections.singletonList(clusterObjectLookup);
      }
      this.union = false;
   }

   public DataPlacementConsistentHash(DataPlacementConsistentHash<CH> baseCH, CH consistentHash) {
      this.consistentHash = consistentHash;
      if (baseCH.getClusterObjectLookupList().isEmpty()) {
         this.clusterObjectLookupList = InfinispanCollections.emptyList();
      } else {
         this.clusterObjectLookupList = Collections.singletonList(baseCH.getClusterObjectLookupList().get(0));
      }
      this.union = false;
   }

   private DataPlacementConsistentHash(CH consistentHash, List<ClusterObjectLookup> clusterObjectLookups, boolean union) {
      this.consistentHash = consistentHash;
      this.clusterObjectLookupList = clusterObjectLookups;
      this.union = union;
   }

   @Override
   public int getNumOwners() {
      return consistentHash.getNumOwners();
   }

   @Override
   public Hash getHashFunction() {
      return consistentHash.getHashFunction();
   }

   @Override
   public int getNumSegments() {
      return consistentHash.getNumSegments();
   }

   @Override
   public List<Address> getMembers() {
      return consistentHash.getMembers();
   }

   @Override
   public Address locatePrimaryOwner(Object key) {
      if (union) {
         return consistentHash.locatePrimaryOwner(key);
      }
      List<Address> newOwners = getNewOwnersForKey(key, 1);
      return newOwners == null || newOwners.isEmpty() ? consistentHash.locatePrimaryOwner(key) :
            newOwners.get(0);
   }

   @Override
   public List<Address> locateOwners(Object key) {
      List<Address> newOwners = getNewOwnersForKey(key, union ? Integer.MAX_VALUE : consistentHash.getNumOwners());
      List<Address> defaultOwners = consistentHash.locateOwners(key);

      if (union) {
         List<Address> union = new LinkedList<Address>(defaultOwners);
         mergeUnique(union, newOwners);
         return union;
      }

      return newOwners == null || newOwners.isEmpty() ? defaultOwners :
            merge(defaultOwners, newOwners, defaultOwners.size());
   }

   @Override
   public Set<Address> locateAllOwners(Collection<Object> keys) {
      Set<Address> owners = new HashSet<Address>();
      for (Object key : keys) {
         owners.addAll(locateOwners(key));
      }
      return owners;
   }

   @Override
   public boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      return locateOwners(key).contains(nodeAddress);
   }

   @Override
   public int getSegment(Object key) {
      return consistentHash.getSegment(key);
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      return consistentHash.locateOwnersForSegment(segmentId);
   }

   @Override
   public Address locatePrimaryOwnerForSegment(int segmentId) {
      return consistentHash.locatePrimaryOwnerForSegment(segmentId);
   }

   @Override
   public Set<Integer> getSegmentsForOwner(Address owner) {
      return consistentHash.getSegmentsForOwner(owner);
   }

   @Override
   public String getRoutingTableAsString() {
      return "Default=" + consistentHash.getRoutingTableAsString() + ". New Mappings=" + clusterObjectLookupList;
   }

   public final CH getConsistentHash() {
      return consistentHash;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DataPlacementConsistentHash that = (DataPlacementConsistentHash) o;

      return !(clusterObjectLookupList != null ? !clusterObjectLookupList.equals(that.clusterObjectLookupList) : that.clusterObjectLookupList != null) &&
            !(consistentHash != null ? !consistentHash.equals(that.consistentHash) : that.consistentHash != null);

   }

   @Override
   public int hashCode() {
      int result = consistentHash != null ? consistentHash.hashCode() : 0;
      result = 31 * result + (clusterObjectLookupList != null ? clusterObjectLookupList.hashCode() : 0);
      return result;
   }

   public final List<ClusterObjectLookup> getClusterObjectLookupList() {
      return clusterObjectLookupList;
   }

   public final DataPlacementConsistentHash<CH> union(DataPlacementConsistentHash<CH> baseCH, CH unionCH) {
      List<ClusterObjectLookup> union = new LinkedList<ClusterObjectLookup>();
      union.addAll(getClusterObjectLookupList());
      union.addAll(baseCH.getClusterObjectLookupList());
      return new DataPlacementConsistentHash<CH>(unionCH, union, true);
   }

   @Override
   public String toString() {
      return "DataPlacementConsistentHash{" +
            "consistentHash=" + consistentHash +
            ", clusterObjectLookupList=" + clusterObjectLookupList +
            '}';
   }

   private List<Address> merge(List<Address> defaultOwners, List<Address> newOwners, int numberOfOwners) {
      List<Address> merged = new ArrayList<Address>(numberOfOwners);
      //first put owners that are in the default and new owners list
      for (Iterator<Address> iterator = defaultOwners.iterator(); iterator.hasNext() && merged.size() < numberOfOwners; ) {
         Address defaultOwner = iterator.next();
         int index = newOwners.indexOf(defaultOwner);
         if (index >= 0) {
            merged.add(defaultOwner);
            newOwners.remove(index);
            iterator.remove();
         }
      }

      //then add the remaining new owners list (if needed)
      for (Address newOwner : newOwners) {
         merged.add(newOwner);
      }

      //finally the default owners list (if needed)
      for (Iterator<Address> iterator = defaultOwners.iterator(); iterator.hasNext() && merged.size() < numberOfOwners; ) {
         merged.add(iterator.next());
      }
      return merged;
   }

   private List<Address> getNewOwnersForKey(Object key, int numberOfOwners) {
      List<Address> newOwners = new LinkedList<Address>();

      for (ClusterObjectLookup clusterObjectLookup : clusterObjectLookupList) {
         mergeUnique(newOwners, clusterObjectLookup.getNewOwnersForKey(key, consistentHash, numberOfOwners));
      }

      return newOwners;
   }

   private void mergeUnique(List<Address> result, List<Address> toMerge) {
      if (toMerge == null) {
         return;
      }
      for (Address address : toMerge) {
         if (!result.contains(address)) {
            result.add(address);
         }
      }
   }

   public static class Externalizer extends AbstractExternalizer<DataPlacementConsistentHash> {

      @Override
      public Set<Class<? extends DataPlacementConsistentHash>> getTypeClasses() {
         return Util.<Class<? extends DataPlacementConsistentHash>>asSet(DataPlacementConsistentHash.class);
      }

      @Override
      public void writeObject(ObjectOutput output, DataPlacementConsistentHash object) throws IOException {
         output.writeObject(object.consistentHash);
         output.writeBoolean(object.union);
         List<ClusterObjectLookup> list = object.getClusterObjectLookupList();
         output.writeInt(list.size());
         for (ClusterObjectLookup clusterObjectLookup : list) {
            ClusterObjectLookup.write(output, clusterObjectLookup);
         }
      }

      @Override
      public DataPlacementConsistentHash readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         ConsistentHash consistentHash = (ConsistentHash) input.readObject();
         boolean union = input.readBoolean();
         int size = input.readInt();
         List<ClusterObjectLookup> clusterObjectLookups = new ArrayList<ClusterObjectLookup>(size);
         for (int i = 0; i < size; ++i) {
            clusterObjectLookups.add(ClusterObjectLookup.read(input, consistentHash.getHashFunction()));
         }
         return new DataPlacementConsistentHash(consistentHash, clusterObjectLookups, union);
      }

      @Override
      public Integer getId() {
         return Ids.DATA_PLACEMENT_CONSISTENT_HASH;
      }
   }
}
