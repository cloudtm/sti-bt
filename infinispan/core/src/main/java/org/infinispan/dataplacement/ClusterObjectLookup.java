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
package org.infinispan.dataplacement;

import org.infinispan.commons.hash.Hash;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ClusterObjectLookup {

   private final ObjectLookup[] objectLookups;
   private final ClusterSnapshot clusterSnapshot;

   public ClusterObjectLookup(ObjectLookup[] objectLookups, ClusterSnapshot clusterSnapshot) {
      this.objectLookups = objectLookups;
      this.clusterSnapshot = clusterSnapshot;
   }

   public static void write(ObjectOutput output, ClusterObjectLookup object) throws IOException {
      if (object == null || object.objectLookups == null || object.objectLookups.length == 0) {
         output.writeInt(0);
      } else {
         output.writeInt(object.objectLookups.length);
         for (ObjectLookup objectLookup : object.objectLookups) {
            output.writeObject(objectLookup);
         }
         List<Address> members = object.clusterSnapshot.getMembers();
         output.writeInt(members.size());
         for (Address address : members) {
            output.writeObject(address);
         }
      }
   }

   public static ClusterObjectLookup read(ObjectInput input, Hash hash) throws IOException, ClassNotFoundException {
      int size = input.readInt();
      if (size == 0) {
         return null;
      }
      ObjectLookup[] objectLookups = new ObjectLookup[size];
      for (int i = 0; i < size; ++i) {
         objectLookups[i] = (ObjectLookup) input.readObject();
      }
      size = input.readInt();
      Address[] members = new Address[size];
      for (int i = 0; i < size; ++i) {
         members[i] = (Address) input.readObject();
      }
      return new ClusterObjectLookup(objectLookups, new ClusterSnapshot(members, hash));
   }

   public final List<Address> getNewOwnersForKey(Object key, ConsistentHash consistentHash, int numberOfOwners) {
      if (objectLookups == null || objectLookups.length == 0 || objectLookups[consistentHash.getSegment(key)] == null) {
         return null;
      }
      List<Integer> newOwnersIndexes = objectLookups[consistentHash.getSegment(key)].query(key);
      if (newOwnersIndexes == null || newOwnersIndexes.isEmpty()) {
         return null;
      }
      List<Address> newOwners = new ArrayList<Address>(Math.min(numberOfOwners, newOwnersIndexes.size()));
      for (Iterator<Integer> iterator = newOwnersIndexes.iterator(); iterator.hasNext() && newOwners.size() < numberOfOwners; ) {
         Address owner = clusterSnapshot.get(iterator.next());
         if (owner != null && consistentHash.getMembers().contains(owner)) {
            newOwners.add(owner);
         }
      }
      return newOwners;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClusterObjectLookup that = (ClusterObjectLookup) o;

      return !(clusterSnapshot != null ? !clusterSnapshot.equals(that.clusterSnapshot) : that.clusterSnapshot != null) &&
            Arrays.equals(objectLookups, that.objectLookups);

   }

   @Override
   public int hashCode() {
      int result = objectLookups != null ? Arrays.hashCode(objectLookups) : 0;
      result = 31 * result + (clusterSnapshot != null ? clusterSnapshot.hashCode() : 0);
      return result;
   }

}
