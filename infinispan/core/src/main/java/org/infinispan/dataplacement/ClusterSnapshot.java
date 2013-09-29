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
import org.infinispan.remoting.transport.Address;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Cluster Snapshot that gives a view of the cluster with the members. This structure ensures the same index for the
 * same address in every member of the cluster and it has search complexity of n*log(n) (as in Arrays.binarySearch())
 * <p/>
 * This collection does not allow duplicate addresses.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ClusterSnapshot {

   private static final InternalAddressComparator COMPARATOR = new InternalAddressComparator();
   private final InternalAddress[] internalAddresses;
   private final Hash hashFunction;

   public ClusterSnapshot(Address[] members, Hash hashFunction) {
      this.hashFunction = hashFunction;
      Set<InternalAddress> unique = new HashSet<InternalAddress>();
      for (Address address : members) {
         unique.add(new InternalAddress(address));
      }
      internalAddresses = unique.toArray(new InternalAddress[unique.size()]);
      Arrays.sort(internalAddresses, COMPARATOR);
   }

   public ClusterSnapshot(Collection<Address> members, Hash hashFunction) {
      this.hashFunction = hashFunction;
      Set<InternalAddress> unique = new HashSet<InternalAddress>();
      for (Address address : members) {
         unique.add(new InternalAddress(address));
      }
      internalAddresses = unique.toArray(new InternalAddress[unique.size()]);
      Arrays.sort(internalAddresses, COMPARATOR);
   }

   /**
    * Returns the index of the address, or -1 if this collection does not contain the address.
    *
    * @param address the address
    * @return the index of the address, or -1 if this collection does not contain the address.
    */
   public final int indexOf(Address address) {
      if (address == null) {
         return -1;
      }
      InternalAddress internalAddress = new InternalAddress(address);
      int index = Arrays.binarySearch(internalAddresses, internalAddress, COMPARATOR);

      if (index < 0) {
         return -1;
      }

      return checkIndex(index, internalAddress);
   }

   /**
    * returns the address in the position defined by {@code index} or null if the index is negative or higher than the
    * size of this collection
    *
    * @param index the index
    * @return the address in the position defined by {@code index} or null if the index is negative or higher than the
    *         size of this collection
    */
   public final Address get(int index) {
      if (index < 0 || index >= internalAddresses.length) {
         return null;
      }
      return internalAddresses[index].address;
   }

   /**
    * returns true if this collection contains the address, false otherwise. More formally, returns true if
    * indexOf(address) != -1
    *
    * @param address the address
    * @return true if this collection contains the address, false otherwise.
    */
   public final boolean contains(Address address) {
      return indexOf(address) != -1;
   }

   /**
    * returns the number of addresses in this collection
    *
    * @return the number of addresses in the collection
    */
   public final int size() {
      return internalAddresses.length;
   }

   public final List<Address> getMembers() {
      List<Address> members = new LinkedList<Address>();
      for (InternalAddress address : internalAddresses) {
         members.add(address.address);
      }
      return members;
   }

   @Override
   public String toString() {
      return "ClusterSnapshot{" +
            "internalAddresses=" + (internalAddresses == null ? null : Arrays.asList(internalAddresses)) +
            '}';
   }

   private int checkIndex(int index, InternalAddress internalAddress) {
      //first check the index returned by the binarySearch
      if (internalAddresses[index].address.equals(internalAddress.address)) {
         return index;
      }

      //second, check backwards
      int newIndex = index - 1;
      while (newIndex >= 0 && internalAddresses[newIndex].hashCode == internalAddress.hashCode) {
         if (internalAddresses[newIndex].address.equals(internalAddress.address)) {
            return newIndex;
         }
         newIndex--;
      }

      //finally, check forwards
      newIndex = index + 1;
      while (newIndex < internalAddresses.length && internalAddresses[newIndex].hashCode == internalAddress.hashCode) {
         if (internalAddresses[newIndex].address.equals(internalAddress.address)) {
            return newIndex;
         }
         newIndex++;
      }

      //it can be the case that the address has the same hash but it is not in the array
      return -1;
   }

   private static class InternalAddressComparator implements Comparator<InternalAddress> {

      @Override
      public int compare(InternalAddress internalAddress1, InternalAddress internalAddress2) {
         return internalAddress1.hashCode - internalAddress2.hashCode;
      }
   }

   private class InternalAddress {
      private final Address address;
      private final int hashCode;

      private InternalAddress(Address address) {
         this.address = address;
         hashCode = Math.abs(hashFunction.hash(address));
      }

      @Override
      public String toString() {
         return "InternalAddress{" +
               "address=" + address +
               ", hashCode=" + hashCode +
               '}';
      }
   }
}
