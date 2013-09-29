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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Maintains information about the new owners and their number of accesses
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class OwnersInfo implements Serializable {
   private final ArrayList<Integer> ownersIndexes;
   private final ArrayList<Long> ownersAccesses;

   public OwnersInfo(int size) {
      ownersIndexes = new ArrayList<Integer>(size);
      ownersAccesses = new ArrayList<Long>(size);
   }

   public void add(int ownerIndex, long numberOfAccesses) {
      ownersIndexes.add(ownerIndex);
      ownersAccesses.add(numberOfAccesses);
   }

   public void calculateNewOwner(int requestIdx, long numberOfAccesses) {
      if (ownersIndexes.contains(requestIdx)) {
         //already there
         return;
      }

      int toReplaceIndex = -1;
      long minAccesses = numberOfAccesses;

      for (int index = 0; index < ownersAccesses.size(); ++index) {
         if (ownersAccesses.get(index) < minAccesses) {
            minAccesses = ownersAccesses.get(index);
            toReplaceIndex = index;
         }
      }

      if (toReplaceIndex != -1) {
         ownersIndexes.set(toReplaceIndex, requestIdx);
         ownersAccesses.set(toReplaceIndex, numberOfAccesses);
      }
   }

   public int getOwner(int index) {
      if (index > ownersIndexes.size()) {
         return -1;
      }
      return ownersIndexes.get(index);
   }

   public int getReplicationCount() {
      return ownersIndexes.size();
   }

   public List<Integer> getNewOwnersIndexes() {
      return new LinkedList<Integer>(ownersIndexes);
   }

   @Override
   public String toString() {
      return "OwnersInfo{" +
            "ownersIndexes=" + ownersIndexes +
            ", ownersAccesses=" + ownersAccesses +
            '}';
   }
}
