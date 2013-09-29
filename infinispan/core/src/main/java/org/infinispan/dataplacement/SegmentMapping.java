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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Keeps track of the new owners for the keys belonging to the segment represented by this class
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class SegmentMapping {

   private final int segmentId;
   private final List<KeyOwners> keyOwnersList;

   public SegmentMapping(int segmentId) {
      this.segmentId = segmentId;
      this.keyOwnersList = new LinkedList<KeyOwners>();
   }

   public final int getSegmentId() {
      return segmentId;
   }

   public final void add(Object key, OwnersInfo info) {
      keyOwnersList.add(new KeyOwners(key, info.getNewOwnersIndexes()));
   }

   public final Iterator<KeyOwners> iterator() {
      return keyOwnersList.iterator();
   }

   @Override
   public String toString() {
      return "SegmentMapping{" +
            "segmentId=" + segmentId +
            ", keyOwnersList=" + keyOwnersList +
            '}';
   }

   public static class KeyOwners {
      private final Object key;
      private final int[] ownerIndexes;

      private KeyOwners(Object key, Collection<Integer> ownerIndexes) {
         this.key = key;
         this.ownerIndexes = new int[ownerIndexes.size()];
         int index = 0;
         for (int i : ownerIndexes) {
            this.ownerIndexes[index++] = i;
         }
      }

      public final Object getKey() {
         return key;
      }

      public final int[] getOwnerIndexes() {
         return ownerIndexes;
      }

      @Override
      public String toString() {
         return "KeyOwners{" +
               "key=" + key +
               ", ownerIndexes=" + Arrays.toString(ownerIndexes) +
               '}';
      }
   }
}
