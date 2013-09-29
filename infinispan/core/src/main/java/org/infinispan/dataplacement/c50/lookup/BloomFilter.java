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
package org.infinispan.dataplacement.c50.lookup;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Collection;

public class BloomFilter implements Serializable {

   private final int numHash;
   private final int filterSize;
   private final BitSet filter;

   //num queries = 1
   public BloomFilter(Collection<Object> objects, double falsePositiveRate) {
      double bitsPerElement = Math.log(falsePositiveRate) / Math.log(0.6185);
      numHash = (int) Math.ceil(Math.log(2) * bitsPerElement);

      filter = new BitSet((int) Math.ceil(bitsPerElement * objects.size()));
      filterSize = filter.size();

      for (Object obj : objects) {
         accessFilterPositions(convertObjectToByteArray(obj), true);
      }
   }

   public boolean contains(Object object) {
      return accessFilterPositions(convertObjectToByteArray(object), false);
   }

   public final int size() {
      return filterSize;
   }

   private byte[] convertObjectToByteArray(Object object) {
      int value = object.hashCode();
      return new byte[]{
            (byte) (value >>> 24),
            (byte) (value >>> 16),
            (byte) (value >>> 8),
            (byte) value};
   }

   private boolean accessFilterPositions(byte[] elementId, boolean set) {
      int hash1 = murmurHash(elementId, elementId.length, 0);
      int hash2 = murmurHash(elementId, elementId.length, hash1);
      for (int i = 0; i < numHash; i++) {
         if (set)
            filter.set(Math.abs((hash1 + i * hash2) % filterSize), true);
         else if (!filter.get(Math.abs((hash1 + i * hash2) % filterSize)))
            return false;
      }
      return true;
   }

   private int murmurHash(byte[] data, int length, int seed) {
      int m = 0x5bd1e995;
      int r = 24;

      int h = seed ^ length;

      int len_4 = length >> 2;

      for (int i = 0; i < len_4; i++) {
         int i_4 = i << 2;
         int k = data[i_4 + 3];
         k = k << 8;
         k = k | (data[i_4 + 2] & 0xff);
         k = k << 8;
         k = k | (data[i_4 + 1] & 0xff);
         k = k << 8;
         k = k | (data[i_4] & 0xff);
         k *= m;
         k ^= k >>> r;
         k *= m;
         h *= m;
         h ^= k;
      }

      // avoid calculating modulo
      int len_m = len_4 << 2;
      int left = length - len_m;

      if (left != 0) {
         if (left >= 3) {
            h ^= (int) data[length - 3] << 16;
         }
         if (left >= 2) {
            h ^= (int) data[length - 2] << 8;
         }
         if (left >= 1) {
            h ^= (int) data[length - 1];
         }

         h *= m;
      }

      h ^= h >>> 13;
      h *= m;
      h ^= h >>> 15;

      return h;
   }
}
