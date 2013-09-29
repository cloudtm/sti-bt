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
package org.infinispan.dataplacement.hm;

import org.infinispan.dataplacement.SegmentMapping;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.dataplacement.stats.IncrementableLong;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * the object lookup implementation for the Hash Map technique
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class HashMapObjectLookup implements ObjectLookup {

   private final Map<Object, List<Integer>> lookup;

   public HashMapObjectLookup(Iterator<SegmentMapping.KeyOwners> iterator) {
      lookup = new HashMap<Object, List<Integer>>();

      while (iterator.hasNext()) {
         SegmentMapping.KeyOwners owners = iterator.next();
         List<Integer> list = new LinkedList<Integer>();
         for (int index : owners.getOwnerIndexes()) {
            list.add(index);
         }
         lookup.put(owners.getKey(), list);
      }
   }

   @Override
   public List<Integer> query(Object key) {
      return lookup.get(key);
   }

   @Override
   public List<Integer> queryWithProfiling(Object key, IncrementableLong[] phaseDurations) {
      long start = System.nanoTime();
      List<Integer> result = lookup.get(key);
      long end = System.nanoTime();

      if (phaseDurations.length == 1) {
         phaseDurations[0].add(end - start);
      }

      return result;
   }
}
