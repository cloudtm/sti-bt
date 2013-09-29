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
package org.infinispan.dataplacement.c50;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.c50.keyfeature.KeyFeatureManager;
import org.infinispan.dataplacement.c50.lookup.BloomFilter;
import org.infinispan.dataplacement.c50.tree.DecisionTree;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.dataplacement.stats.IncrementableLong;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * the object lookup implementation for the Bloom Filter + Machine Learner technique
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class C50MLObjectLookup implements ObjectLookup {

   private final BloomFilter bloomFilter;
   private final DecisionTree[] decisionTreeArray;
   private transient KeyFeatureManager keyFeatureManager;

   public C50MLObjectLookup(int numberOfOwners, BloomFilter bloomFilter) {
      this.bloomFilter = bloomFilter;
      decisionTreeArray = new DecisionTree[numberOfOwners];
   }

   public void setDecisionTreeList(int index, DecisionTree decisionTree) {
      decisionTreeArray[index] = decisionTree;
   }

   public void setKeyFeatureManager(KeyFeatureManager keyFeatureManager) {
      this.keyFeatureManager = keyFeatureManager;
   }

   public BloomFilter getBloomFilter() {
      return bloomFilter;
   }

   public DecisionTree[] getDecisionTreeArray() {
      return decisionTreeArray;
   }

   @Override
   public List<Integer> query(Object key) {
      if (!bloomFilter.contains(key)) {
         return null;
      } else {
         Map<Feature, FeatureValue> keyFeatures = keyFeatureManager.getFeatures(key);
         List<Integer> owners = new LinkedList<Integer>();

         for (DecisionTree tree : decisionTreeArray) {
            owners.add(tree.query(keyFeatures));
         }
         return owners;
      }
   }

   @Override
   public List<Integer> queryWithProfiling(Object key, IncrementableLong[] phaseDurations) {
      long ts0 = System.nanoTime();
      if (!bloomFilter.contains(key)) {
         long ts1 = System.nanoTime();
         if (phaseDurations.length > 0) {
            phaseDurations[0].add(ts1 - ts0);
         }
         return null;
      } else {
         long ts1 = System.nanoTime();
         List<Integer> owners = new LinkedList<Integer>();
         Map<Feature, FeatureValue> keyFeatures = keyFeatureManager.getFeatures(key);
         long ts2 = System.nanoTime();

         for (DecisionTree tree : decisionTreeArray) {
            owners.add(tree.query(keyFeatures));
         }

         long ts3 = System.nanoTime();

         if (phaseDurations.length > 2) {
            phaseDurations[0].add(ts1 - ts0);
            phaseDurations[1].add(ts2 - ts1);
            phaseDurations[2].add(ts3 - ts2);
         } else if (phaseDurations.length > 1) {
            phaseDurations[0].add(ts1 - ts0);
            phaseDurations[1].add(ts2 - ts1);
         } else if (phaseDurations.length > 0) {
            phaseDurations[0].add(ts1 - ts0);
         }

         return owners;
      }
   }
}
