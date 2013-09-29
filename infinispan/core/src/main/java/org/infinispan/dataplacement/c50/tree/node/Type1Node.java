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
package org.infinispan.dataplacement.c50.tree.node;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * Type 1 decision tree node: discrete attributes
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class Type1Node implements DecisionTreeNode {

   private static final Log log = LogFactory.getLog(Type1Node.class);
   private final int value;
   private final Feature feature;
   private final DecisionTreeNode[] forks;
   private final FeatureValue[] attributeValues;

   public Type1Node(int value, Feature feature, DecisionTreeNode[] forks) {
      this.value = value;
      this.feature = feature;
      if (forks == null || forks.length == 0) {
         throw new IllegalArgumentException("Expected a non-null with at least one fork");
      }

      String[] possibleValues = feature.getMachineLearnerClasses();

      if (forks.length != possibleValues.length + 1) {
         throw new IllegalArgumentException("Number of forks different from the number of possible values");
      }

      this.forks = forks;
      attributeValues = new FeatureValue[forks.length - 1];

      for (int i = 0; i < attributeValues.length; ++i) {
         attributeValues[i] = feature.featureValueFromParser(possibleValues[i]);
      }
   }

   @Override
   public DecisionTreeNode find(Map<Feature, FeatureValue> keyFeatures) {
      if (log.isTraceEnabled()) {
         log.tracef("Try to find key [%s] with feature %s", keyFeatures, feature);
      }

      FeatureValue keyValue = keyFeatures.get(feature);
      if (keyValue == null) { //N/A
         if (log.isTraceEnabled()) {
            log.tracef("Feature Not Available...");
         }
         return forks[0];
      }

      if (log.isTraceEnabled()) {
         log.tracef("Comparing key value [%s] with possible values %s", keyValue, Arrays.asList(attributeValues));
      }

      for (int i = 0; i < attributeValues.length; ++i) {
         if (attributeValues[i].isEquals(keyValue)) {
            if (log.isTraceEnabled()) {
               log.tracef("Next decision tree found. The value in %s matched", i);
            }
            return forks[i + 1];
         }
      }

      throw new IllegalStateException("Expected one value match");
   }

   @Override
   public int getValue() {
      return value;
   }

   @Override
   public int getDeep() {
      int maxDeep = 0;
      for (DecisionTreeNode decisionTreeNode : forks) {
         maxDeep = Math.max(maxDeep, decisionTreeNode.getDeep());
      }
      return maxDeep + 1;
   }
}
