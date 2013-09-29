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
package org.infinispan.dataplacement.c50.tree;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.c50.tree.node.DecisionTreeNode;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents a decision tree in which you can query based on the values of some attributes and returns the new owner
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DecisionTree implements Serializable {

   private final DecisionTreeNode root;

   public DecisionTree(DecisionTreeNode root) {
      this.root = root;
   }

   /**
    * queries the decision tree looking for the value depending of the features value
    *
    * @param keyFeature the feature values
    * @return the index of the new owner
    */
   public final int query(Map<Feature, FeatureValue> keyFeature) {
      if (root == null) {
         throw new IllegalStateException("Expected to find a root node to start");
      }
      DecisionTreeNode node = root.find(keyFeature);

      if (node == null) {
         return root.getValue();
      }

      DecisionTreeNode result = node;

      while (node != null) {
         result = node;
         node = node.find(keyFeature);
      }

      return result.getValue();
   }

   /**
    * @return the deep of the tree
    */
   public final int getDeep() {
      return root.getDeep();
   }
}
