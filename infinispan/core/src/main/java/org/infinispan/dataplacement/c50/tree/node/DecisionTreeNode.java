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

import java.io.Serializable;
import java.util.Map;

/**
 * Interface of a Decision Tree node
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface DecisionTreeNode extends Serializable {

   /**
    * find and return the best node that respect the feature values
    *
    * @param keyFeatures the feature values
    * @return the best node
    */
   DecisionTreeNode find(Map<Feature, FeatureValue> keyFeatures);

   /**
    * returns the value of the node (i.e. the new owner)
    *
    * @return the value of the node (i.e. the new owner)
    */
   int getValue();

   /**
    * @return the deep of the sub-tree where this node is the root
    */
   int getDeep();
}
