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
package org.infinispan.dataplacement.c50.keyfeature;

import java.io.Serializable;

/**
 * Represents an interface for a key feature
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface Feature extends Serializable {

   /**
    * returns the feature name
    *
    * @return the feature name
    */
   String getName();

   /**
    * returns the classes for the machine learner, corresponding to this feature (attribute)
    *
    * @return the classes for the machine learner
    */
   String[] getMachineLearnerClasses();

   /**
    * creates a feature value of the type of this attribute
    *
    * @param value the value
    * @return the feature value corresponding to {@code value}
    */
   FeatureValue createFeatureValue(Object value);

   /**
    * creates a feature value from the value parsed from the decision tree
    *
    * @param value the value
    * @return the feature value corresponding to {@code value}
    */
   FeatureValue featureValueFromParser(String value);
}
