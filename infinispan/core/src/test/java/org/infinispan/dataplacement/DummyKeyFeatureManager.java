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

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.c50.keyfeature.KeyFeatureManager;
import org.infinispan.dataplacement.c50.keyfeature.NumericFeature;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Dummy Key Feature manager
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DummyKeyFeatureManager implements KeyFeatureManager {
   private final Feature[] features = new Feature[]{
         new NumericFeature("B"),
         new NumericFeature("C")
   };

   public static Object getKey(int c) {
      return String.format("KEY_%s", c);
   }

   public static Object getKey(int b, int c) {
      return String.format("KEY_%s_%s", b, c);
   }

   @Override
   public Feature[] getAllKeyFeatures() {
      return features;
   }

   @Override
   public Map<Feature, FeatureValue> getFeatures(Object key) {

      if (!(key instanceof String)) {
         return Collections.emptyMap();
      }

      String[] split = ((String) key).split("_");
      Map<Feature, FeatureValue> features = new HashMap<Feature, FeatureValue>();

      if (split.length == 3) {
         int b = Integer.parseInt(split[1]);
         int c = Integer.parseInt(split[2]);

         features.put(this.features[0], this.features[0].createFeatureValue(b));
         features.put(this.features[1], this.features[1].createFeatureValue(c));
      } else if (split.length == 2) {
         int c = Integer.parseInt(split[1]);

         features.put(this.features[1], this.features[1].createFeatureValue(c));
      }

      return features;
   }
}
