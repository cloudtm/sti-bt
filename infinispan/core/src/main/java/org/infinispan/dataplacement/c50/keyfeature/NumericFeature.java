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

import java.text.NumberFormat;
import java.text.ParseException;

import static java.lang.Double.compare;

/**
 * Implements a Feature that has as values a number
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NumericFeature implements Feature {

   private static final String[] CLASSES = new String[]{"continuous"};
   private final String name;

   public NumericFeature(String name) {
      if (name == null) {
         throw new IllegalArgumentException("Null is not a valid Feature name");
      }
      this.name = name.replaceAll("\\s", "");
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public String[] getMachineLearnerClasses() {
      return CLASSES;
   }

   @Override
   public FeatureValue createFeatureValue(Object value) {
      if (value instanceof Number) {
         return new NumericValue((Number) value);
      }
      throw new IllegalArgumentException("Expected a number type value");
   }

   @Override
   public FeatureValue featureValueFromParser(String value) {
      try {
         Number number = NumberFormat.getNumberInstance().parse(value);
         return new NumericValue(number);
      } catch (ParseException e) {
         throw new IllegalStateException("Error parsing value from decision tree");
      }
   }

   @Override
   public String toString() {
      return "NumericFeature{" +
            "name='" + name + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NumericFeature that = (NumericFeature) o;

      return name != null ? name.equals(that.name) : that.name == null;

   }

   @Override
   public int hashCode() {
      return name != null ? name.hashCode() : 0;
   }

   public static class NumericValue implements FeatureValue {

      private final Number value;

      private NumericValue(Number value) {
         this.value = value;
      }

      @Override
      public boolean isLessOrEqualsThan(FeatureValue other) {
         return other instanceof NumericValue &&
               compare(value.doubleValue(), ((NumericValue) other).value.doubleValue()) <= 0;
      }

      @Override
      public boolean isGreaterThan(FeatureValue other) {
         return other instanceof NumericValue &&
               compare(value.doubleValue(), ((NumericValue) other).value.doubleValue()) > 0;
      }

      @Override
      public boolean isEquals(FeatureValue other) {
         return other instanceof NumericValue &&
               compare(value.doubleValue(), ((NumericValue) other).value.doubleValue()) == 0;
      }

      @Override
      public String getValueAsString() {
         return value.toString();
      }

      @Override
      public String toString() {
         return "NumericValue{" +
               "value=" + value +
               '}';
      }
   }
}
