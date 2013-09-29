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
 * Represents an interface for the key feature values
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface FeatureValue extends Serializable {

   /**
    * returns true if this is less or equals than {@code other}. Returns false otherwise or if the {@code other} is not
    * comparable
    *
    * @param other the other value
    * @return true if this is less or equals than {@code other}. Returns false otherwise or if the {@code other} is not
    *         comparable
    */
   boolean isLessOrEqualsThan(FeatureValue other);

   /**
    * returns true if this is greater than {@code other}. Returns false otherwise or if the {@code other} is not
    * comparable
    *
    * @param other the other value
    * @return true if this is greater than {@code other}. Returns false otherwise or if the {@code other} is not
    *         comparable
    */
   boolean isGreaterThan(FeatureValue other);

   /**
    * returns true if this is equals to {@code other}. Returns false otherwise or if the {@code other} is not
    * comparable
    *
    * @param other the other value
    * @return true if this is equals to {@code other}. Returns false otherwise or if the {@code other} is not
    *         comparable
    */
   boolean isEquals(FeatureValue other);

   /**
    * Returns the String representation of the value to write in the .data Machine Learner file
    *
    * @return the String representation of the value
    */
   String getValueAsString();

}
