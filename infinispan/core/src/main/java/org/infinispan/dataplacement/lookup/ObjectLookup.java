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
package org.infinispan.dataplacement.lookup;

import org.infinispan.dataplacement.stats.IncrementableLong;

import java.io.Serializable;
import java.util.List;

/**
 * An interface that is used to query for the new owner index defined by the data placement optimization
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface ObjectLookup extends Serializable {

   /**
    * queries this object lookup for the node index where the key can be (if the keys is moved)
    *
    * @param key the key to find
    * @return the owners index where the key is or null if the key was not moved
    */
   List<Integer> query(Object key);

   /**
    * the same as {@link #query(Object)} but it profiling information
    *
    * @param key            the key to find
    * @param phaseDurations the array with the duration of the phase (in nanoseconds)
    * @return the owners index where the key is or null if the key was not moved
    */
   List<Integer> queryWithProfiling(Object key, IncrementableLong[] phaseDurations);
}
