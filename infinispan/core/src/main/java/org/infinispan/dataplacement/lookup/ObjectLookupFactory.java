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

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.dataplacement.SegmentMapping;

import java.util.Collection;

/**
 * Interface that creates the Object Lookup instances based on the keys to be moved
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface ObjectLookupFactory {

   /**
    * sets the Infinispan configuration to configure this object lookup factory
    *
    * @param configuration the Infinispan configuration
    */
   void setConfiguration(Configuration configuration);

   /**
    * creates the {@link ObjectLookup} corresponding to the keys to be moved
    *
    * @param keysToMove     the keys to move and the new owners
    * @param numberOfOwners the number of owners (a.k.a. replication degree)
    * @return the object lookup or null if it is not possible to create it
    */
   ObjectLookup createObjectLookup(SegmentMapping keysToMove, int numberOfOwners);

   /**
    * init the object lookup
    *
    * @param objectLookup the object lookup
    */
   void init(Collection<ObjectLookup> objectLookup);

   /**
    * returns the number of phases when the query profiling
    *
    * @return the number of phases when the query profiling
    */
   int getNumberOfQueryProfilingPhases();
}
