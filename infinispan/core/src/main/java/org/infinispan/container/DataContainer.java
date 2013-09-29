/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
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
package org.infinispan.container;

import java.util.Collection;
import java.util.Set;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * The main internal data structure which stores entries
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface DataContainer extends Iterable<InternalCacheEntry> {

   /**
    * Retrieves a cached entry version that is less or equals than {@param version}.    
    *
    * @param k key under which entry is stored
    * @param version the snapshot version to read. if it is null, the most recent version is returned
    * @return entry, if it exists and has not expired, or null if not
    */
   InternalCacheEntry get(Object k, EntryVersion version);
   
   /**
    * Retrieves a cache entry in the same way as {@link #get(Object, org.infinispan.container.versioning.EntryVersion)}}
    * except that it does not update or reorder any of the internal constructs. 
    * I.e., expiration does not happen, and in the case of the LRU container, 
    * the entry is not moved to the end of the chain.
    * 
    * This method should be used instead of {@link #get(Object, org.infinispan.container.versioning.EntryVersion)}} when called
    * while iterating through the data container using methods like {@link #keySet(org.infinispan.container.versioning.EntryVersion)}
    * to avoid changing the underlying collection's order.  
    *
    * @param k key under which entry is stored
    * @param version the snapshot version to read. if it is null, the most recent version is returned
    * @return entry, if it exists, or null if not
    */
   InternalCacheEntry peek(Object k, EntryVersion version);

   /**
    * Puts an entry in the cache along with a lifespan and a maxIdle time
    * @param k key under which to store entry
    * @param v value to store
    * @param version the snapshot version of the new value
    * @param lifespan lifespan in milliseconds.  -1 means immortal.
    * @param maxIdle max idle time for which to store entry.  -1 means forever.
    */
   void put(Object k, Object v, EntryVersion version, long lifespan, long maxIdle);

   /**
    * Tests whether an entry exists in the container with a snapshot version less than the {@param version}
    *
    * @param k key to test
    * @param version the snapshot version to check. if it is null, it checks if the key exists independent of the version
    * @return true if entry exists and has not expired; false otherwise
    */
   boolean containsKey(Object k, EntryVersion version);

   /**
    * Removes an entry from the cache
    *
    * @param k key to remove
    * @param version the snapshot version to set to the removed value. if it is null, the key is removed with all the 
    *                version chain
    * @return entry removed, or null if it didn't exist or had expired
    */
   InternalCacheEntry remove(Object k, EntryVersion version);

   /**
    *
    * @return count of the number of entries in the container with a snapshot version less than the {@param version} 
    * @param version the snapshot version to check. if it is null, it counts the number of entries
    */
   int size(EntryVersion version);

   /**
    * Removes all entries in the container (including the version chain)
    */
   @Stop(priority = 999)
   void clear();

   /**
    * see {@link #clear()}. This method puts a remove entry value with version {@param version}
    *
    * @param version the new version of the values deleted.
    */
   void clear(EntryVersion version);

   /**
    * Returns a set of keys in the container. When iterating through the container using this method,
    * clients should never call {@link #get(Object, org.infinispan.container.versioning.EntryVersion)}
    * method but instead {@link #peek(Object, org.infinispan.container.versioning.EntryVersion)}, in order to avoid
    * changing the order of the underlying collection as a side of effect of iterating through it.
    * 
    * @return a set of keys
    * @param version the snapshot version. if it is null, it returns all the current keys
    */
   Set<Object> keySet(EntryVersion version);

   /**
    * @return a set of values contained in the container
    * @param version the snapshot version of the values. if it is null, it returns the most recent value of each key
    */
   Collection<Object> values(EntryVersion version);

   /**
    * Returns a mutable set of immutable cache entries exposed as immutable Map.Entry instances. Clients 
    * of this method such as Cache.entrySet() operation implementors are free to convert the set into an 
    * immutable set if needed, which is the most common use case. 
    * 
    * If a client needs to iterate through a mutable set of mutable cache entries, it should iterate the 
    * container itself rather than iterating through the return of entrySet().
    * 
    * @return a set of immutable cache entries
    * @param version the snapshot version of the entries. if it is null, the most recent version of each key is returned
    */
   Set<InternalCacheEntry> entrySet(EntryVersion version);

   /**
    * Purges entries that have passed their expiry time
    */
   void purgeExpired();

   boolean dumpTo(String filePath);

   void gc(EntryVersion minimumVersion);
}
