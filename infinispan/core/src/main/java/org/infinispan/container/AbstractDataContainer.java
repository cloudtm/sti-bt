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

package org.infinispan.container;

import org.infinispan.CacheException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.util.Immutables;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap;
import org.infinispan.util.concurrent.ConcurrentMapFactory;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class AbstractDataContainer<T> implements DataContainer {

   protected final ConcurrentMap<Object, T> entries;
   protected InternalEntryFactory entryFactory;
   private EvictionManager evictionManager;
   private PassivationManager passivator;
   private ActivationManager activator;
   private CacheLoaderManager clm;

   protected AbstractDataContainer(int concurrencyLevel) {
      entries = ConcurrentMapFactory.makeConcurrentMap(128, concurrencyLevel);
   }

   protected AbstractDataContainer(int concurrencyLevel, int maxEntries, EvictionStrategy strategy, EvictionThreadPolicy policy) {
      // translate eviction policy and strategy
      BoundedConcurrentHashMap.EvictionListener<Object, T> evictionListener;
      switch (policy) {
         case PIGGYBACK:
         case DEFAULT:
            evictionListener = new DefaultEvictionListener();
            break;
         default:
            throw new IllegalArgumentException("No such eviction thread policy " + strategy);
      }

      BoundedConcurrentHashMap.Eviction eviction;
      switch (strategy) {
         case FIFO:
         case UNORDERED:
         case LRU:
            eviction = BoundedConcurrentHashMap.Eviction.LRU;
            break;
         case LIRS:
            eviction = BoundedConcurrentHashMap.Eviction.LIRS;
            break;
         default:
            throw new IllegalArgumentException("No such eviction strategy " + strategy);
      }
      entries = new BoundedConcurrentHashMap<Object, T>(maxEntries, concurrencyLevel, eviction, evictionListener);
   }

   @Inject
   public void initialize(EvictionManager evictionManager, PassivationManager passivator,
                          InternalEntryFactory entryFactory, ActivationManager activator, CacheLoaderManager clm) {
      this.evictionManager = evictionManager;
      this.passivator = passivator;
      this.entryFactory = entryFactory;
      this.activator = activator;
      this.clm = clm;

   }

   @Override
   public Set<Object> keySet(EntryVersion version) {
      return Collections.unmodifiableSet(entries.keySet());
   }

   @Override
   public Collection<Object> values(EntryVersion version) {
      return new Values(version);
   }

   @Override
   public Set<InternalCacheEntry> entrySet(EntryVersion version) {
      return new EntrySet(version);
   }

   @Override
   public Iterator<InternalCacheEntry> iterator() {
      return createEntryIterator(null);
   }

   protected abstract Map<Object, InternalCacheEntry> getCacheEntries(Map<Object, T> evicted);

   protected abstract InternalCacheEntry getCacheEntry(T evicted);

   protected abstract InternalCacheEntry getCacheEntry(T entry, EntryVersion version);

   protected abstract EntryIterator createEntryIterator(EntryVersion version);

   protected abstract static class EntryIterator implements Iterator<InternalCacheEntry> {
   }

   private final class DefaultEvictionListener implements BoundedConcurrentHashMap.EvictionListener<Object, T> {

      @Override
      public void onEntryEviction(Map<Object, T> evicted) {
         evictionManager.onEntryEviction(getCacheEntries(evicted));
      }

      @Override
      public void onEntryChosenForEviction(T entry) {
         passivator.passivate(getCacheEntry(entry));
      }

      @Override
      public void onEntryActivated(Object key) {
         activator.activate(key);
      }

      @Override
      public void onEntryRemoved(Object key) {
         try {
            CacheStore cacheStore = clm.getCacheStore();
            if (cacheStore != null)
               cacheStore.remove(key);
         } catch (CacheLoaderException e) {
            throw new CacheException(e);
         }
      }
   }

   private class ImmutableEntryIterator implements Iterator<InternalCacheEntry> {

      private final EntryIterator entryIterator;

      ImmutableEntryIterator(EntryIterator entryIterator) {
         this.entryIterator = entryIterator;
      }

      @Override
      public boolean hasNext() {
         return entryIterator.hasNext();
      }

      @Override
      public InternalCacheEntry next() {
         return Immutables.immutableInternalCacheEntry(entryIterator.next());
      }

      @Override
      public void remove() {
         entryIterator.remove();
      }
   }

   /**
    * Minimal implementation needed for unmodifiable Set
    */
   public class EntrySet extends AbstractSet<InternalCacheEntry> {

      private final EntryVersion version;

      public EntrySet(EntryVersion version) {
         this.version = version;
      }

      @Override
      public boolean contains(Object o) {
         if (!(o instanceof Map.Entry)) {
            return false;
         }

         Map.Entry e = (Map.Entry) o;
         InternalCacheEntry ice = getCacheEntry(entries.get(e.getKey()), version);
         return ice != null && ice.getValue().equals(e.getValue());
      }

      @Override
      public Iterator<InternalCacheEntry> iterator() {
         return new ImmutableEntryIterator(createEntryIterator(version));
      }

      @Override
      public int size() {
         return AbstractDataContainer.this.size(version);
      }
   }

   /**
    * Minimal implementation needed for unmodifiable Collection
    */
   private class Values extends AbstractCollection<Object> {

      private final EntryVersion version;

      private Values(EntryVersion version) {
         this.version = version;
      }

      @Override
      public Iterator<Object> iterator() {
         return new ValueIterator(createEntryIterator(version));
      }

      @Override
      public int size() {
         return AbstractDataContainer.this.size(version);
      }
   }

   private class ValueIterator implements Iterator<Object> {
      private final EntryIterator currentIterator;

      private ValueIterator(EntryIterator it) {
         currentIterator = it;
      }

      @Override
      public boolean hasNext() {
         return currentIterator.hasNext();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      @Override
      public Object next() {
         return currentIterator.next().getValue();
      }
   }
}
