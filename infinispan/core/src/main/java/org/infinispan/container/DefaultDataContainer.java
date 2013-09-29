/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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

import net.jcip.annotations.ThreadSafe;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.util.Util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * DefaultDataContainer is both eviction and non-eviction based data container.
 *
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 4.0
 */
@ThreadSafe
public class DefaultDataContainer extends AbstractDataContainer<InternalCacheEntry> {

   public DefaultDataContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   protected DefaultDataContainer(int concurrencyLevel, int maxEntries, EvictionStrategy strategy, EvictionThreadPolicy policy) {
      super(concurrencyLevel, maxEntries, strategy, policy);
   }

   public static DataContainer boundedDataContainer(int concurrencyLevel, int maxEntries,
            EvictionStrategy strategy, EvictionThreadPolicy policy) {
      return new DefaultDataContainer(concurrencyLevel, maxEntries, strategy, policy);
   }

   public static DataContainer unBoundedDataContainer(int concurrencyLevel) {
      return new DefaultDataContainer(concurrencyLevel);
   }

   @Override
   public InternalCacheEntry peek(Object key, EntryVersion version) {
      return entries.get(key);
   }

   @Override
   public InternalCacheEntry get(Object k, EntryVersion version) {
      InternalCacheEntry e = peek(k, version);
      if (e != null && e.canExpire()) {
         long currentTimeMillis = System.currentTimeMillis();
         if (e.isExpired(currentTimeMillis)) {
            entries.remove(k);
            e = null;
         } else {
            e.touch(currentTimeMillis);
         }
      }
      return e;
   }

   @Override
   public void put(Object k, Object v, EntryVersion version, long lifespan, long maxIdle) {
      InternalCacheEntry e = entries.get(k);
      if (e != null) {
         e.setValue(v);
         InternalCacheEntry original = e;
         e.setVersion(version);
         e = entryFactory.update(e, lifespan, maxIdle);
         // we have the same instance. So we need to reincarnate.
         if(original == e) {
            e.reincarnate();
         }
      } else {
         // this is a brand-new entry
         e = entryFactory.create(k, v, version, lifespan, maxIdle);
      }
      entries.put(k, e);
   }

   @Override
   public boolean containsKey(Object k, EntryVersion version) {
      InternalCacheEntry ice = peek(k, null);
      if (ice != null && ice.canExpire() && ice.isExpired(System.currentTimeMillis())) {
         entries.remove(k);
         ice = null;
      }
      return ice != null;
   }

   @Override
   public InternalCacheEntry remove(Object k, EntryVersion version) {
      InternalCacheEntry e = entries.remove(k);
      return e == null || (e.canExpire() && e.isExpired(System.currentTimeMillis())) ? null : e;
   }

   @Override
   public int size(EntryVersion version) {
      return entries.size();
   }

   @Override
   public void clear() {
      entries.clear();
   }

   @Override
   public void clear(EntryVersion version) {
      clear();
   }

   @Override
   public void purgeExpired() {
      long currentTimeMillis = System.currentTimeMillis();
      for (Iterator<InternalCacheEntry> purgeCandidates = entries.values().iterator(); purgeCandidates.hasNext();) {
         InternalCacheEntry e = purgeCandidates.next();
         if (e.isExpired(currentTimeMillis)) {
            purgeCandidates.remove();
         }
      }
   }

   @Override
   protected Map<Object, InternalCacheEntry> getCacheEntries(Map<Object, InternalCacheEntry> evicted) {
      return evicted;
   }

   @Override
   protected InternalCacheEntry getCacheEntry(InternalCacheEntry evicted) {
      return evicted;
   }

   @Override
   protected InternalCacheEntry getCacheEntry(InternalCacheEntry entry, EntryVersion version) {
      return entry;
   }

   @Override
   protected EntryIterator createEntryIterator(EntryVersion version) {
      return new DefaultEntryIterator(entries.values().iterator());
   }

   @Override
   public final boolean dumpTo(String filePath) {
      BufferedWriter bufferedWriter = Util.getBufferedWriter(filePath);
      if (bufferedWriter == null) {
         return false;
      }
      try {
         for (Map.Entry<Object, InternalCacheEntry> entry : entries.entrySet()) {
            Util.safeWrite(bufferedWriter, entry.getKey());
            Util.safeWrite(bufferedWriter, "=");
            Util.safeWrite(bufferedWriter, entry.getValue().getValue());
            Util.safeWrite(bufferedWriter, "=");
            Util.safeWrite(bufferedWriter, entry.getValue().getVersion());
            bufferedWriter.newLine();
            bufferedWriter.flush();
         }
         return true;
      } catch (IOException e) {
         return false;
      } finally {
         Util.close(bufferedWriter);
      }
   }

   @Override
   public void gc(EntryVersion minimumVersion) {
      //no-op
   }

   public static class DefaultEntryIterator extends EntryIterator {

      private final Iterator<InternalCacheEntry> it;

      DefaultEntryIterator(Iterator<InternalCacheEntry> it){this.it=it;}

      @Override
      public InternalCacheEntry next() {
         return it.next();
      }

      @Override
      public boolean hasNext() {
         return it.hasNext();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }
   }
}
