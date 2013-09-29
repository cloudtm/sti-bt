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
package org.infinispan.container.gmu;

import org.infinispan.container.AbstractDataContainer;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUNullCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMURemovedCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.gmu.EvictedVersion;
import org.infinispan.container.versioning.gmu.GMUCacheEntryVersion;
import org.infinispan.container.versioning.gmu.GMUReadVersion;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.infinispan.container.gmu.GMUEntryFactoryImpl.wrap;
import static org.infinispan.transaction.gmu.GMUHelper.convert;
import static org.infinispan.transaction.gmu.GMUHelper.toInternalGMUCacheEntry;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUDataContainer extends AbstractDataContainer<GMUDataContainer.DataContainerVersionChain> {

   private static final Log log = LogFactory.getLog(GMUDataContainer.class);
   private CommitLog commitLog;

   protected GMUDataContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   protected GMUDataContainer(int concurrencyLevel, int maxEntries, EvictionStrategy strategy, EvictionThreadPolicy policy) {
      super(concurrencyLevel, maxEntries, strategy, policy);
   }

   public static DataContainer boundedDataContainer(int concurrencyLevel, int maxEntries,
                                                    EvictionStrategy strategy, EvictionThreadPolicy policy) {
      return new GMUDataContainer(concurrencyLevel, maxEntries, strategy, policy);
   }

   public static DataContainer unBoundedDataContainer(int concurrencyLevel) {
      return new GMUDataContainer(concurrencyLevel);
   }

   @Inject
   public void setCommitLog(CommitLog commitLog) {
      this.commitLog = commitLog;
   }

   @Override
   public InternalCacheEntry get(Object k, EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.get(%s,%s)", k, version);
      }
      InternalCacheEntry entry = peek(k, version);
      long now = System.currentTimeMillis();
      if (entry.canExpire() && entry.isExpired(now)) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.get(%s,%s) => EXPIRED", k, version);
         }

         return new InternalGMUNullCacheEntry(toInternalGMUCacheEntry(entry));
      }
      entry.touch(now);

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.get(%s,%s) => %s", k, version, entry);
      }

      return entry;
   }

   @Override
   public InternalCacheEntry peek(Object k, EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.peek(%s,%s)", k, version);
      }

      DataContainerVersionChain chain = entries.get(k);
      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.peek(%s,%s) => NOT_FOUND", k, version);
         }
         return wrap(k, null, true, version, null, null);
      }
      
      EntryVersion toRead = getReadVersion(version);
      VersionEntry<InternalCacheEntry> entry = chain.get(toRead);
      // System.out.println(Thread.currentThread().getId() + " key " + k + "\tversion: " + version + " toRead: " + toRead + "\tentry: " + entry.getEntry() + "\tlastversion? " + entry.isMostRecent());

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.peek(%s,%s) => %s", k, version, entry);
      }
      EntryVersion creationVersion = entry.getEntry() == null ? null : entry.getEntry().getVersion();

      return wrap(k, entry.getEntry(), entry.isMostRecent(), version, creationVersion, entry.getNextVersion());
   }

   @Override
   public void put(Object k, Object v, EntryVersion version, long lifespan, long maxIdle) {
      if (version == null) {
         throw new IllegalArgumentException("Key cannot have null versions!");
      }
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.put(%s,%s,%s,%s,%s)", k, v, version, lifespan, maxIdle);
      }
      GMUCacheEntryVersion cacheEntryVersion = assertGMUCacheEntryVersion(version);
      DataContainerVersionChain chain = entries.get(k);
      
      // System.out.println(Thread.currentThread().getId() + " ==> PUT key " + k + "\tversion: " + version + "\tvalue: " + v);

      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.put(%s,%s,%s,%s,%s), create new VersionChain", k, v, version, lifespan, maxIdle);
         }
         chain = new DataContainerVersionChain();
         entries.put(k, chain);
      }

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.put(%s,%s,%s,%s,%s), correct version is %s", k, v, version, lifespan, maxIdle, cacheEntryVersion);
      }

      chain.add(entryFactory.create(k, v, cacheEntryVersion, lifespan, maxIdle));
      if (log.isTraceEnabled()) {
         StringBuilder stringBuilder = new StringBuilder();
         chain.chainToString(stringBuilder);
         log.tracef("Updated chain is %s", stringBuilder);
      }
   }

   @Override
   public boolean containsKey(Object k, EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.containsKey(%s,%s)", k, version);
      }

      VersionChain chain = entries.get(k);
      boolean contains = chain != null && chain.contains(getReadVersion(version));

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.containsKey(%s,%s) => %s", k, version, contains);
      }

      return contains;
   }

   @Override
   public InternalCacheEntry remove(Object k, EntryVersion version) {
      if (version == null) {
         throw new IllegalArgumentException("Key cannot have null version!");
      }
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.remove(%s,%s)", k, version);
      }
      if (version == EvictedVersion.INSTANCE) {
         entries.remove(k);
      }

      DataContainerVersionChain chain = entries.get(k);
      if (chain == null) {
         if (log.isTraceEnabled()) {
            log.tracef("DataContainer.remove(%s,%s) => NOT_FOUND", k, version);
         }
         return wrap(k, null, true, null, null, null);
      }
      VersionEntry<InternalCacheEntry> entry = chain.remove(new InternalGMURemovedCacheEntry(k, assertGMUCacheEntryVersion(version)));

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.remove(%s,%s) => %s", k, version, entry);
      }
      return wrap(k, entry.getEntry(), entry.isMostRecent(), null, null, null);
   }

   @Override
   public int size(EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.size(%s)", version);
      }
      int size = 0;
      for (VersionChain chain : entries.values()) {
         if (chain.contains(getReadVersion(version))) {
            size++;
         }
      }

      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.size(%s) => %s", version, size);
      }
      return size;
   }

   @Override
   public void clear() {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.clear()");
      }
      entries.clear();
   }

   @Override
   public void clear(EntryVersion version) {
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.clear(%s)", version);
      }
      for (Object key : entries.keySet()) {
         remove(key, version);
      }
   }

   @Override
   public void purgeExpired() {
      long currentTimeMillis = System.currentTimeMillis();
      if (log.isTraceEnabled()) {
         log.tracef("DataContainer.purgeExpired(%s)", currentTimeMillis);
      }
      for (VersionChain chain : entries.values()) {
         chain.purgeExpired(currentTimeMillis);
      }
   }

   @Override
   public final boolean dumpTo(String filePath) {
      BufferedWriter bufferedWriter = Util.getBufferedWriter(filePath);
      if (bufferedWriter == null) {
         return false;
      }
      try {
         for (Map.Entry<Object, DataContainerVersionChain> entry : entries.entrySet()) {
            Util.safeWrite(bufferedWriter, entry.getKey());
            Util.safeWrite(bufferedWriter, "=");
            entry.getValue().dumpChain(bufferedWriter);
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
   public final void gc(EntryVersion minimumVersion) {
      for (DataContainerVersionChain versionChain : entries.values()) {
         versionChain.gc(minimumVersion);
      }
   }

   public final VersionChain<?> getVersionChain(Object key) {
      return entries.get(key);
   }

   public final String stateToString() {
      StringBuilder stringBuilder = new StringBuilder(8132);
      for (Map.Entry<Object, DataContainerVersionChain> entry : entries.entrySet()) {
         stringBuilder.append(entry.getKey())
               .append("=");
         entry.getValue().chainToString(stringBuilder);
         stringBuilder.append("\n");
      }
      return stringBuilder.toString();
   }

   @Override
   protected Map<Object, InternalCacheEntry> getCacheEntries(Map<Object, DataContainerVersionChain> evicted) {
      Map<Object, InternalCacheEntry> evictedMap = new HashMap<Object, InternalCacheEntry>();
      for (Map.Entry<Object, DataContainerVersionChain> entry : evicted.entrySet()) {
         evictedMap.put(entry.getKey(), entry.getValue().get(null).getEntry());
      }
      return evictedMap;
   }

   @Override
   protected InternalCacheEntry getCacheEntry(DataContainerVersionChain evicted) {
      return evicted.get(null).getEntry();
   }

   @Override
   protected InternalCacheEntry getCacheEntry(DataContainerVersionChain entry, EntryVersion version) {
      return entry == null ? null : entry.get(version).getEntry();
   }

   @Override
   protected EntryIterator createEntryIterator(EntryVersion version) {
      return new GMUEntryIterator(version, entries.values().iterator());
   }

   private GMUCacheEntryVersion assertGMUCacheEntryVersion(EntryVersion entryVersion) {
      return convert(entryVersion, GMUCacheEntryVersion.class);
   }

   private GMUReadVersion getReadVersion(EntryVersion entryVersion) {
      GMUReadVersion gmuReadVersion = commitLog.getReadVersion(entryVersion);
      if (log.isDebugEnabled()) {
         log.debugf("getReadVersion(%s) ==> %s", entryVersion, gmuReadVersion);
      }
      return gmuReadVersion;
   }

   public static class DataContainerVersionChain extends VersionChain<InternalCacheEntry> {

      @Override
      protected VersionBody<InternalCacheEntry> newValue(InternalCacheEntry value) {
         return new DataContainerVersionBody(value);
      }

      @Override
      protected void writeValue(BufferedWriter writer, InternalCacheEntry value) throws IOException {
         writer.write(String.valueOf(value.getValue()));
         writer.write("=");
         writer.write(String.valueOf(value.getVersion()));
      }
   }

   public static class DataContainerVersionBody extends VersionBody<InternalCacheEntry> {

      protected DataContainerVersionBody(InternalCacheEntry value) {
         super(value);
      }

      @Override
      public EntryVersion getVersion() {
         return getValue().getVersion();
      }

      @Override
      public boolean isOlder(VersionBody<InternalCacheEntry> otherBody) {
         return isOlder(getValue().getVersion(), otherBody.getVersion());
      }

      @Override
      public boolean isOlderOrEquals(EntryVersion entryVersion) {
         return isOlderOrEquals(getValue().getVersion(), entryVersion);
      }

      @Override
      public boolean isEqual(VersionBody<InternalCacheEntry> otherBody) {
         return isEqual(getValue().getVersion(), otherBody.getVersion());
      }

      @Override
      public boolean isRemove() {
         return getValue().isRemoved();
      }

      @Override
      public void reincarnate(VersionBody<InternalCacheEntry> other) {
         //throw new IllegalStateException("This cannot happen");
         //ignored since the entries loaded from cache store can be committed multiple times.
      }

      @Override
      public VersionBody<InternalCacheEntry> gc(EntryVersion minVersion) {
         if (isOlderOrEquals(getValue().getVersion(), minVersion)) {
            VersionBody<InternalCacheEntry> previous = getPrevious();
            //GC previous entries, removing all the references to the previous version entry
            setPrevious(null);
            return previous;
         } else {
            return getPrevious();
         }
      }

      @Override
      protected boolean isExpired(long now) {
         InternalCacheEntry entry = getValue();
         return entry != null && entry.canExpire() && entry.isExpired(now);
      }
   }

   private class GMUEntryIterator extends EntryIterator {

      private final EntryVersion version;
      private final Iterator<DataContainerVersionChain> iterator;
      private InternalCacheEntry next;

      private GMUEntryIterator(EntryVersion version, Iterator<DataContainerVersionChain> iterator) {
         this.version = version;
         this.iterator = iterator;
         findNext();
      }

      @Override
      public boolean hasNext() {
         return next != null;
      }

      @Override
      public InternalCacheEntry next() {
         if (next == null) {
            throw new NoSuchElementException();
         }
         InternalCacheEntry toReturn = next;
         findNext();
         return toReturn;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      private void findNext() {
         next = null;
         while (iterator.hasNext()) {
            DataContainerVersionChain chain = iterator.next();
            next = chain.get(version).getEntry();
            if (next != null) {
               return;
            }
         }
      }
   }
}
