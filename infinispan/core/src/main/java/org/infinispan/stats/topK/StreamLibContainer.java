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
package org.infinispan.stats.topK;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This contains all the stream lib top keys. Stream lib is a space efficient technique to obtains the top-most
 * counters.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StreamLibContainer {

   public static final int MAX_CAPACITY = 100000;
   private static final Log log = LogFactory.getLog(StreamLibContainer.class);
   private final String cacheName;
   private final String address;
   private final AtomicBoolean flushing;
   private final EnumMap<Stat, TopKeyWrapper> topKeyWrapper;
   private volatile int capacity = 1000;
   private volatile boolean active = false;
   private volatile boolean reset = false;

   public StreamLibContainer(String cacheName, String address) {
      this.cacheName = cacheName;
      this.address = address;
      topKeyWrapper = new EnumMap<Stat, TopKeyWrapper>(Stat.class);
      for (Stat stat : Stat.values()) {
         topKeyWrapper.put(stat, new TopKeyWrapper());
      }
      flushing = new AtomicBoolean(false);
   }

   public static StreamLibContainer getOrCreateStreamLibContainer(Cache cache) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      StreamLibContainer streamLibContainer = componentRegistry.getComponent(StreamLibContainer.class);
      if (streamLibContainer == null) {
         String cacheName = cache.getName();
         String address = String.valueOf(cache.getCacheManager().getAddress());
         componentRegistry.registerComponent(new StreamLibContainer(cacheName, address), StreamLibContainer.class);
      }
      return componentRegistry.getComponent(StreamLibContainer.class);
   }

   public boolean isActive() {
      return active;
   }

   public void setActive(boolean active) {
      if (!this.active && active) {
         resetAll();
      } else if (!active) {
         resetAll();
      }
      this.active = active;
   }

   public int getCapacity() {
      return capacity;
   }

   public void setCapacity(int capacity) {
      if (capacity <= 0) {
         this.capacity = 1;
      } else {
         this.capacity = capacity;
      }
   }

   public void addGet(Object key, boolean remote) {
      if (!isActive()) {
         return;
      }
      syncOffer(remote ? Stat.REMOTE_GET : Stat.LOCAL_GET, key);
      tryFlushAll();
   }

   public void addPut(Object key, boolean remote) {
      if (!isActive()) {
         return;
      }

      syncOffer(remote ? Stat.REMOTE_PUT : Stat.LOCAL_PUT, key);
      tryFlushAll();
   }

   public void addLockInformation(Object key, boolean contention, boolean abort) {
      if (!isActive()) {
         return;
      }

      syncOffer(Stat.MOST_LOCKED_KEYS, key);

      if (contention) {
         syncOffer(Stat.MOST_CONTENDED_KEYS, key);
      }
      if (abort) {
         syncOffer(Stat.MOST_FAILED_KEYS, key);
      }
      tryFlushAll();
   }

   public void addWriteSkewFailed(Object key) {
      syncOffer(Stat.MOST_WRITE_SKEW_FAILED_KEYS, key);
      tryFlushAll();
   }

   public Map<Object, Long> getTopKFrom(Stat stat) {
      return getTopKFrom(stat, capacity);
   }

   public Map<Object, Long> getTopKFrom(Stat stat, int topK) {
      tryFlushAll();
      return topKeyWrapper.get(stat).topK(topK);
   }

   public Map<String, Long> getTopKFromAsKeyString(Stat stat) {
      return getTopKFromAsKeyString(stat, capacity);
   }

   public Map<String, Long> getTopKFromAsKeyString(Stat stat, int topK) {
      tryFlushAll();
      return topKeyWrapper.get(stat).topKAsString(topK);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StreamLibContainer that = (StreamLibContainer) o;

      return !(address != null ? !address.equals(that.address) : that.address != null) && !(cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null);

   }

   @Override
   public int hashCode() {
      int result = cacheName != null ? cacheName.hashCode() : 0;
      result = 31 * result + (address != null ? address.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "StreamLibContainer{" +
            "cacheName='" + cacheName + '\'' +
            ", address='" + address + '\'' +
            '}';
   }

   public final void tryFlushAll() {
      if (flushing.compareAndSet(false, true)) {
         if (reset) {
            for (Stat stat : Stat.values()) {
               topKeyWrapper.get(stat).reset(createNewStreamSummary(capacity));
            }
            reset = false;
         } else {
            for (Stat stat : Stat.values()) {
               topKeyWrapper.get(stat).flush();
            }
         }
         flushing.set(false);
      }
   }

   public final void resetAll() {
      reset = true;
      tryFlushAll();
   }

   private StreamSummary<Object> createNewStreamSummary(int customCapacity) {
      return new StreamSummary<Object>(Math.min(MAX_CAPACITY, customCapacity));
   }

   private void syncOffer(final Stat stat, Object key) {
      if (log.isTraceEnabled()) {
         log.tracef("Offer key=%s to stat=%s in %s", key, stat, this);
      }
      topKeyWrapper.get(stat).offer(key);
   }

   public static enum Stat {
      REMOTE_GET,
      LOCAL_GET,
      REMOTE_PUT,
      LOCAL_PUT,

      MOST_LOCKED_KEYS,
      MOST_CONTENDED_KEYS,
      MOST_FAILED_KEYS,
      MOST_WRITE_SKEW_FAILED_KEYS
   }

   private class TopKeyWrapper {
      private final BlockingQueue<Object> pendingOffers;
      private volatile StreamSummary<Object> streamSummary;

      public TopKeyWrapper() {
         pendingOffers = new LinkedBlockingQueue<Object>();
         streamSummary = createNewStreamSummary(capacity);
      }

      private void offer(final Object element) {
         pendingOffers.add(element);
      }

      private void reset(StreamSummary<Object> streamSummary) {
         pendingOffers.clear();
         this.streamSummary = streamSummary;
      }

      private void flush() {
         List<Object> keys = new ArrayList<Object>();
         pendingOffers.drainTo(keys);
         synchronized (this) {
            for (Object k : keys) {
               streamSummary.offer(k);
            }
         }
      }

      private Map<Object, Long> topK(int k) {
         List<Counter<Object>> counterList;
         synchronized (this) {
            counterList = streamSummary.topK(k);
         }
         Map<Object, Long> map = new LinkedHashMap<Object, Long>();
         for (Counter<Object> counter : counterList) {
            map.put(counter.getItem(), counter.getCount());
         }
         return map;
      }

      private Map<String, Long> topKAsString(int k) {
         List<Counter<Object>> counterList;
         synchronized (this) {
            counterList = streamSummary.topK(k);
         }
         Map<String, Long> map = new LinkedHashMap<String, Long>();
         for (Counter<Object> counter : counterList) {
            map.put(String.valueOf(counter.getItem()), counter.getCount());
         }
         return map;
      }
   }
}
