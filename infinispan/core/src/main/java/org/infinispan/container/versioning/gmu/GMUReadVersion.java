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
package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.remoting.transport.Address;

import java.util.Set;
import java.util.TreeSet;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.BEFORE;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUReadVersion extends GMUVersion {

   private final long version;
   private final Set<Pair> notVisibleSubVersions;

   public GMUReadVersion(String cacheName, int viewId, GMUVersionGenerator versionGenerator, long version) {
      super(cacheName, viewId, versionGenerator);
      this.version = version;
      this.notVisibleSubVersions = new TreeSet<Pair>();
   }

   @Override
   public final long getVersionValue(Address address) {
      return getThisNodeVersionValue();
   }

   @Override
   public final long getVersionValue(int addressIndex) {
      return getThisNodeVersionValue();
   }

   @Override
   public long getThisNodeVersionValue() {
      return version;
   }

   public final void addNotVisibleSubversion(long version, int subVersion) {
      notVisibleSubVersions.add(new Pair(version, subVersion));
   }

   public final boolean contains(long version, int subVersion) {
      return notVisibleSubVersions.contains(new Pair(version, subVersion));
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      //only comparable with GMU cache entry version
      if (other == null) {
         return BEFORE;
      } else if (other instanceof GMUCacheEntryVersion) {
         GMUCacheEntryVersion cacheEntryVersion = (GMUCacheEntryVersion) other;
         if (contains(cacheEntryVersion.getThisNodeVersionValue(), cacheEntryVersion.getSubVersion())) {
            //the cache entry is an invalid version.
            return BEFORE;
         }
         return compare(version, cacheEntryVersion.getThisNodeVersionValue());
      }
      throw new IllegalArgumentException("Cannot compare " + getClass() + " with " + other.getClass());
   }

   @Override
   public String toString() {
      return "GMUReadVersion{" +
            "version=" + version +
            ", notVisibleSubVersions=" + notVisibleSubVersions +
            ", " + super.toString();
   }

   private class Pair implements Comparable<Pair> {
      private final long version;
      private final int subVersion;

      private Pair(long version, int subVersion) {
         this.version = version;
         this.subVersion = subVersion;
      }

      @Override
      public int compareTo(Pair pair) {
         int result = Long.valueOf(version).compareTo(pair.version);
         if (result == 0) {
            return Integer.valueOf(subVersion).compareTo(pair.subVersion);
         }
         return result;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Pair pair = (Pair) o;
         return subVersion == pair.subVersion && version == pair.version;

      }

      @Override
      public int hashCode() {
         int result = (int) (version ^ (version >>> 32));
         result = 31 * result + subVersion;
         return result;
      }

      @Override
      public String toString() {
         return "(" + version + "," + subVersion + ")";
      }
   }
}
