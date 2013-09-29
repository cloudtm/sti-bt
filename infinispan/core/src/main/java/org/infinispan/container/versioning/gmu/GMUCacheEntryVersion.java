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
import org.infinispan.dataplacement.ClusterSnapshot;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUCacheEntryVersion extends GMUVersion {

   private final long version;
   private final int subVersion;

   public GMUCacheEntryVersion(String cacheName, int viewId, GMUVersionGenerator versionGenerator, long version,
                               int subVersion) {
      super(cacheName, viewId, versionGenerator);
      this.version = version;
      this.subVersion = subVersion;
   }

   private GMUCacheEntryVersion(String cacheName, int viewId, ClusterSnapshot clusterSnapshot,
                                long version, int subVersion) {
      super(cacheName, viewId, clusterSnapshot);
      this.version = version;
      this.subVersion = subVersion;
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
   public final long getThisNodeVersionValue() {
      return version;
   }

   public final int getSubVersion() {
      return subVersion;
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      //this particular version can only be compared with this type of GMU version or with GMUReadVersion
      if (other == null) {
         return BEFORE;
      } else if (other instanceof GMUCacheEntryVersion) {
         GMUCacheEntryVersion cacheEntryVersion = (GMUCacheEntryVersion) other;
         InequalVersionComparisonResult result = compare(version, cacheEntryVersion.version);
         if (result == EQUAL) {
            return compare(subVersion, cacheEntryVersion.subVersion);
         }
         return result;
      } else if (other instanceof GMUReadVersion) {
         GMUReadVersion readVersion = (GMUReadVersion) other;
         if (readVersion.contains(version, subVersion)) {
            //this is an invalid version. set it higher
            return AFTER;
         }
         return compare(version, readVersion.getThisNodeVersionValue());
      } else if (other instanceof GMUReplicatedVersion) {
         GMUReplicatedVersion replicatedVersion = (GMUReplicatedVersion) other;
         InequalVersionComparisonResult result = compare(version, replicatedVersion.getThisNodeVersionValue());
         if (result == EQUAL) {
            return compare(viewId, replicatedVersion.getViewId());
         }
         return result;
      }  else if (other instanceof GMUDistributedVersion) {
         GMUDistributedVersion distributedVersion = (GMUDistributedVersion) other;
         InequalVersionComparisonResult result = compare(version, distributedVersion.getThisNodeVersionValue());
         if (result == EQUAL) {
            return compare(viewId, distributedVersion.getViewId());
         }
         return result;
      }
      throw new IllegalArgumentException("Cannot compare " + getClass() + " with " + other.getClass());
   }

   @Override
   public String toString() {
      return "GMUCacheEntryVersion{" +
            "version=" + version +
            ", subVersion=" + subVersion +
            ", " + super.toString();
   }

   public static class Externalizer extends AbstractExternalizer<GMUCacheEntryVersion> {

      private final GlobalComponentRegistry globalComponentRegistry;

      public Externalizer(GlobalComponentRegistry globalComponentRegistry) {
         this.globalComponentRegistry = globalComponentRegistry;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Set<Class<? extends GMUCacheEntryVersion>> getTypeClasses() {
         return Util.<Class<? extends GMUCacheEntryVersion>>asSet(GMUCacheEntryVersion.class);
      }

      @Override
      public void writeObject(ObjectOutput output, GMUCacheEntryVersion object) throws IOException {
         output.writeUTF(object.cacheName);
         output.writeInt(object.viewId);
         output.writeLong(object.version);
         output.writeInt(object.subVersion);
      }

      @Override
      public GMUCacheEntryVersion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = input.readUTF();
         GMUVersionGenerator gmuVersionGenerator = getGMUVersionGenerator(globalComponentRegistry, cacheName);
         int viewId = input.readInt();
         ClusterSnapshot clusterSnapshot = gmuVersionGenerator.getClusterSnapshot(viewId);
         if (clusterSnapshot == null) {
            throw new IllegalArgumentException("View Id " + viewId + " not found in this node");
         }
         long version = input.readLong();
         int subVersion = input.readInt();
         return new GMUCacheEntryVersion(cacheName, viewId, clusterSnapshot, version, subVersion);
      }

      @Override
      public Integer getId() {
         return Ids.GMU_CACHE_VERSION;
      }
   }
}
