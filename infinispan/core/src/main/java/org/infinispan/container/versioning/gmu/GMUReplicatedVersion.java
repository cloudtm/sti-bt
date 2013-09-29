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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUReplicatedVersion extends GMUVersion {

   private static final Log log = LogFactory.getLog(GMUReplicatedVersion.class);

   private final long version;

   public GMUReplicatedVersion(String cacheName, int viewId, GMUVersionGenerator versionGenerator, long version) {
      super(cacheName, viewId, versionGenerator);
      this.version = version;
   }

   private GMUReplicatedVersion(String cacheName, int viewId, ClusterSnapshot clusterSnapshot, long version) {
      super(cacheName, viewId, clusterSnapshot);
      this.version = version;
   }

   @Override
   public long getVersionValue(Address address) {
      return getThisNodeVersionValue();
   }

   @Override
   public long getVersionValue(int addressIndex) {
      return getThisNodeVersionValue();
   }

   @Override
   public long getThisNodeVersionValue() {
      return version;
   }

   @Override
   public InequalVersionComparisonResult compareTo(EntryVersion other) {
      if (other instanceof GMUReplicatedVersion) {
         GMUReplicatedVersion cacheEntryVersion = (GMUReplicatedVersion) other;
         InequalVersionComparisonResult versionComparisonResult = compare(this.version, cacheEntryVersion.version);

         if (versionComparisonResult == InequalVersionComparisonResult.EQUAL) {
            versionComparisonResult = compare(this.viewId, cacheEntryVersion.viewId);
         }

         if (log.isTraceEnabled()) {
            log.tracef("Compare this[%s] with other[%s] => %s", this, other, versionComparisonResult);
         }
         return versionComparisonResult;
      }

      if (other instanceof GMUDistributedVersion) {
         GMUDistributedVersion clusterEntryVersion = (GMUDistributedVersion) other;
         InequalVersionComparisonResult versionComparisonResult = compare(this.version, clusterEntryVersion.getThisNodeVersionValue());

         if (versionComparisonResult == InequalVersionComparisonResult.EQUAL) {
            versionComparisonResult = compare(this.viewId, clusterEntryVersion.viewId);
         }

         if (log.isTraceEnabled()) {
            log.tracef("Compare this[%s] with other[%s] => %s", this, other, versionComparisonResult);
         }

         return versionComparisonResult;
      }

      throw new IllegalArgumentException("Cannot compare GMU entry versions with " + (other == null ? "N/A" :
                                                                                            other.getClass().getSimpleName()));
   }

   @Override
   public String toString() {
      return "GMUReplicatedVersion{" +
            "version=" + version +
            ", " + super.toString();
   }

   public static class Externalizer extends AbstractExternalizer<GMUReplicatedVersion> {

      private final GlobalComponentRegistry globalComponentRegistry;

      public Externalizer(GlobalComponentRegistry globalComponentRegistry) {
         this.globalComponentRegistry = globalComponentRegistry;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Set<Class<? extends GMUReplicatedVersion>> getTypeClasses() {
         return Util.<Class<? extends GMUReplicatedVersion>>asSet(GMUReplicatedVersion.class);
      }

      @Override
      public void writeObject(ObjectOutput output, GMUReplicatedVersion object) throws IOException {
         output.writeUTF(object.cacheName);
         output.writeInt(object.viewId);
         output.writeLong(object.version);
      }

      @Override
      public GMUReplicatedVersion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = input.readUTF();
         GMUVersionGenerator gmuVersionGenerator = getGMUVersionGenerator(globalComponentRegistry, cacheName);
         int viewId = input.readInt();
         ClusterSnapshot clusterSnapshot = gmuVersionGenerator.getClusterSnapshot(viewId);
         if (clusterSnapshot == null) {
            throw new IllegalArgumentException("View Id " + viewId + " not found in this node");
         }
         long version = input.readLong();
         return new GMUReplicatedVersion(cacheName, viewId, clusterSnapshot, version);
      }

      @Override
      public Integer getId() {
         return Ids.GMU_REPLICATED_VERSION;
      }
   }
}
