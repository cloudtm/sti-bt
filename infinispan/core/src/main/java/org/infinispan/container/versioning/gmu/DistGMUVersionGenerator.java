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

import org.infinispan.Cache;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.dataplacement.ClusterSnapshot;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import static org.infinispan.container.versioning.gmu.GMUVersion.NON_EXISTING;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DistGMUVersionGenerator implements GMUVersionGenerator {

   private static final Hash HASH = new MurmurHash3();
   private static final Log log = LogFactory.getLog(DistGMUVersionGenerator.class);
   private final TreeMap<Integer, ClusterSnapshot> viewIdClusterSnapshot;
   private RpcManager rpcManager;
   private String cacheName;
   private volatile int currentViewId;

   public DistGMUVersionGenerator() {
      viewIdClusterSnapshot = new TreeMap<Integer, ClusterSnapshot>();
   }

   @Inject
   public final void init(RpcManager rpcManager, Cache cache) {
      this.rpcManager = rpcManager;
      this.cacheName = cache.getName();
   }

   @Start(priority = 11) // needs to happen *after* the transport starts.
   public final void setEmptyViewId() {
      currentViewId = -1;
      viewIdClusterSnapshot.put(-1, new ClusterSnapshot(Collections.singleton(rpcManager.getAddress()), HASH));
   }

   @Override
   public final IncrementableEntryVersion generateNew() {
      int viewId = currentViewId;
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versions = create(true, clusterSnapshot.size());
      versions[clusterSnapshot.indexOf(getAddress())] = 0;
      return new GMUDistributedVersion(cacheName, currentViewId, this, versions);
   }

   @Override
   public final IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion) {
      if (initialVersion == null) {
         throw new NullPointerException("Cannot increment a null version");
      }

      GMUVersion gmuVersion = toGMUVersion(initialVersion);
      int viewId = currentViewId;
      GMUDistributedVersion incrementedVersion = new GMUDistributedVersion(cacheName, viewId, this,
                                                                           increment(gmuVersion, viewId));

      if (log.isTraceEnabled()) {
         log.tracef("increment(%s) ==> %s", initialVersion, incrementedVersion);
      }
      return incrementedVersion;
   }

   @Override
   public final GMUVersion mergeAndMax(EntryVersion... entryVersions) {
      if (entryVersions == null || entryVersions.length == 0) {
         throw new IllegalStateException("Cannot merge an empy list");
      }

      List<GMUVersion> gmuVersions = new ArrayList<GMUVersion>(entryVersions.length);
      //validate the entry versions
      for (EntryVersion entryVersion : entryVersions) {
         if (entryVersion == null) {
            log.errorf("Null version in list %s. It will be ignored", entryVersion);
         } else if (entryVersion instanceof GMUVersion) {
            gmuVersions.add(toGMUVersion(entryVersion));
         } else {
            throw new IllegalArgumentException("Expected an array of GMU entry version but it has " +
                                                     entryVersion.getClass().getSimpleName());
         }
      }

      int viewId = currentViewId;
      GMUDistributedVersion mergedVersion = new GMUDistributedVersion(cacheName, viewId, this,
                                                                      mergeClustered(viewId, gmuVersions));
      if (log.isTraceEnabled()) {
         log.tracef("mergeAndMax(%s) ==> %s", entryVersions, mergedVersion);
      }
      return mergedVersion;
   }

   @Override
   public GMUVersion mergeAndMin(EntryVersion... entryVersions) {
      int viewId = currentViewId;
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versions = create(true, clusterSnapshot.size());
      if (entryVersions.length == 0) {
         return new GMUDistributedVersion(cacheName, viewId, this, versions);
      }

      for (EntryVersion entryVersion : entryVersions) {
         GMUVersion gmuVersion = toGMUVersion(entryVersion);
         for (int i = 0; i < clusterSnapshot.size(); ++i) {
            long value = gmuVersion.getVersionValue(clusterSnapshot.get(i));
            if (versions[i] == NON_EXISTING) {
               versions[i] = value;
            } else if (value != NON_EXISTING) {
               versions[i] = Math.min(versions[i], value);
            }
         }
      }

      return new GMUDistributedVersion(cacheName, viewId, this, versions);
   }

   @Override
   public final GMUVersion calculateCommitVersion(EntryVersion prepareVersion,
                                                  Collection<Address> affectedOwners) {
      int viewId = currentViewId;
      GMUDistributedVersion commitVersion = new GMUDistributedVersion(cacheName, viewId, this,
                                                                      calculateVersionToCommit(viewId, prepareVersion,
                                                                                               affectedOwners));
      if (log.isTraceEnabled()) {
         log.tracef("calculateCommitVersion(%s,%s) ==> %s", prepareVersion, affectedOwners, commitVersion);
      }
      return commitVersion;
   }

   @Override
   public final GMUCacheEntryVersion convertVersionToWrite(EntryVersion version, int subVersion) {
      GMUVersion gmuVersion = toGMUVersion(version);
      GMUCacheEntryVersion cacheEntryVersion = new GMUCacheEntryVersion(cacheName, currentViewId, this,
                                                                        gmuVersion.getThisNodeVersionValue(), subVersion);

      if (log.isTraceEnabled()) {
         log.tracef("convertVersionToWrite(%s) ==> %s", version, cacheEntryVersion);
      }
      return cacheEntryVersion;
   }

   @Override
   public GMUReadVersion convertVersionToRead(EntryVersion version) {
      if (version == null) {
         return null;
      }
      GMUVersion gmuVersion = toGMUVersion(version);
      return new GMUReadVersion(cacheName, currentViewId, this, gmuVersion.getThisNodeVersionValue());
   }

   @Override
   public GMUVersion calculateMaxVersionToRead(EntryVersion transactionVersion,
                                               Collection<Address> alreadyReadFrom) {
      if (alreadyReadFrom == null || alreadyReadFrom.isEmpty()) {
         if (log.isTraceEnabled()) {
            log.tracef("calculateMaxVersionToRead(%s, %s) ==> null", transactionVersion, alreadyReadFrom);
         }
         return null;
      }
      //the max version is calculated with the position of the version in which this node has already read from
      GMUVersion gmuVersion = toGMUVersion(transactionVersion);

      int viewId = currentViewId;
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versionsValues = create(true, clusterSnapshot.size());

      for (Address readFrom : alreadyReadFrom) {
         int index = clusterSnapshot.indexOf(readFrom);
         if (index == -1) {
            //does not exists in current view (is this safe? -- I think that it depends of the state transfer...)
            continue;
         }
         versionsValues[index] = gmuVersion.getVersionValue(readFrom);
      }

      GMUDistributedVersion maxVersionToRead = new GMUDistributedVersion(cacheName, viewId, this, versionsValues);
      if (log.isTraceEnabled()) {
         log.tracef("calculateMaxVersionToRead(%s, %s) ==> %s", transactionVersion, alreadyReadFrom, maxVersionToRead);
      }
      return maxVersionToRead;
   }

   @Override
   public GMUVersion calculateMinVersionToRead(EntryVersion transactionVersion,
                                               Collection<Address> alreadyReadFrom) {
      if (transactionVersion == null) {
         throw new NullPointerException("Transaction Version cannot be null to calculate the min version to read");
      }
      if (alreadyReadFrom == null || alreadyReadFrom.isEmpty()) {
         if (log.isTraceEnabled()) {
            log.tracef("calculateMinVersionToRead(%s, %s) ==> %s", transactionVersion, alreadyReadFrom,
                       transactionVersion);
         }
         return updatedVersion(transactionVersion);
      }

      //the min version is defined by the nodes that we haven't read yet

      GMUVersion gmuVersion = toGMUVersion(transactionVersion);
      int viewId = currentViewId;
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versionValues = create(true, clusterSnapshot.size());

      for (int i = 0; i < clusterSnapshot.size(); ++i) {
         Address address = clusterSnapshot.get(i);
         if (alreadyReadFrom.contains(address)) {
            continue;
         }
         versionValues[i] = gmuVersion.getVersionValue(address);
      }

      GMUDistributedVersion minVersionToRead = new GMUDistributedVersion(cacheName, viewId, this, versionValues);
      if (log.isTraceEnabled()) {
         log.tracef("calculateMinVersionToRead(%s, %s) ==> %s", transactionVersion, alreadyReadFrom, minVersionToRead);
      }
      return minVersionToRead;
   }

   @Override
   public GMUVersion setNodeVersion(EntryVersion version, long value) {

      GMUVersion gmuVersion = toGMUVersion(version);
      int viewId = currentViewId;
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versionValues = create(true, clusterSnapshot.size());

      for (int i = 0; i < clusterSnapshot.size(); ++i) {
         Address address = clusterSnapshot.get(i);
         versionValues[i] = gmuVersion.getVersionValue(address);
      }

      versionValues[clusterSnapshot.indexOf(getAddress())] = value;

      GMUDistributedVersion newVersion = new GMUDistributedVersion(cacheName, viewId, this, versionValues);
      if (log.isTraceEnabled()) {
         log.tracef("setNodeVersion(%s, %s) ==> %s", version, value, newVersion);
      }
      return newVersion;
   }

   @Override
   public GMUVersion updatedVersion(EntryVersion entryVersion) {
      if (entryVersion instanceof GMUReplicatedVersion) {
         return new GMUReplicatedVersion(cacheName, currentViewId, this,
                                         ((GMUReplicatedVersion) entryVersion).getThisNodeVersionValue());
      } else if (entryVersion instanceof GMUDistributedVersion) {
         int viewId = currentViewId;
         ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
         long[] newVersions = new long[clusterSnapshot.size()];
         for (int i = 0; i < clusterSnapshot.size(); ++i) {
            newVersions[i] = ((GMUDistributedVersion) entryVersion).getVersionValue(clusterSnapshot.get(i));
         }
         return new GMUDistributedVersion(cacheName, viewId, this, newVersions);
      }
      throw new IllegalArgumentException("Cannot handle " + entryVersion);
   }

   @Override
   public synchronized final ClusterSnapshot getClusterSnapshot(int viewId) {
      if (viewId < viewIdClusterSnapshot.firstKey()) {
         //we don't have this view id anymore
         return null;
      }
      while (!viewIdClusterSnapshot.containsKey(viewId)) {
         try {
            wait();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
      return viewIdClusterSnapshot.get(viewId);
   }

   @Override
   public final Address getAddress() {
      return rpcManager.getAddress();
   }

   @Override
   public synchronized void updateCacheTopology(List<CacheTopology> cacheTopologyList) {
      //this is a little overkill =(
      for (CacheTopology cacheTopology : cacheTopologyList) {
         addCacheTopology(cacheTopology);
      }
   }

   @Override
   public synchronized void addCacheTopology(CacheTopology cacheTopology) {
      int topologyId = cacheTopology.getTopologyId();
      if (viewIdClusterSnapshot.containsKey(topologyId)) {
         //can this happen??
         if (log.isTraceEnabled()) {
            log.tracef("Update members to view Id %s. But it already exists.", topologyId);
         }
         return;
      }

      Collection<Address> addresses = cacheTopology.getMembers();
      if (log.isTraceEnabled()) {
         log.tracef("Update members to view Id %s [current view id is %s]. Members are %s", topologyId, currentViewId, addresses);
      }
      currentViewId = Math.max(topologyId, currentViewId);
      viewIdClusterSnapshot.put(topologyId, new ClusterSnapshot(addresses, HASH));
      notifyAll();
   }

   @Override
   public synchronized void gcTopologyIds(int minTopologyId) {
      viewIdClusterSnapshot.headMap(minTopologyId).clear();
   }

   @Override
   public synchronized int getViewHistorySize() {
      return viewIdClusterSnapshot.size();
   }

   private long[] create(boolean fill, int size) {
      long[] versions = new long[size];
      if (fill) {
         Arrays.fill(versions, NON_EXISTING);
      }
      return versions;
   }

   private long[] increment(GMUVersion initialVersion, int viewId) {
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);

      long[] versions = create(false, clusterSnapshot.size());

      for (int index = 0; index < clusterSnapshot.size(); ++index) {
         versions[index] = initialVersion.getVersionValue(clusterSnapshot.get(index));
      }

      int myIndex = clusterSnapshot.indexOf(getAddress());
      versions[myIndex]++;
      return versions;
   }

   private long[] mergeClustered(int viewId, Collection<GMUVersion> entryVersions) {
      ClusterSnapshot clusterSnapshot = getClusterSnapshot(viewId);
      long[] versions = create(true, clusterSnapshot.size());

      for (GMUVersion entryVersion : entryVersions) {
         for (int index = 0; index < clusterSnapshot.size(); ++index) {
            versions[index] = Math.max(versions[index], entryVersion.getVersionValue(clusterSnapshot.get(index)));
         }
      }
      return versions;
   }

   private long[] calculateVersionToCommit(int newViewId, EntryVersion version, Collection<Address> addresses) {
      GMUVersion gmuVersion = toGMUVersion(version);

      if (addresses == null) {
         int oldViewId = gmuVersion.getViewId();
         ClusterSnapshot oldClusterSnapshot = getClusterSnapshot(oldViewId);
         long commitValue = 0;
         for (int i = 0; i < oldClusterSnapshot.size(); ++i) {
            commitValue = Math.max(commitValue, gmuVersion.getVersionValue(i));
         }

         ClusterSnapshot clusterSnapshot = getClusterSnapshot(newViewId);
         long[] versions = create(true, clusterSnapshot.size());
         for (int i = 0; i < clusterSnapshot.size(); ++i) {
            versions[i] = commitValue;
         }
         return versions;
      }

      ClusterSnapshot clusterSnapshot = getClusterSnapshot(newViewId);
      long[] versions = create(true, clusterSnapshot.size());
      List<Integer> ownersIndex = new LinkedList<Integer>();
      long commitValue = 0;

      for (Address owner : addresses) {
         int index = clusterSnapshot.indexOf(owner);
         if (index < 0) {
            continue;
         }
         commitValue = Math.max(commitValue, gmuVersion.getVersionValue(owner));
         ownersIndex.add(index);
      }

      for (int index = 0; index < clusterSnapshot.size(); ++index) {
         if (ownersIndex.contains(index)) {
            versions[index] = commitValue;
         } else {
            versions[index] = gmuVersion.getVersionValue(clusterSnapshot.get(index));
         }
      }
      return versions;
   }
}
