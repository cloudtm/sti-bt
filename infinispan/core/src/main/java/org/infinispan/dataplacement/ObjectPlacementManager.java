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
package org.infinispan.dataplacement;

import org.infinispan.dataplacement.ch.DataPlacementConsistentHash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Collects all the remote and local access for each member for the key in which this member is the primary owner
 *
 * @author Zhongmiao Li
 * @author João Paiva
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ObjectPlacementManager {

   private static final Log log = LogFactory.getLog(ObjectPlacementManager.class);
   private final BitSet requestReceived;
   private ClusterSnapshot clusterSnapshot;
   private ObjectRequest[] objectRequests;
   //this can be quite big. save it as an array to save some memory
   private Object[] allKeysMoved;
   private ConsistentHash consistentHash;

   public ObjectPlacementManager() {
      requestReceived = new BitSet();
      allKeysMoved = new Object[0];
   }

   /**
    * reset the state (before each round)
    *
    * @param roundClusterSnapshot the current cluster members
    */
   public final synchronized void resetState(ClusterSnapshot roundClusterSnapshot, ConsistentHash consistentHash) {
      clusterSnapshot = roundClusterSnapshot;
      this.consistentHash = consistentHash;
      objectRequests = new ObjectRequest[clusterSnapshot.size()];
      requestReceived.clear();
   }

   /**
    * collects the local and remote accesses for each member
    *
    * @param member        the member that sent the {@code objectRequest}
    * @param objectRequest the local and remote accesses
    * @return true if all requests are received, false otherwise. It only returns true on the first time it has all the
    *         objects
    */
   public final synchronized boolean aggregateRequest(Address member, ObjectRequest objectRequest) {
      if (hasReceivedAllRequests()) {
         return false;
      }

      int senderIdx = clusterSnapshot.indexOf(member);

      if (senderIdx < 0) {
         log.warnf("Received request list from %s but it does not exits in %s", member, clusterSnapshot);
         return false;
      }

      objectRequests[senderIdx] = objectRequest;
      requestReceived.set(senderIdx);

      logRequestReceived(member, objectRequest);

      return hasReceivedAllRequests();
   }

   /**
    * calculate the new owners based on the requests received.
    *
    * @return a map with the keys to be moved and the new owners
    */
   public final synchronized Collection<SegmentMapping> calculateObjectsToMove() {
      Map<Object, OwnersInfo> newOwnersMap = new HashMap<Object, OwnersInfo>();

      for (int requesterIdx = 0; requesterIdx < clusterSnapshot.size(); ++requesterIdx) {
         ObjectRequest objectRequest = objectRequests[requesterIdx];

         if (objectRequest == null) {
            continue;
         }

         Map<Object, Long> requestedObjects = objectRequest.getRemoteAccesses();

         for (Map.Entry<Object, Long> entry : requestedObjects.entrySet()) {
            calculateNewOwners(newOwnersMap, entry.getKey(), entry.getValue(), requesterIdx);
         }
         //release memory asap
         requestedObjects.clear();
      }

      removeNotMovedObjects(newOwnersMap);

      if (log.isDebugEnabled()) {
         log.debugf("This round final Owners per key are %s", newOwnersMap);
      }

      //process the old moved keys. this will set the new owners of the previous rounds
      for (Object key : allKeysMoved) {
         if (!newOwnersMap.containsKey(key)) {
            newOwnersMap.put(key, createOwnersInfo(key));
         }
      }

      if (log.isDebugEnabled()) {
         log.debugf("Final Owners per key are %s", newOwnersMap);
      }

      //update all the keys moved array
      allKeysMoved = newOwnersMap.keySet().toArray(new Object[newOwnersMap.size()]);

      Map<Integer, SegmentMapping> segmentMappingMap = new HashMap<Integer, SegmentMapping>(consistentHash.getNumSegments());

      for (Map.Entry<Object, OwnersInfo> entry : newOwnersMap.entrySet()) {
         int segmentId = consistentHash.getSegment(entry.getKey());
         SegmentMapping segmentMapping = segmentMappingMap.get(segmentId);
         if (segmentMapping == null) {
            segmentMapping = new SegmentMapping(segmentId);
            segmentMappingMap.put(segmentId, segmentMapping);
         }
         segmentMapping.add(entry.getKey(), entry.getValue());
      }

      if (log.isDebugEnabled()) {
         log.debugf("Final Owners per segment are %s", segmentMappingMap);
      }

      return segmentMappingMap.values();
   }

   /**
    * returns all keys moved so far
    *
    * @return all keys moved so far
    */
   public final Collection<Object> getKeysToMove() {
      return Arrays.asList(allKeysMoved);
   }

   public final int getNumberOfOwners() {
      return consistentHash.getNumOwners();
   }

   /**
    * for each object to move, it checks if the owners are different from the owners returned by the original
    * Infinispan's consistent hash. If this is true, the object is removed from the map {@code newOwnersMap}
    *
    * @param newOwnersMap the map with the key to be moved and the new owners
    */
   private void removeNotMovedObjects(Map<Object, OwnersInfo> newOwnersMap) {
      ConsistentHash defaultConsistentHash = getDefaultConsistentHash();
      Iterator<Map.Entry<Object, OwnersInfo>> iterator = newOwnersMap.entrySet().iterator();

      //if the owners info corresponds to the default consistent hash owners, remove the key from the map 
      mainLoop:
      while (iterator.hasNext()) {
         Map.Entry<Object, OwnersInfo> entry = iterator.next();
         Object key = entry.getKey();
         OwnersInfo ownersInfo = entry.getValue();
         Collection<Integer> ownerInfoIndexes = ownersInfo.getNewOwnersIndexes();
         Collection<Address> defaultOwners = defaultConsistentHash.locateOwners(key);

         if (ownerInfoIndexes.size() != defaultOwners.size()) {
            continue;
         }

         for (Address address : defaultOwners) {
            if (!ownerInfoIndexes.contains(clusterSnapshot.indexOf(address))) {
               continue mainLoop;
            }
         }
         iterator.remove();
      }
   }

   /**
    * updates the owner information for the {@code key} based in the {@code numberOfRequests} made by the member who
    * requested this {@code key} (identified by {@code requesterId})
    *
    * @param newOwnersMap     the new owners map to be updated
    * @param key              the key requested
    * @param numberOfRequests the number of accesses made to this key
    * @param requesterId      the member id
    */
   private void calculateNewOwners(Map<Object, OwnersInfo> newOwnersMap, Object key, long numberOfRequests, int requesterId) {
      OwnersInfo newOwnersInfo = newOwnersMap.get(key);
      if (log.isTraceEnabled()) {
         log.tracef("Analyzing key=%s, numberOfRequest=%s, requesterId=%s, OwnerInfo=%s", key, numberOfRequests,
                    requesterId, newOwnersInfo);
      }

      if (newOwnersInfo == null) {
         newOwnersInfo = createOwnersInfo(key);
         newOwnersMap.put(key, newOwnersInfo);
         if (log.isTraceEnabled()) {
            log.tracef("Creating new OwnerInfo for key [%s]: %s", key, newOwnersInfo);
         }
      }
      newOwnersInfo.calculateNewOwner(requesterId, numberOfRequests);
      if (log.isTraceEnabled()) {
         log.tracef("Result of analyzing key=%s, numberOfRequest=%s, requesterId=%s, OwnerInfo=%s", key, numberOfRequests,
                    requesterId, newOwnersInfo);
      }
   }

   /**
    * returns the local accesses and owners for the {@code key}
    *
    * @param key the key
    * @return the local accesses and owners for the key
    */
   private Map<Integer, Long> getLocalAccesses(Object key) {
      Map<Integer, Long> localAccessesMap = new TreeMap<Integer, Long>();

      for (int memberIndex = 0; memberIndex < objectRequests.length; ++memberIndex) {
         ObjectRequest request = objectRequests[memberIndex];
         if (request == null) {
            continue;
         }
         Long localAccesses = request.getLocalAccesses().remove(key);
         if (localAccesses != null) {
            localAccessesMap.put(memberIndex, localAccesses);
         }
      }

      return localAccessesMap;
   }

   /**
    * creates a new owners information initialized with the current owners returned by the current consistent hash and
    * their number of accesses for the {@code key}
    *
    * @param key the key
    * @return the new owners information.
    */
   private OwnersInfo createOwnersInfo(Object key) {
      Collection<Address> replicas = consistentHash.locateOwners(key);
      Map<Integer, Long> localAccesses = getLocalAccesses(key);

      OwnersInfo ownersInfo = new OwnersInfo(replicas.size());

      for (Address currentOwner : replicas) {
         int ownerIndex = clusterSnapshot.indexOf(currentOwner);

         if (ownerIndex == -1) {
            ownerIndex = findNewOwner(key, replicas);
         }

         Long accesses = localAccesses.remove(ownerIndex);

         if (accesses == null) {
            //TODO check if this should be zero or the min number of local accesses from the member
            accesses = 0L;
         }

         ownersInfo.add(ownerIndex, accesses);
      }

      return ownersInfo;
   }

   /**
    * finds the new owner for the {@code key} based on the Infinispan's consistent hash. this is invoked when the one or
    * more current owners are not in the cluster anymore and it is necessary to find new owners to respect the default
    * number of owners per key
    *
    * @param key          the key
    * @param alreadyOwner the current owners
    * @return the new owner index
    */
   private int findNewOwner(Object key, Collection<Address> alreadyOwner) {
      int size = clusterSnapshot.size();

      if (size <= 1) {
         return 0;
      }

      int startIndex = consistentHash.getHashFunction().hash(key) % size;

      for (int index = startIndex + 1; index != startIndex; index = (index + 1) % size) {
         if (!alreadyOwner.contains(clusterSnapshot.get(index))) {
            return index;
         }
         index = (index + 1) % size;
      }

      return 0;
   }

   /**
    * returns the actual consistent hashing
    *
    * @return the actual consistent hashing
    */
   private ConsistentHash getDefaultConsistentHash() {
      return consistentHash instanceof DataPlacementConsistentHash ?
            ((DataPlacementConsistentHash) consistentHash).getConsistentHash() :
            consistentHash;
   }

   private boolean hasReceivedAllRequests() {
      return requestReceived.cardinality() == clusterSnapshot.size();
   }

   private void logRequestReceived(Address sender, ObjectRequest request) {
      if (log.isTraceEnabled()) {
         StringBuilder missingMembers = new StringBuilder();

         for (int i = 0; i < clusterSnapshot.size(); ++i) {
            if (!requestReceived.get(i)) {
               missingMembers.append(clusterSnapshot.get(i)).append(" ");
            }
         }

         log.debugf("Object Request received from %s. Missing request are %s. The Object Request is %s", sender,
                    missingMembers, request.toString(true));
      } else if (log.isDebugEnabled()) {
         log.debugf("Object Request received from %s. Missing request are %s. The Object Request is %s", sender,
                    (clusterSnapshot.size() - requestReceived.cardinality()), request.toString());
      }
   }

}

