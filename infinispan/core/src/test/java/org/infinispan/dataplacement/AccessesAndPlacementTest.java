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

import junit.framework.Assert;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.dataplacement.AccessesManager.*;

/**
 * Test the functionality of the Remote Accesses Manager and the Object placement manager
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "dataplacement.AccessesAndPlacementTest")
public class AccessesAndPlacementTest {

   private static final Log log = LogFactory.getLog(AccessesAndPlacementTest.class);
   private static final Hash HASH = new MurmurHash3();
   private static final AtomicInteger KEY_NEXT_ID = new AtomicInteger(0);
   private static final DefaultConsistentHashFactory DEFAULT_CONSISTENT_HASH_FACTORY = new DefaultConsistentHashFactory();
   private static final int NUMBER_OF_SEGMENTS = 1000;

   public void testNoMovement() {
      ClusterSnapshot clusterSnapshot = createClusterSnapshot(4);
      ConsistentHash consistentHash = createDefaultConsistentHash(clusterSnapshot.getMembers(), 1);
      ObjectPlacementManager manager = createObjectPlacementManager();
      manager.resetState(clusterSnapshot, consistentHash);
      Collection<SegmentMapping> newOwners = manager.calculateObjectsToMove();
      Assert.assertTrue(newOwners.isEmpty());
   }

   @SuppressWarnings("AssertWithSideEffects")
   public void testReturnValue() {
      ClusterSnapshot clusterSnapshot = createClusterSnapshot(2);
      ConsistentHash consistentHash = createDefaultConsistentHash(clusterSnapshot.getMembers(), 1);
      ObjectPlacementManager manager = createObjectPlacementManager();
      manager.resetState(clusterSnapshot, consistentHash);

      Assert.assertFalse(manager.aggregateRequest(clusterSnapshot.get(0), new ObjectRequest(null, null)));
      Assert.assertTrue(manager.aggregateRequest(clusterSnapshot.get(1), new ObjectRequest(null, null)));
      Assert.assertFalse(manager.aggregateRequest(clusterSnapshot.get(0), new ObjectRequest(null, null)));
      Assert.assertFalse(manager.aggregateRequest(clusterSnapshot.get(1), new ObjectRequest(null, null)));
   }

   public void testSwapOwners() {
      ClusterSnapshot clusterSnapshot = createClusterSnapshot(4);
      ConsistentHash consistentHash = createDefaultConsistentHash(clusterSnapshot.getMembers(), 2);
      ObjectPlacementManager manager = createObjectPlacementManager();
      manager.resetState(clusterSnapshot, consistentHash);

      List<Address> key1Owners = new ArrayList<Address>(2);
      List<Address> key2Owners = new ArrayList<Address>(2);
      Object key1 = createKey(consistentHash, null, key1Owners);
      Object key2 = createKey(consistentHash, key1Owners, key2Owners);

      int[] key1OwnersIndexes = new int[key1Owners.size()];
      int i = 0;
      for (Address address : key1Owners) {
         key1OwnersIndexes[i++] = clusterSnapshot.indexOf(address);
      }

      int[] key2OwnersIndexes = new int[key1Owners.size()];
      i = 0;
      for (Address address : key2Owners) {
         key2OwnersIndexes[i++] = clusterSnapshot.indexOf(address);
      }

      log.debugf("%s: Owners: %s, owners indexes: %s", key1, key1Owners, Arrays.toString(key1OwnersIndexes));
      log.debugf("%s: Owners: %s, owners indexes: %s", key2, key2Owners, Arrays.toString(key2OwnersIndexes));

      for (int index : key2OwnersIndexes) {
         Map<Object, Long> request = new HashMap<Object, Long>();
         request.put(key1, 1L);
         manager.aggregateRequest(clusterSnapshot.get(index), new ObjectRequest(request, null));
      }

      for (int index : key1OwnersIndexes) {
         Map<Object, Long> request = new HashMap<Object, Long>();
         request.put(key2, 1L);
         manager.aggregateRequest(clusterSnapshot.get(index), new ObjectRequest(request, null));
      }

      Collection<SegmentMapping> newOwners = manager.calculateObjectsToMove();
      log.debug("Swap owners final segment mapping: " + newOwners);

      Assert.assertEquals(2, newOwners.size());

      for (SegmentMapping segmentMapping : newOwners) {
         Iterator<SegmentMapping.KeyOwners> iterator = segmentMapping.iterator();
         while (iterator.hasNext()) {
            SegmentMapping.KeyOwners keyOwners = iterator.next();
            if (keyOwners.getKey().equals(key1)) {
               assertOwner(keyOwners, key2OwnersIndexes);
            } else if (keyOwners.getKey().equals(key2)) {
               assertOwner(keyOwners, key1OwnersIndexes);
            }

         }
      }
   }

   public void testOneOwnerChange() {
      ClusterSnapshot clusterSnapshot = createClusterSnapshot(4);
      ConsistentHash consistentHash = createDefaultConsistentHash(clusterSnapshot.getMembers(), 2);
      ObjectPlacementManager manager = createObjectPlacementManager();
      manager.resetState(clusterSnapshot, consistentHash);

      Map<Object, Long> remote = new HashMap<Object, Long>();
      Map<Object, Long> local = new HashMap<Object, Long>();

      List<Address> key1Owners = new ArrayList<Address>(2);
      List<Address> key2Owners = new ArrayList<Address>(2);
      Object key1 = createKey(consistentHash, null, key1Owners);
      Object key2 = createKey(consistentHash, key1Owners, key2Owners);

      int[] key1OwnersIndexes = new int[key1Owners.size()];
      int i = 0;
      for (Address address : key1Owners) {
         key1OwnersIndexes[i++] = clusterSnapshot.indexOf(address);
      }

      int[] key2OwnersIndexes = new int[key1Owners.size()];
      i = 0;
      for (Address address : key2Owners) {
         key2OwnersIndexes[i++] = clusterSnapshot.indexOf(address);
      }

      log.debugf("%s: Owners: %s, owners indexes: %s", key1, key1Owners, Arrays.toString(key1OwnersIndexes));
      log.debugf("%s: Owners: %s, owners indexes: %s", key2, key2Owners, Arrays.toString(key2OwnersIndexes));

      remote.put(key2, 1L);
      local.put(key1, 4L);
      manager.aggregateRequest(key1Owners.get(0), new ObjectRequest(remote, local));

      remote = new HashMap<Object, Long>();
      local = new HashMap<Object, Long>();
      remote.put(key2, 3L);
      local.put(key1, 1L);
      manager.aggregateRequest(key1Owners.get(1), new ObjectRequest(remote, local));

      remote = new HashMap<Object, Long>();
      local = new HashMap<Object, Long>();
      remote.put(key1, 2L);
      local.put(key2, 4L);
      manager.aggregateRequest(key2Owners.get(0), new ObjectRequest(remote, local));

      remote = new HashMap<Object, Long>();
      local = new HashMap<Object, Long>();
      remote.put(key1, 3L);
      local.put(key2, 2L);
      manager.aggregateRequest(key2Owners.get(1), new ObjectRequest(remote, local));

      Collection<SegmentMapping> newOwners = manager.calculateObjectsToMove();

      log.debug("One Owner change final segment mapping: " + newOwners);
      Assert.assertEquals(2, newOwners.size());

      for (SegmentMapping segmentMapping : newOwners) {
         Iterator<SegmentMapping.KeyOwners> iterator = segmentMapping.iterator();
         while (iterator.hasNext()) {
            SegmentMapping.KeyOwners keyOwners = iterator.next();
            if (keyOwners.getKey().equals(key1)) {
               assertOwner(keyOwners, key1OwnersIndexes[0], key2OwnersIndexes[1]);
            } else if (keyOwners.getKey().equals(key2)) {
               assertOwner(keyOwners, key1OwnersIndexes[1], key2OwnersIndexes[0]);
            }

         }
      }
   }

   public void testNotEnoughRemoteAccesses() {
      ClusterSnapshot clusterSnapshot = createClusterSnapshot(4);
      ConsistentHash consistentHash = createDefaultConsistentHash(clusterSnapshot.getMembers(), 2);
      ObjectPlacementManager manager = createObjectPlacementManager();
      manager.resetState(clusterSnapshot, consistentHash);

      List<Address> keyOwners = new ArrayList<Address>(2);
      Object key = createKey(consistentHash, null, keyOwners);

      int[] keyOwnersIndexes = new int[keyOwners.size()];
      int i = 0;
      for (Address address : keyOwners) {
         keyOwnersIndexes[i++] = clusterSnapshot.indexOf(address);
      }

      int[] nonOwners = new int[clusterSnapshot.size() - keyOwners.size()];
      i = 0;
      for (int j = 0; j < clusterSnapshot.size(); ++j) {
         if (!keyOwners.contains(clusterSnapshot.get(j))) {
            nonOwners[i++] = j;
         }

      }

      log.debugf("%s: Owners: %s, owners indexes: %s, nonOwners indexes: %s", key, keyOwners,
                 Arrays.toString(keyOwnersIndexes), Arrays.toString(nonOwners));

      for (int index : keyOwnersIndexes) {
         Map<Object, Long> request = new HashMap<Object, Long>();
         request.put(key, 10L);
         manager.aggregateRequest(clusterSnapshot.get(index), new ObjectRequest(null, request));
      }

      for (int index : nonOwners) {
         Map<Object, Long> request = new HashMap<Object, Long>();
         request.put(key, 5L);
         manager.aggregateRequest(clusterSnapshot.get(index), new ObjectRequest(request, null));
      }

      Collection<SegmentMapping> newOwners = manager.calculateObjectsToMove();

      log.debug("Not enough remote accesses final segment mapping: " + newOwners);

      Assert.assertTrue(newOwners.isEmpty());
   }

   public void testRemoteAccesses() {
      ClusterSnapshot clusterSnapshot = createClusterSnapshot(4);
      ConsistentHash consistentHash = createDefaultConsistentHash(clusterSnapshot.getMembers(), 2);
      AccessesManager manager = createRemoteAccessManager();
      manager.resetState(clusterSnapshot, consistentHash);
      StreamLibContainer container = new StreamLibContainer("dummy", "dummy");
      container.setCapacity(2);
      container.setActive(true);
      manager.setStreamLibContainer(container);

      Object key1 = createKey(consistentHash, clusterSnapshot.get(0));
      Object key2 = createKey(consistentHash, clusterSnapshot.get(1));
      Object key3 = createKey(consistentHash, clusterSnapshot.get(2));
      Object key4 = createKey(consistentHash, clusterSnapshot.get(3));

      log.debugf("%s: owners: %s, primary owner: %s", key1, consistentHash.locateOwners(key1), consistentHash.locatePrimaryOwner(key1));
      log.debugf("%s: owners: %s, primary owner: %s", key2, consistentHash.locateOwners(key2), consistentHash.locatePrimaryOwner(key2));
      log.debugf("%s: owners: %s, primary owner: %s", key3, consistentHash.locateOwners(key3), consistentHash.locatePrimaryOwner(key3));
      log.debugf("%s: owners: %s, primary owner: %s", key4, consistentHash.locateOwners(key4), consistentHash.locatePrimaryOwner(key4));

      addKey(key1, !consistentHash.isKeyLocalToNode(clusterSnapshot.get(0), key1), 10, container);
      addKey(key2, !consistentHash.isKeyLocalToNode(clusterSnapshot.get(0), key2), 5, container);
      addKey(key3, !consistentHash.isKeyLocalToNode(clusterSnapshot.get(0), key3), 15, container);
      addKey(key4, !consistentHash.isKeyLocalToNode(clusterSnapshot.get(0), key4), 2, container);

      manager.calculateAccesses();

      Map<Object, Long> remote = new HashMap<Object, Long>();
      Map<Object, Long> local = new HashMap<Object, Long>();

      (consistentHash.isKeyLocalToNode(clusterSnapshot.get(0), key1) ? local : remote).put(key1, 10L);
      assertAccesses(manager.getObjectRequestForAddress(clusterSnapshot.get(0)), remote, local);
      remote.clear();
      local.clear();

      (consistentHash.isKeyLocalToNode(clusterSnapshot.get(0), key2) ? local : remote).put(key2, 5L);
      assertAccesses(manager.getObjectRequestForAddress(clusterSnapshot.get(1)), remote, local);
      remote.clear();
      local.clear();

      (consistentHash.isKeyLocalToNode(clusterSnapshot.get(0), key3) ? local : remote).put(key3, 15L);
      assertAccesses(manager.getObjectRequestForAddress(clusterSnapshot.get(2)), remote, local);
      remote.clear();
      local.clear();

      (consistentHash.isKeyLocalToNode(clusterSnapshot.get(0), key4) ? local : remote).put(key4, 2L);
      assertAccesses(manager.getObjectRequestForAddress(clusterSnapshot.get(3)), remote, local);
      remote.clear();
      local.clear();
   }

   public void testRemoteTopKeyRequest() {
      RemoteTopKeyRequest request = new RemoteTopKeyRequest(10);

      TestKey key1 = createRandomKey();
      TestKey key2 = createRandomKey();
      TestKey key3 = createRandomKey();
      TestKey key4 = createRandomKey();

      Map<Object, Long> counter = new HashMap<Object, Long>();
      counter.put(key1, 1L);
      counter.put(key2, 1L);
      counter.put(key3, 1L);
      counter.put(key4, 1L);

      request.merge(counter, 1, null);

      assertKeyAccess(key1, request, 1);
      assertKeyAccess(key2, request, 1);
      assertKeyAccess(key3, request, 1);
      assertKeyAccess(key4, request, 1);

      counter.put(key1, 1L);
      counter.put(key2, 2L);
      counter.put(key3, 3L);
      counter.put(key4, 4L);

      request.merge(counter, 2, null);

      assertKeyAccess(key1, request, 3);
      assertKeyAccess(key2, request, 5);
      assertKeyAccess(key3, request, 7);
      assertKeyAccess(key4, request, 9);

      assertSortedKeyAccess(key1, request, 3, 3);
      assertSortedKeyAccess(key2, request, 5, 2);
      assertSortedKeyAccess(key3, request, 7, 1);
      assertSortedKeyAccess(key4, request, 9, 0);

      assertSortedKeyAccess(request);

      counter.clear();

      TestKey[] keys = new TestKey[11];

      for (int i = 0; i < keys.length; ++i) {
         TestKey key = createRandomKey();
         keys[i] = key;
         counter.put(key, (long) i);
      }

      request.merge(counter, 1, null);

      assertKeyAccess(key1, request, 3);
      assertKeyAccess(key2, request, 5);
      assertKeyAccess(key3, request, 7);
      assertKeyAccess(key4, request, 9);

      for (int i = 1; i < keys.length; ++i) {
         assertKeyAccess(keys[i], request, i);
      }

      int idx = 0;
      assertSortedKeyAccess(keys[10], request, 10, idx++);
      assertSortedKeyAccess(keys[9], request, 9, idx++);
      assertSortedKeyAccess(key4, request, 9, idx++);
      assertSortedKeyAccess(keys[8], request, 8, idx++);
      assertSortedKeyAccess(keys[7], request, 7, idx++);
      assertSortedKeyAccess(key3, request, 7, idx++);
      assertSortedKeyAccess(keys[6], request, 6, idx++);
      assertSortedKeyAccess(keys[5], request, 5, idx++);
      assertSortedKeyAccess(key2, request, 5, idx++);
      assertSortedKeyAccess(keys[4], request, 4, idx++);
      assertSortedKeyAccess(keys[3], request, 3, idx++);
      assertSortedKeyAccess(key1, request, 3, idx++);
      assertSortedKeyAccess(keys[2], request, 2, idx++);
      assertSortedKeyAccess(keys[1], request, 1, idx);

      assertSortedKeyAccess(request);

      assertNoKeyAccess(keys[0], request);
   }

   public void testLocalTopKeyRequest() {
      LocalTopKeyRequest request = new LocalTopKeyRequest();

      TestKey key1 = createRandomKey();
      TestKey key2 = createRandomKey();
      TestKey key3 = createRandomKey();
      TestKey key4 = createRandomKey();

      Map<Object, Long> counter = new HashMap<Object, Long>();
      counter.put(key1, 1L);
      counter.put(key2, 1L);
      counter.put(key3, 1L);
      counter.put(key4, 1L);

      request.merge(counter, 1);

      assertKeyAccess(key1, request, 1);
      assertKeyAccess(key2, request, 1);
      assertKeyAccess(key3, request, 1);
      assertKeyAccess(key4, request, 1);

      counter.put(key1, 1L);
      counter.put(key2, 2L);
      counter.put(key3, 3L);
      counter.put(key4, 4L);

      request.merge(counter, 2);

      assertKeyAccess(key1, request, 3);
      assertKeyAccess(key2, request, 5);
      assertKeyAccess(key3, request, 7);
      assertKeyAccess(key4, request, 9);

      counter.clear();

      TestKey[] keys = new TestKey[11];

      for (int i = 0; i < keys.length; ++i) {
         TestKey key = createRandomKey();
         keys[i] = key;
         counter.put(key, (long) i);
      }

      request.merge(counter, 1);

      assertKeyAccess(key1, request, 3);
      assertKeyAccess(key2, request, 5);
      assertKeyAccess(key3, request, 7);
      assertKeyAccess(key4, request, 9);

      for (int i = 1; i < keys.length; ++i) {
         assertKeyAccess(keys[i], request, i);
      }

      assertNoKeyAccess(keys[0], request);
   }

   private void assertNoKeyAccess(Object key, RemoteTopKeyRequest request) {
      assert !request.contains(key) : "Key " + key + " has found in map";
      for (KeyAccess keyAccess : request.getSortedKeyAccess()) {
         assert !keyAccess.getKey().equals(key) : "Key " + key + " has found in list";
      }
   }

   private void assertNoKeyAccess(Object key, LocalTopKeyRequest request) {
      assert !request.contains(key) : "Key " + key + " has found";
   }

   private void assertKeyAccess(Object key, RemoteTopKeyRequest request, long accesses) {
      assert request.contains(key) : "Key " + key + " not found";
      KeyAccess keyAccess = request.get(key);
      assert keyAccess.getAccesses() == accesses : "Number of accesses: " + keyAccess.getAccesses() + " != " + accesses;
      assert keyAccess.getKey().equals(key) : "Key is different: " + keyAccess.getKey() + " != " + key;
   }

   private void assertKeyAccess(Object key, LocalTopKeyRequest request, long accesses) {
      assert request.contains(key) : "Key " + key + " not found";
      KeyAccess keyAccess = request.get(key);
      assert keyAccess.getAccesses() == accesses : "Number of accesses: " + keyAccess.getAccesses() + " != " + accesses;
      assert keyAccess.getKey().equals(key) : "Key is different: " + keyAccess.getKey() + " != " + key;
   }

   private void assertSortedKeyAccess(Object key, RemoteTopKeyRequest request, long accesses, int pos) {
      List<KeyAccess> keyAccessList = request.getSortedKeyAccess();
      assert keyAccessList.size() > pos : "Size (" + keyAccessList.size() + ") is smaller than " + pos;
      assert keyAccessList.get(pos).getKey().equals(key) : "Key is different [" + pos + "]: " +
            keyAccessList.get(pos).getKey() + " != " + key;
      assert keyAccessList.get(pos).getAccesses() == accesses : "Number of accesses [" + pos + "]: " +
            keyAccessList.get(pos).getAccesses() + " != " + accesses;
   }

   private void assertSortedKeyAccess(RemoteTopKeyRequest request) {
      List<KeyAccess> keyAccessList = request.getSortedKeyAccess();
      if (keyAccessList.isEmpty()) {
         return;
      }

      long maxValue = keyAccessList.get(0).getAccesses();

      for (KeyAccess keyAccess : keyAccessList) {
         assert keyAccess.getAccesses() <= maxValue : "Different order: " + keyAccess.getAccesses() + " > " + maxValue;
         maxValue = keyAccess.getAccesses();
      }
   }

   private TestKey createRandomKey() {
      return new TestKey("KEY_" + KEY_NEXT_ID.incrementAndGet());
   }

   private void assertAccesses(ObjectRequest request, Map<Object, Long> remote, Map<Object, Long> local) {
      Map<Object, Long> remoteAccesses = request.getRemoteAccesses();
      Map<Object, Long> localAccesses = request.getLocalAccesses();

      assert remoteAccesses.size() == remote.size();
      assert localAccesses.size() == local.size();

      for (Map.Entry<Object, Long> entry : remote.entrySet()) {
         long value1 = entry.getValue();
         long value2 = remoteAccesses.get(entry.getKey());

         assert value1 == value2;
      }

      for (Map.Entry<Object, Long> entry : local.entrySet()) {
         long value1 = entry.getValue();
         long value2 = localAccesses.get(entry.getKey());

         assert value1 == value2;
      }
   }

   private void addKey(Object key, boolean remote, int count, StreamLibContainer container) {
      log.debugf("Add %s accesses for key %s [is remote? %s]", count, key, remote);
      for (int i = 0; i < count; ++i) {
         container.addGet(key, remote);
      }
   }

   private void assertOwner(SegmentMapping.KeyOwners keyOwners, int... expected) {
      Assert.assertNotNull(keyOwners);
      int[] actual = keyOwners.getOwnerIndexes();
      Assert.assertEquals(expected.length, actual.length);

      Arrays.sort(actual);
      Arrays.sort(expected);

      for (int i = 0; i < expected.length; ++i) {
         Assert.assertEquals(expected[i], actual[i]);
      }
   }

   private ClusterSnapshot createClusterSnapshot(int size) {
      List<Address> members = new ArrayList<Address>(size);
      for (int i = 0; i < size; ++i) {
         members.add(new TestAddress(i));
      }
      return new ClusterSnapshot(members.toArray(new Address[size]), HASH);
   }

   private ObjectPlacementManager createObjectPlacementManager() {
      return new ObjectPlacementManager();
   }

   private AccessesManager createRemoteAccessManager() {
      AccessesManager manager = new AccessesManager();
      manager.setMaxNumberOfKeysToRequest(100);
      return manager;
   }

   private ConsistentHash createDefaultConsistentHash(List<Address> members, int numberOfOwners) {
      return DEFAULT_CONSISTENT_HASH_FACTORY.create(HASH, numberOfOwners, NUMBER_OF_SEGMENTS, members);
   }

   private Object createKey(ConsistentHash consistentHash, Collection<Address> differentFrom, List<Address> owners) {
      TestKey key;
      while (true) {
         key = new TestKey("KEY_" + KEY_NEXT_ID.incrementAndGet());
         if (differentFrom == null || differentFrom.isEmpty()) {
            owners.addAll(consistentHash.locateOwners(key));
            return key;
         }
         List<Address> copy = new LinkedList<Address>(differentFrom);
         copy.removeAll(consistentHash.locateOwners(key));
         if (copy.equals(differentFrom)) {
            owners.addAll(consistentHash.locateOwners(key));
            return key;
         }
      }
   }

   private Object createKey(ConsistentHash consistentHash, Address primaryOwner) {
      TestKey key;
      while (true) {
         key = new TestKey("KEY_" + KEY_NEXT_ID.incrementAndGet());
         if (primaryOwner == null) {
            return key;
         }
         if (consistentHash.locatePrimaryOwner(key).equals(primaryOwner)) {
            return key;
         }
      }
   }

   private class TestKey {

      private final String name;

      private TestKey(String name) {
         this.name = name;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         TestKey testKey = (TestKey) o;

         return !(name != null ? !name.equals(testKey.name) : testKey.name != null);

      }

      @Override
      public int hashCode() {
         return name != null ? name.hashCode() : 0;
      }

      @Override
      public String toString() {
         return "TestKey{" +
               "name='" + name + '\'' +
               '}';
      }
   }
}
