/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.transaction.totalorder;

import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.TotalOrderRemoteTransactionState;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class behaves as a synchronization point between incoming transactions (totally ordered) and between incoming
 * transactions and state transfer.
 * <p/>
 * Main functions: <ul> <li> ensure an order between prepares before sending them to the thread pool, i.e.
 * non-conflicting prepares can be processed concurrently; </li> <li> ensure that the state transfer waits for the
 * previous delivered prepares; </li> <li> ensure that the prepare waits for state transfer in progress. </li> </ul>
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class GMUTotalOrderManager implements TotalOrderManager {

   private static final Log log = LogFactory.getLog(GMUTotalOrderManager.class);
   /**
    * this map is used to keep track of concurrent transactions.
    */
   private final ConcurrentMap<Object, TotalOrderLock> keysLocked;
   private final AtomicReference<TotalOrderLatch> clear;
   private final AtomicReference<TotalOrderLatch> stateTransferInProgress;
   private BlockingTaskAwareExecutorService totalOrderExecutor;

   public GMUTotalOrderManager() {
      keysLocked = ConcurrentMapFactory.makeConcurrentMap();
      clear = new AtomicReference<TotalOrderLatch>(null);
      stateTransferInProgress = new AtomicReference<TotalOrderLatch>(null);
   }

   @Inject
   public void inject(@ComponentName(KnownComponentNames.TOTAL_ORDER_EXECUTOR) BlockingTaskAwareExecutorService totalOrderExecutor) {
      this.totalOrderExecutor = totalOrderExecutor;
   }

   /**
    * It ensures the validation order for the transaction corresponding to the prepare command. This allow the prepare
    * command to be moved to a thread pool.
    *
    * @param state the total order prepare state
    */
   @Override
   public final void ensureOrder(TotalOrderRemoteTransactionState state, Object[][] keyWriteAndRead) throws InterruptedException {
      //the retries due to state transfer re-uses the same state. we need that the keys previous locked to be release
      //in order to insert it again in the keys locked.
      //NOTE: this method does not need to be synchronized because it is invoked by a one thread at the time, namely
      //the thread that is delivering the messages in total order.
      state.awaitUntilReset();
      TotalOrderLatch transactionSynchronizedBlock = new TotalOrderLatchImpl(state.getGlobalTransaction().globalId());
      state.setTransactionSynchronizedBlock(transactionSynchronizedBlock);
      if (keyWriteAndRead == null) { //clear state
         TotalOrderLatch oldClear = clear.get();
         if (oldClear != null) {
            state.addSynchronizedBlock(oldClear);
            clear.set(transactionSynchronizedBlock);
         }
         //add all other "locks"
         for (TotalOrderLock lock : keysLocked.values()) {
            state.addAllSynchronizedBlocks(lock.acquireForClear());
         }
         keysLocked.clear();
         state.addKeysLockedForClear();
      } else {
         TotalOrderLatch clearTx = clear.get();
         if (clearTx != null) {
            state.addSynchronizedBlock(clearTx);
         }
         Set<Object> acquiredLocks = new HashSet<Object>();
         Object[] writeSet = keyWriteAndRead[0];
         Object[] readSet = keyWriteAndRead[1];
         //this will collect all the count down latch corresponding to the previous transactions in the queue
         for (Object key : writeSet) {
            if (acquiredLocks.contains(key)) {
               //safety... should never happen
               continue;
            }
            acquireLock(state, transactionSynchronizedBlock, key, true);
            acquiredLocks.add(key);
         }
         for (Object key : readSet) {
            if (acquiredLocks.contains(key)) {
               //safety... should never happen
               continue;
            }
            acquireLock(state, transactionSynchronizedBlock, key, false);
            acquiredLocks.add(key);
         }
         //no longer needed
         acquiredLocks.clear();
      }

      TotalOrderLatch stateTransfer = stateTransferInProgress.get();
      if (stateTransfer != null) {
         state.addSynchronizedBlock(stateTransfer);
      }

      if (log.isTraceEnabled()) {
         log.tracef("Transaction [%s] will wait for %s and locked %s", state.getGlobalTransaction().globalId(),
                    state.getConflictingTransactionBlocks(), state.getLockedKeys() == null ? "[ClearCommand]" :
               state.getLockedKeys());
      }
   }

   /**
    * Release the locked key possibly unblock waiting prepares.
    *
    * @param state the state
    */
   @Override
   public final void release(TotalOrderRemoteTransactionState state) {
      TotalOrderLatch synchronizedBlock = state.getTransactionSynchronizedBlock();
      if (synchronizedBlock == null) {
         //already released!
         return;
      }
      Collection<Object> lockedKeys = state.getLockedKeys();
      synchronizedBlock.unBlock();
      if (lockedKeys == null) {
         clear.compareAndSet(synchronizedBlock, null);
      } else {
         for (Object key : lockedKeys) {
            TotalOrderLock lock = keysLocked.get(key);
            if (lock.releaseLock(synchronizedBlock)) {
               keysLocked.remove(key, lock);
            }
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Release %s and locked keys %s. Checking pending tasks!", synchronizedBlock,
                    lockedKeys == null ? "[ClearCommand]" : lockedKeys);
      }
      state.reset();
      totalOrderExecutor.checkForReadyTasks();
   }

   /**
    * It notifies that a state transfer is about to start.
    *
    * @param topologyId the new topology ID
    * @return the current pending prepares
    */
   @Override
   public final Collection<TotalOrderLatch> notifyStateTransferStart(int topologyId) {
      List<TotalOrderLatch> preparingTransactions = new ArrayList<TotalOrderLatch>(keysLocked.size());
      for (TotalOrderLock lock : keysLocked.values()) {
         preparingTransactions.addAll(lock.stateTransfer());
      }
      TotalOrderLatch clearBlock = clear.get();
      if (clearBlock != null) {
         preparingTransactions.add(clearBlock);
      }
      if (stateTransferInProgress.get() == null) {
         stateTransferInProgress.set(new TotalOrderLatchImpl("StateTransfer-" + topologyId));
      }
      if (log.isTraceEnabled()) {
         log.tracef("State Transfer start. It will wait for %s", preparingTransactions);
      }
      return preparingTransactions;
   }

   /**
    * It notifies the end of the state transfer possibly unblock waiting prepares.
    */
   @Override
   public final void notifyStateTransferEnd() {
      TotalOrderLatch block = stateTransferInProgress.getAndSet(null);
      if (block != null) {
         block.unBlock();
      }
      if (log.isTraceEnabled()) {
         log.tracef("State Transfer finish. It will release %s", block);
      }
      totalOrderExecutor.checkForReadyTasks();
   }

   @Override
   public final boolean hasAnyLockAcquired() {
      return !keysLocked.isEmpty() || clear.get() != null;
   }

   private void acquireLock(TotalOrderRemoteTransactionState state, TotalOrderLatch latch, Object key, boolean write) {
      state.addLockedKey(key);
      TotalOrderLock lock = new TotalOrderLock();
      TotalOrderLock existing = keysLocked.putIfAbsent(key, lock);
      Collection<TotalOrderLatch> dependencies;
      if (existing != null) {
         dependencies = write ? existing.writeLock(latch) : existing.readLock(latch);
         if (dependencies == null) {
            keysLocked.put(key, lock);
            dependencies = write ? lock.writeLock(latch) : lock.readLock(latch);
         }
      } else {
         dependencies = write ? lock.writeLock(latch) : lock.readLock(latch);
      }
      if (dependencies == null) {
         throw new IllegalStateException("Dependencies are null. This is not possible. Possible concurrent put/remove");
      }
      state.addAllSynchronizedBlocks(dependencies);
   }

   private class TotalOrderLock {
      private final List<TotalOrderLatch> readLock;
      private TotalOrderLatch writeLock;
      private boolean newEntry;
      private boolean markedForRemove;

      private TotalOrderLock() {
         this.readLock = new ArrayList<TotalOrderLatch>();
         this.newEntry = true;
         this.markedForRemove = false;
      }

      /**
       * Sets requestor to the write lock;
       *
       * @return {@code null} if this entry is marked for remove, otherwise a non-null list of dependencies.
       */
      public final synchronized List<TotalOrderLatch> writeLock(TotalOrderLatch requestor) {
         invariantCheck();
         if (markedForRemove) {
            return null;
         }
         List<TotalOrderLatch> dependencies = new ArrayList<TotalOrderLatch>();
         if (writeLock != null) {
            dependencies.add(writeLock);
         } else {
            dependencies.addAll(readLock);
            readLock.clear();
         }
         newEntry = false;
         writeLock = requestor;
         return dependencies;
      }

      /**
       * Add the requestor to the read lock collection;
       *
       * @return {@code null} if this entry is marked for remove, otherwise a non-null list of dependencies.
       */
      public final synchronized List<TotalOrderLatch> readLock(TotalOrderLatch requestor) {
         invariantCheck();
         if (markedForRemove) {
            return null;
         }
         List<TotalOrderLatch> dependencies = new ArrayList<TotalOrderLatch>();
         if (writeLock != null) {
            if (writeLock.equals(requestor)) {
               //this tx already acquired the read lock.
               return dependencies;
            }
            dependencies.add(writeLock);
            writeLock = null;
         }
         readLock.add(requestor);
         newEntry = false;
         return dependencies;
      }

      public final synchronized List<TotalOrderLatch> acquireForClear() {
         invariantCheck();
         if (markedForRemove) {
            return Collections.emptyList();
         }
         List<TotalOrderLatch> dependencies = new ArrayList<TotalOrderLatch>();
         if (writeLock != null) {
            dependencies.add(writeLock);
            writeLock = null;
         } else {
            dependencies.addAll(readLock);
            readLock.clear();
         }
         markedForRemove = true;
         return dependencies;
      }

      /**
       * Removes the owner from the write lock or read lock collection.
       *
       * @return {@code true} if this key lock has no more locks acquired and it can be removed from the map
       */
      public final synchronized boolean releaseLock(TotalOrderLatch owner) {
         invariantCheck();
         if (markedForRemove) {
            //safe because the write lock and read lock are both null or empty
            return false;
         }
         if (writeLock != null && writeLock.equals(owner)) {
            writeLock = null;
         } else {
            readLock.remove(owner);
         }
         //this is a new entry and it's empty in the first usage. Don't remove it!
         if (newEntry) {
            return false;
         }
         markedForRemove = writeLock == null && readLock.isEmpty();
         return markedForRemove;
      }

      /**
       * Removes the owner from the write lock or read lock collection.
       *
       * @return {@code true} if this key lock has no more locks acquired and it can be removed from the map
       */
      public final synchronized List<TotalOrderLatch> stateTransfer() {
         invariantCheck();
         if (markedForRemove) {
            return Collections.emptyList();
         }
         return writeLock != null ? Collections.singletonList(writeLock) : readLock;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         TotalOrderLock lock = (TotalOrderLock) o;

         return markedForRemove == lock.markedForRemove &&
               newEntry == lock.newEntry &&
               readLock.equals(lock.readLock) &&
               !(writeLock != null ? !writeLock.equals(lock.writeLock) : lock.writeLock != null);

      }

      @Override
      public int hashCode() {
         int result = readLock.hashCode();
         result = 31 * result + (writeLock != null ? writeLock.hashCode() : 0);
         result = 31 * result + (newEntry ? 1 : 0);
         result = 31 * result + (markedForRemove ? 1 : 0);
         return result;
      }

      private void invariantCheck() {
         //or we have read locks or write lock
         if (writeLock != null && !readLock.isEmpty()) {
            throw new IllegalStateException("Total Order Manager has write lock and read locks acquired");
         }
      }
   }
}
