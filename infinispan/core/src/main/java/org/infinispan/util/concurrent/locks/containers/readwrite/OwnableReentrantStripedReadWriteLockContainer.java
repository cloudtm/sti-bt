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
package org.infinispan.util.concurrent.locks.containers.readwrite;

import org.infinispan.util.concurrent.locks.OwnableReentrantReadWriteLock;
import org.infinispan.util.concurrent.locks.containers.AbstractStripedLockContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class OwnableReentrantStripedReadWriteLockContainer extends AbstractStripedLockContainer<OwnableReentrantReadWriteLock> {

   private static final Log log = LogFactory.getLog(OwnableReentrantStripedReadWriteLockContainer.class);
   
   private OwnableReentrantReadWriteLock[] sharedLocks;

   public OwnableReentrantStripedReadWriteLockContainer(int concurrencyLevel) {
      initLocks(calculateNumberOfSegments(concurrencyLevel));
   }

   @Override
   protected void initLocks(int numLocks) {
      sharedLocks = new OwnableReentrantReadWriteLock[numLocks];
      for (int i = 0; i < numLocks; i++) sharedLocks[i] = new OwnableReentrantReadWriteLock();
   }

   @Override
   protected void unlock(OwnableReentrantReadWriteLock toRelease, Object owner) {
      toRelease.unlock(owner);
   }

   @Override
   protected boolean tryExclusiveLock(OwnableReentrantReadWriteLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.tryLock(lockOwner, timeout, unit);
   }

   @Override
   protected boolean tryShareLock(OwnableReentrantReadWriteLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.tryShareLock(lockOwner, timeout, unit);
   }

   @Override
   protected void exclusiveLock(OwnableReentrantReadWriteLock lock, Object lockOwner) {
      lock.lock(lockOwner);
   }

   @Override
   protected void shareLock(OwnableReentrantReadWriteLock lock, Object lockOwner) {
      lock.lockShare(lockOwner);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   public boolean ownsExclusiveLock(Object key, Object owner) {
      OwnableReentrantReadWriteLock lock = getLock(key);
      return owner.equals(lock.getOwner());
   }

   @Override
   public boolean ownsShareLock(Object key, Object owner) {
      return getLock(key).ownsShareLock(owner);
   }

   @Override
   public boolean isExclusiveLocked(Object key) {
      return getLock(key).isLocked();
   }

   @Override
   public boolean isSharedLocked(Object key) {
      return getLock(key).isShareLocked();
   }

   @Override
   public OwnableReentrantReadWriteLock getLock(Object key) {
      return sharedLocks[hashToIndex(key)];
   }

   @Override
   public int getNumLocksHeld() {
      int i = 0;
      for (OwnableReentrantReadWriteLock l : sharedLocks) if (l.isLocked()) i++;
      return i;
   }

   @Override
   public int size() {
      return sharedLocks.length;
   }
}
