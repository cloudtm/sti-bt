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

package org.infinispan.util.concurrent.locks;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class OwnableReentrantReadWriteLock extends OwnableReentrantLock {

   private static final Log log = LogFactory.getLog(OwnableReentrantReadWriteLock.class);

   private transient final Map<Object, AtomicInteger> readCounters = new HashMap<Object, AtomicInteger>();

   public final int getLockState() {
      return getState();
   }

   public final boolean tryShareLock(Object requestor, long time, TimeUnit unit) throws InterruptedException {
      if (log.isTraceEnabled()) {
         log.tracef("%s tryShareLock(%s)", requestor, System.identityHashCode(this));
      }
      setCurrentRequestor(requestor);
      try {
         return tryAcquireSharedNanos(1, unit.toNanos(time));
      } finally {
         if (log.isTraceEnabled()) {
            log.tracef("%s tryShareLock(%s) => FINISH", requestor, System.identityHashCode(this));
         }
         unsetCurrentRequestor();
      }
   }

   @Override
   public void unlock(Object requestor) {
      if (getState() < 0) {
         unlockShare(requestor);
      } else {
         super.unlock(requestor);
      }
   }

   private void unlockShare(Object requestor) {
      if (log.isTraceEnabled()) {
         log.tracef("%s unlockShare(%s)", requestor, System.identityHashCode(this));
      }
      setCurrentRequestor(requestor);
      try {
         releaseShared(1);
      } finally {
         if (log.isTraceEnabled()) {
            log.tracef("%s unlockShare(%s) => FINISH", requestor, System.identityHashCode(this));
         }
         unsetCurrentRequestor();
      }
   }

   public final void lockShare(Object requestor) {
      setCurrentRequestor(requestor);
      try {
         acquireShared(1);
      } finally {
         unsetCurrentRequestor();
      }
   }
   
   public final boolean ownsShareLock(Object owner) {
      synchronized (readCounters) {
         return readCounters.containsKey(owner);
      }
   }
   
   public final boolean isShareLocked() {
      return getState() < 0;
   }

   @Override
   protected int tryAcquireShared(int i) {
      Object requestor = currentRequestor();
      int state = getState();
      if (state <= 0 && compareAndSetState(state, state - 1)) {
         incrementRead(requestor);
         if (log.isTraceEnabled()) {
            log.tracef("%s tryAcquireShared(%s) => SUCCESS", requestor, System.identityHashCode(this));
         }
         return 1;
      } else if (state > 0) {
         if (log.isTraceEnabled()) {
            log.tracef("%s tryAcquireShared(%s) => WRITE_LOCKED (%s)", requestor, System.identityHashCode(this),
                       requestor.equals(getOwner()));
         }
         return requestor.equals(getOwner()) ? 0 : -1 ;
      }
      if (log.isTraceEnabled()) {
         log.tracef("%s tryAcquireShared(%s) => FAILED", requestor, System.identityHashCode(this));
      }
      return -1;
   }

   @Override
   protected boolean tryReleaseShared(int i) {
      if (!decrementRead(currentRequestor())) {
         if (log.isTraceEnabled()) {
            log.tracef("%s tryReleaseShared(%s) => FAILED (Not Onwer)", currentRequestor(), System.identityHashCode(this));
         }
         throw new IllegalMonitorStateException(this.toString() + "[Requestor is "+currentRequestor()+"]");
      }
      while (true) {
         int state = getState();
         if (compareAndSetState(state, state + 1)) {
            if (log.isTraceEnabled()) {
               log.tracef("%s tryReleaseShared(%s) => SUCCESS", currentRequestor(), System.identityHashCode(this));
            }
            return true;
         }
      }
   }        

   @Override
   protected void resetState() {
      super.resetState();
      readCounters.clear();
   }

   private void incrementRead(Object owner) {
      synchronized (readCounters) {
         AtomicInteger counter = readCounters.get(owner);
         if (counter == null) {
            readCounters.put(owner, new AtomicInteger(1));
         } else {
            counter.incrementAndGet();
         }
      }
   }
   
   private boolean decrementRead(Object owner) {
      synchronized (readCounters) {
         AtomicInteger counter = readCounters.get(owner);
         if (counter == null) {
            return false;
         }
         if (counter.decrementAndGet() == 0) {
            readCounters.remove(owner);
         }
         return true;
      }
   }
}
