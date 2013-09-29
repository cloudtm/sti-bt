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

package org.infinispan.interceptors.locking;

import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.gmu.GMUHelper;
import org.infinispan.util.TimSort;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class OptimisticReadWriteLockingInterceptor extends OptimisticLockingInterceptor {

   @Override
   protected void afterWriteLocksAcquired(TxInvocationContext ctx, PrepareCommand command) throws InterruptedException {
      if (ctx.isOriginLocal() && ctx.getCacheTransaction().getAllModifications().isEmpty()) {
         //transaction is read-only. don't acquire the locks because nothing will be validated.
         return;
      }
      GMUPrepareCommand spc = GMUHelper.convert(command, GMUPrepareCommand.class);
      Object[] readSet = ctx.isOriginLocal() ? ctx.getReadSet().toArray() : spc.getReadSet();
      TimSort.sort(readSet, PrepareCommand.KEY_COMPARATOR);
      acquireReadLocks(ctx, readSet);
   }

   private void acquireReadLocks(TxInvocationContext ctx, Object[] readSet) throws InterruptedException {
      long lockTimeout = cacheConfiguration.locking().lockAcquisitionTimeout();
      for (Object key : readSet) {
         lockAndRegisterShareBackupLock(ctx, key, lockTimeout, false);
         ctx.addAffectedKey(key);
      }
   }

   @Override
   protected boolean releaseLockOnTxCompletion(TxInvocationContext ctx) {
      return true;
   }
}
