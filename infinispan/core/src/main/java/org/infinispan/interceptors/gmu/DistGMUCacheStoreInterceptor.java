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

package org.infinispan.interceptors.gmu;

import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DistCacheStoreInterceptor;
import org.infinispan.loaders.gmu.GMUStore;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DistGMUCacheStoreInterceptor extends DistCacheStoreInterceptor {

   @Override
   protected void commitCommand(TxInvocationContext ctx) throws Throwable {
      if (!ctx.getCacheTransaction().getAllModifications().isEmpty()) {
         // this is a commit call.
         GlobalTransaction tx = ctx.getGlobalTransaction();

         // Regardless of outcome, remove from preparing txs
         preparingTxs.remove(tx);

         if (getStatisticsEnabled()) {
            Integer puts = txStores.get(tx);
            if (puts != null) {
               cacheStores.getAndAdd(puts);
            }
            txStores.remove(tx);
         }
      } else {
         if (getLog().isTraceEnabled()) getLog().trace("Commit called with no modifications; ignoring.");
      }
   }

   @Override
   protected StoreModificationsBuilder createNewStoreModificationsBuilder(int modificationSize) {
      return new GMUStoreModificationsBuilder(getStatisticsEnabled(), modificationSize);
   }

   public class GMUStoreModificationsBuilder extends StoreModificationsBuilder {

      public GMUStoreModificationsBuilder(boolean generateStatistics, int numMods) {
         super(generateStatistics, numMods);
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         return visitSingleStore(ctx, command.getKey());
      }

      @Override
      protected Store createStoreModification(InvocationContext ctx, Object key) {
         CacheEntry entry = ctx.lookupEntry(key);
         if (entry instanceof SerializableEntry) {
            return new GMUStore((SerializableEntry) entry, entryFactory);
         }
         throw new IllegalStateException("Unsupported cache entry type " + entry.getClass().getSimpleName());
      }
   }
}
