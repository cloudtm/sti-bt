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

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.ClusteredActivationInterceptor;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.GMUHelper;

import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * //TODO: document this!
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class GMUActivationInterceptor extends ClusteredActivationInterceptor {

   private CommitLog commitLog;
   private GMUVersionGenerator versionGenerator;

   @Inject
   public void setGMUComponents(CommitLog commitLog, VersionGenerator versionGenerator) {
      this.commitLog = commitLog;
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
   }

   @Override
   protected boolean loadIfNeeded(InvocationContext context, Object key, boolean isRetrieval, FlagAffectedCommand cmd) throws Throwable {
      if (clm.isShared()) {
         throw new IllegalStateException("Shared cache loader does not work with GMU");
      }

      if (cmd.hasFlag(Flag.SKIP_CACHE_STORE) || cmd.hasFlag(Flag.SKIP_CACHE_LOAD)
            || cmd.hasFlag(Flag.IGNORE_RETURN_VALUES)) {
         return false; //skip operation
      }

      // If this is a remote call, skip loading UNLESS we are the primary data owner of this key, and
      // are using eviction or write skew checking.
      if (!isRetrieval && !context.isOriginLocal() && !forceLoad(key, cmd.getFlags())) return false;

      // first check if the container contains the key we need.  Try and load this into the context.
      CacheEntry e = context.lookupEntry(key);
      if (e == null || e.isNull() || e.getValue() == null) {
         InternalCacheEntry loaded = GMUHelper.loadFromCacheLoader(context, key, loader, commitLog, versionGenerator);
         if (loaded != null) {
            CacheEntry wrappedEntry;
            if (cmd instanceof ApplyDeltaCommand) {
               context.putLookedUpEntry(key, loaded);
               wrappedEntry = entryFactory.wrapEntryForDelta(context, key, ((ApplyDeltaCommand) cmd).getDelta());
            } else {
               wrappedEntry = entryFactory.wrapEntryForPut(context, key, loaded, false, cmd);
            }
            recordLoadedEntry(context, key, wrappedEntry, loaded);
            return true;
         } else {
            return false;
         }
      } else {
         return true;
      }
   }

   @Override
   protected void removeFromStoreIfNeeded(Object... keys) {
      //no-op
   }
}
