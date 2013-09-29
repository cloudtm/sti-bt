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
package org.infinispan.transaction.gmu;

import org.infinispan.CacheException;
import org.infinispan.CacheImpl;
import org.infinispan.DelayedComputation;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.gmu.GMUEntryFactoryImpl;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.dataplacement.ClusterSnapshot;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.container.gmu.GMUEntryFactoryImpl.wrap;
import static org.infinispan.container.versioning.InequalVersionComparisonResult.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUHelper {

   private static final Log log = LogFactory.getLog(GMUHelper.class);

   public static void performReadSetValidation(GMUPrepareCommand prepareCommand,
                                               DataContainer dataContainer,
                                               ClusteringDependentLogic keyLogic, GMUVersion readVersion) {
      GlobalTransaction gtx = prepareCommand.getGlobalTransaction();
      for (Object key : prepareCommand.getReadSet()) {
         //if (keyLogic.localNodeIsOwner(key)) {
         if (keyLogic.localNodeIsPrimaryOwner(key)) {      //DIE: for now, hardcoded
            final InternalGMUCacheEntry cacheEntry = toInternalGMUCacheEntry(dataContainer.get(key, readVersion));
            if (log.isDebugEnabled()) {
               log.debugf("[%s] Validate [%s]: checking %s", gtx.globalId(), key, cacheEntry);
            }
            if (!cacheEntry.isMostRecent()) {
               throw new ValidationException("Validation failed for key [" + key + "]", key);
            }
         } else {
            if (log.isDebugEnabled()) {
               log.debugf("[%s] Validate [%s]: keys is not local", gtx.globalId(), key);
            }
         }
      }
      
   }

   public static EntryVersion calculateCommitVersion(EntryVersion mergedVersion, GMUVersionGenerator versionGenerator,
                                                     Collection<Address> affectedOwners) {
      if (mergedVersion == null) {
         throw new NullPointerException("Null merged version is not allowed to calculate commit view");
      }

      return versionGenerator.calculateCommitVersion(mergedVersion, affectedOwners);
   }

   public static InternalGMUCacheEntry toInternalGMUCacheEntry(InternalCacheEntry entry) {
      return convert(entry, InternalGMUCacheEntry.class);
   }

   public static GMUVersion toGMUVersion(EntryVersion version) {
      return convert(version, GMUVersion.class);
   }

   public static GMUVersionGenerator toGMUVersionGenerator(VersionGenerator versionGenerator) {
      return convert(versionGenerator, GMUVersionGenerator.class);
   }

   public static <T> T convert(Object object, Class<T> clazz) {
      if (log.isDebugEnabled()) {
         log.debugf("Convert object %s to class %s", object, clazz.getCanonicalName());
      }
      try {
         return clazz.cast(object);
      } catch (ClassCastException cce) {
         log.fatalf(cce, "Error converting object %s to class %s", object, clazz.getCanonicalName());
         throw new IllegalArgumentException("Expected " + clazz.getSimpleName() +
                                                  " and not " + object.getClass().getSimpleName());
      }
   }

   public static void joinAndSetTransactionVersion(Collection<Response> responses, TxInvocationContext ctx,
                                                   GMUVersionGenerator versionGenerator, EntryVersion[] remotePrepares) {
      if (responses.isEmpty() && remotePrepares == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Versions received are empty!");
         }
         return;
      }
      List<EntryVersion> allPreparedVersions = new LinkedList<EntryVersion>();
      allPreparedVersions.add(ctx.getTransactionVersion());
      GlobalTransaction gtx = ctx.getGlobalTransaction();

      //process all responses
      for (Response r : responses) {
         if (r == null) {
            throw new IllegalStateException("Non-null response with new version is expected");
         } else if (r instanceof SuccessfulResponse) {
            EntryVersion version = convert(((SuccessfulResponse) r).getResponseValue(), EntryVersion.class);
            allPreparedVersions.add(version);
         } else if (r instanceof ExceptionResponse) {
            throw new ValidationException(((ExceptionResponse) r).getException());
         } else if (!r.isSuccessful()) {
            throw new CacheException("Unsuccessful response received... aborting transaction " + gtx.globalId());
         }
      }

      // merge remote prepares
      if (remotePrepares != null) {
         for (EntryVersion ev : remotePrepares) {
            allPreparedVersions.add(ev);
         }
      }
      
      EntryVersion[] preparedVersionsArray = new EntryVersion[allPreparedVersions.size()];
      EntryVersion commitVersion = versionGenerator.mergeAndMax(allPreparedVersions.toArray(preparedVersionsArray));

      if (log.isTraceEnabled()) {
         log.tracef("Merging transaction [%s] prepare versions %s ==> %s", gtx.globalId(), allPreparedVersions,
                    commitVersion);
      }

      ctx.setTransactionVersion(commitVersion);
   }

   public static InternalCacheEntry loadFromCacheLoader(InvocationContext context, Object key, CacheLoader loader,
                                                        CommitLog commitLog, GMUVersionGenerator versionGenerator)
         throws Throwable {
      //code copied from getFromContainer()
      boolean singleRead = context instanceof SingleKeyNonTxInvocationContext;
      boolean remotePrepare = !context.isOriginLocal() && context.isInTxScope();
      boolean remoteRead = !context.isOriginLocal() && !context.isInTxScope();

      EntryVersion versionToRead;
      if (singleRead || remotePrepare) {
         //read the most recent version
         //in the prepare, the value does not matter (it will be written or it is not read)
         //                and the version does not matter either (it will be overwritten)
         versionToRead = null;
      } else {
         versionToRead = context.calculateVersionToRead(versionGenerator);
      }

      boolean hasAlreadyReadFromThisNode = context.hasAlreadyReadOnThisNode();

      if (context.isInTxScope() && context.isOriginLocal() && !context.hasAlreadyReadOnThisNode()) {
         //firs read on the local node for a transaction. ensure the min version
         EntryVersion transactionVersion = ((TxInvocationContext) context).getTransactionVersion();
         try {
            commitLog.waitForVersion(transactionVersion, -1);
         } catch (InterruptedException ex) {
            //ignore...
         }
      }

      EntryVersion maxVersionToRead = hasAlreadyReadFromThisNode ? versionToRead :
            commitLog.getAvailableVersionLessThan(versionToRead);

      EntryVersion mostRecentCommitLogVersion = commitLog.getCurrentVersion();

      InternalCacheEntry loaded = loader.load(key);
      boolean valid = true;
      if (log.isTraceEnabled()) {
         log.tracef("Loaded %s from cache store. Compare %s to %s", loaded, loaded == null ? "Not Loaded" : loaded.getVersion(),
                    maxVersionToRead);
      }
      if (loaded != null && loaded.getVersion() == null) {
         throw new IllegalStateException("Cache entry stored must have a version");
      }
      if (loaded != null && maxVersionToRead != null) {
         InequalVersionComparisonResult result = loaded.getVersion().compareTo(maxVersionToRead);
         valid = result == BEFORE || result == BEFORE_OR_EQUAL || result == EQUAL;
      }

      if (log.isTraceEnabled()) {
         log.tracef("Loaded %s from cache store. Is valid? %s", loaded, valid);
      }
      if (!valid) {
         throw new CacheException("No valid version available");
      }

      //this is considered the most recent version.
      InternalGMUCacheEntry entry = wrap(key, loaded, true, maxVersionToRead,
                                         loaded == null ? null : loaded.getVersion(), null);

      if (log.isTraceEnabled()) {
         log.tracef("Loaded %s from cache store. Created wrapped cache entry %s", loaded, entry);
      }
      if (remoteRead) {
         if (entry.getMaximumValidVersion() == null) {
            entry.setMaximumValidVersion(mostRecentCommitLogVersion);
         } else {
            entry.setMaximumValidVersion(commitLog.getEntry(entry.getMaximumValidVersion()));
         }
         if (entry.getCreationVersion() == null) {
            entry.setCreationVersion(commitLog.getOldestVersion());
         } else {
            entry.setCreationVersion(commitLog.getEntry(entry.getCreationVersion()));
         }
      }

      context.addKeyReadInCommand(key, entry);

      return loaded;
   }

   public static BitSet toAlreadyReadFromMask(Collection<Address> alreadyReadFrom, GMUVersionGenerator versionGenerator,
                                              int viewId) {
      if (alreadyReadFrom == null || alreadyReadFrom.isEmpty()) {
         return null;
      }

      final ClusterSnapshot clusterSnapshot = versionGenerator.getClusterSnapshot(viewId);
      final BitSet alreadyReadFromMask = new BitSet(clusterSnapshot.size());

      for (Address address : alreadyReadFrom) {
         int idx = clusterSnapshot.indexOf(address);
         if (idx != -1) {
            alreadyReadFromMask.set(idx);
         }
      }
      return alreadyReadFromMask;
   }

   public static List<Address> fromAlreadyReadFromMask(BitSet alreadyReadFromMask, GMUVersionGenerator versionGenerator,
                                                       int viewId) {
      if (alreadyReadFromMask == null || alreadyReadFromMask.isEmpty()) {
         return Collections.emptyList();
      }
      ClusterSnapshot clusterSnapshot = versionGenerator.getClusterSnapshot(viewId);
      List<Address> addressList = new LinkedList<Address>();
      for (int i = 0; i < clusterSnapshot.size(); ++i) {
         if (alreadyReadFromMask.get(i)) {
            addressList.add(clusterSnapshot.get(i));
         }
      }
      return addressList;
   }

   public static void performDelayedComputations(CacheTransaction cacheTx, ClusteringDependentLogic distributionLogic) {
       DelayedComputation[] delayedComputations = cacheTx.getDelayedComputations();
       if (delayedComputations == null) {
          return;
       }
       for (DelayedComputation computation : delayedComputations) {
          Object key = computation.getAffectedKey();
          if (distributionLogic.localNodeIsOwner(key)) {
              computation.compute();
          } 
       }
    }

}
