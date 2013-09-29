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
package org.infinispan.interceptors.gmu;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheValue;
import org.infinispan.container.gmu.L1GMUContainer;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.DEFPrepare;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.ValidationException;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.infinispan.transaction.gmu.GMUHelper.*;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @author Hugo Pimentel
 * @since 5.2
 */
public class GMUDistributionInterceptor extends TxDistributionInterceptor {

   private static final Log log = LogFactory.getLog(GMUDistributionInterceptor.class);
   protected GMUVersionGenerator versionGenerator;
   private L1GMUContainer l1GMUContainer;
   private CommitLog commitLog;

   @Inject
   public void setVersionGenerator(VersionGenerator versionGenerator, L1GMUContainer l1GMUContainer, CommitLog commitLog) {
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
      this.l1GMUContainer = l1GMUContainer;
      this.commitLog = commitLog;
   }

   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command, Collection<Address> recipients, boolean sync) {
      LocalTransaction localTx = (LocalTransaction)ctx.getCacheTransaction();
      // potential cool place to send asynch prepares to remote txs, and check them out after doing the local prepare
      Map<Object, GlobalTransaction> remoteDEFs = localTx.getRemoteDEFs();
      List<Future> defAnswers = null;
      if (remoteDEFs != null) {
         defAnswers = new ArrayList<Future>(remoteDEFs.size());
         for (Map.Entry<Object, GlobalTransaction> entry : remoteDEFs.entrySet()) {
            defAnswers.add(TransactionCoordinator.des.submit(new DEFPrepare(entry.getValue()), entry.getKey()));
         }
         localTx.sentDEFPrepare = true;
      }
      
      // invoke the locally known prepares
      Map<Address, Response> responses = rpcManager.invokeRemotely(recipients, command, true, true, false);
      log.debugf("prepare command for transaction %s is sent. responses are: %s",
                 command.getGlobalTransaction().globalId(), responses.toString());

      EntryVersion[] remotePrepares = null;
      // synchronize with remote prepares
      if (defAnswers != null) {
         remotePrepares = new EntryVersion[defAnswers.size()];
         int i = 0;
         for (Future fut : defAnswers) {
            try {
               remotePrepares[i] = (EntryVersion) fut.get();
               i++;
            } catch (InterruptedException e) {
               e.printStackTrace();
               System.err.println("Interrupted Exception in DEF 2PC");
               System.exit(-1);
            } catch (ExecutionException e) {
               Throwable t = e.getCause();
               if (t instanceof RuntimeException) {
                  throw (RuntimeException) t;
               } else if (t instanceof javax.transaction.RollbackException) {
        	   throw new ValidationException(t);
               } else {
                  t.printStackTrace();
                  System.err.println("Checked Exception in DEF 2PC");
                  System.exit(-1);
               }
            }
         }
      }
      
      // merge all vectors
      joinAndSetTransactionVersion(responses.values(), ctx, versionGenerator, remotePrepares);
   }

   @Override
   protected InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx, boolean acquireRemoteLock, FlagAffectedCommand command)
         throws Exception {
      if (isL1CacheEnabled && ctx instanceof TxInvocationContext) {
         if (log.isTraceEnabled()) {
            log.tracef("Trying to retrieve a the key %s from L1 GMU Data Container", key);
         }
         TxInvocationContext txInvocationContext = (TxInvocationContext) ctx;
         InternalGMUCacheEntry gmuCacheEntry = l1GMUContainer.getValidVersion(key,
                                                                              txInvocationContext.getTransactionVersion(),
                                                                              txInvocationContext.getAlreadyReadFrom());
         if (gmuCacheEntry != null) {
            if (log.isTraceEnabled()) {
               log.tracef("Retrieve a L1 entry for key %s: %s", key, gmuCacheEntry);
            }
            txInvocationContext.addKeyReadInCommand(key, gmuCacheEntry);
            txInvocationContext.addReadFrom(dm.getPrimaryLocation(key));
            return gmuCacheEntry.getInternalCacheEntry();
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Failed to retrieve  a L1 entry for key %s", key);
      }
      return performRemoteGet(key, ctx, acquireRemoteLock, command);
   }

   @Override
   protected void storeInL1(Object key, InternalCacheEntry ice, InvocationContext ctx, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Doing a put in L1 into the L1 GMU Data Container");
      }
      InternalGMUCacheEntry gmuCacheEntry = ctx.getKeysReadInCommand().get(key);
      if (gmuCacheEntry == null) {
         throw new NullPointerException("GMU cache entry cannot be null");
      }
      l1GMUContainer.insertOrUpdate(key, gmuCacheEntry);
      CacheEntry ce = ctx.lookupEntry(key);
      if (ce == null || ce.isNull() || ce.isLockPlaceholder() || ce.getValue() == null) {
         if (ce != null && ce.isChanged()) {
            ce.setValue(ice.getValue());
         } else {
            if (isWrite)
               entryFactory.wrapEntryForPut(ctx, key, ice, false, command);
            else
               ctx.putLookedUpEntry(key, ice);
         }
      }
   }

   private InternalCacheEntry performRemoteGet(Object key, InvocationContext ctx, boolean acquireRemoteLock, FlagAffectedCommand command) throws Exception {
      if (ctx instanceof SingleKeyNonTxInvocationContext) {
         return retrieveSingleKeyFromRemoteSource(key, (SingleKeyNonTxInvocationContext) ctx, command);
      } else if (ctx instanceof TxInvocationContext) {
         return retrieveTransactionalGetFromRemoteSource(key, (TxInvocationContext) ctx, acquireRemoteLock, command);
      }
      throw new IllegalStateException("Only handles transaction context or single key gets");
   }

   private InternalCacheEntry retrieveTransactionalGetFromRemoteSource(Object key, TxInvocationContext txInvocationContext,
                                                                       boolean acquireRemoteLock, FlagAffectedCommand command) {
      GlobalTransaction gtx = acquireRemoteLock ? txInvocationContext.getGlobalTransaction() : null;

      //List<Address> targets = new ArrayList<Address>(stateTransferManager.getCacheTopology().getReadConsistentHash().locateOwners(key));      // if any of the recipients has left the cluster since the command was issued, just don't wait for its response
      //Cloud-TM patch
      Address primaryA = stateTransferManager.getCacheTopology().getReadConsistentHash().locatePrimaryOwner(key);
      List<Address> targets = new ArrayList<Address>();
      targets.add(primaryA);
      targets.retainAll(rpcManager.getTransport().getMembers());

      Collection<Address> alreadyReadFrom = txInvocationContext.getAlreadyReadFrom();
      GMUVersion transactionVersion = toGMUVersion(txInvocationContext.getTransactionVersion());

      final BitSet alreadyReadFromMask = toAlreadyReadFromMask(alreadyReadFrom, versionGenerator,
                                                               transactionVersion.getViewId());

      ClusteredGetCommand get = cf.buildGMUClusteredGetCommand(key, command.getFlags(), acquireRemoteLock,
                                                               gtx, transactionVersion, alreadyReadFromMask);

      if (log.isDebugEnabled()) {
         log.debugf("Perform a remote get for transaction %s. %s",
                    txInvocationContext.getGlobalTransaction().globalId(), get);
      }

      ResponseFilter filter = new ClusteredGetResponseValidityFilter(targets, rpcManager.getAddress());
      Map<Address, Response> responses = rpcManager.invokeRemotely(targets, get, ResponseMode.WAIT_FOR_VALID_RESPONSE,
                                                                   cacheConfiguration.clustering().sync().replTimeout(), true, filter, false);

      if (log.isDebugEnabled()) {
         log.debugf("Remote get done for transaction %s [key:%s]. response are: %s",
                    txInvocationContext.getGlobalTransaction().globalId(),
                    key, responses);
      }

      if (!responses.isEmpty()) {
         for (Map.Entry<Address, Response> entry : responses.entrySet()) {
            Response r = entry.getValue();
            if (r instanceof SuccessfulResponse) {
               InternalGMUCacheValue gmuCacheValue = convert(((SuccessfulResponse) r).getResponseValue(),
                                                             InternalGMUCacheValue.class);

               InternalGMUCacheEntry gmuCacheEntry = (InternalGMUCacheEntry) gmuCacheValue.toInternalCacheEntry(key);
               txInvocationContext.addKeyReadInCommand(key, gmuCacheEntry);
               txInvocationContext.addReadFrom(entry.getKey());

               if (log.isDebugEnabled()) {
                  log.debugf("Remote Get successful for transaction %s and key %s. Return value is %s",
                             txInvocationContext.getGlobalTransaction().globalId(), key, gmuCacheValue);
               }
               return gmuCacheEntry;
            }
         }
      }

      // TODO If everyone returned null, and the read CH has changed, retry the remote get.
      // Otherwise our get command might be processed by the old owners after they have invalidated their data
      // and we'd return a null even though the key exists on
      return null;
   }

   private InternalCacheEntry retrieveSingleKeyFromRemoteSource(Object key, SingleKeyNonTxInvocationContext ctx, FlagAffectedCommand command) {
      //List<Address> targets = new ArrayList<Address>(stateTransferManager.getCacheTopology().getReadConsistentHash().locateOwners(key));      // if any of the recipients has left the cluster since the command was issued, just don't wait for its response
      //Cloud-TM patch
      Address primaryA = stateTransferManager.getCacheTopology().getReadConsistentHash().locatePrimaryOwner(key);
      List<Address> targets = new ArrayList<Address>();
      targets.add(primaryA);
      targets.retainAll(rpcManager.getTransport().getMembers());

      ClusteredGetCommand get = cf.buildGMUClusteredGetCommand(key, command.getFlags(), false, null,
                                                               toGMUVersion(commitLog.getCurrentVersion()), null);

      if (log.isDebugEnabled()) {
         log.debugf("Perform a single remote get. %s", get);
      }

      ResponseFilter filter = new ClusteredGetResponseValidityFilter(targets, rpcManager.getAddress());
      Map<Address, Response> responses = rpcManager.invokeRemotely(targets, get, ResponseMode.WAIT_FOR_VALID_RESPONSE,
                                                                   cacheConfiguration.clustering().sync().replTimeout(), true, filter, false);

      if (log.isDebugEnabled()) {
         log.debugf("Remote get done for single key [key:%s]. response are: %s", key, responses);
      }


      if (!responses.isEmpty()) {
         for (Map.Entry<Address, Response> entry : responses.entrySet()) {
            Response r = entry.getValue();
            if (r == null) {
               continue;
            }
            if (r instanceof SuccessfulResponse) {
               InternalGMUCacheValue gmuCacheValue = convert(((SuccessfulResponse) r).getResponseValue(),
                                                             InternalGMUCacheValue.class);

               InternalGMUCacheEntry gmuCacheEntry = (InternalGMUCacheEntry) gmuCacheValue.toInternalCacheEntry(key);
               ctx.addKeyReadInCommand(key, gmuCacheEntry);

               if (log.isDebugEnabled()) {
                  log.debugf("Remote Get successful for single key %s. Return value is %s", key, gmuCacheValue);
               }
               return gmuCacheEntry;
            }
         }
      }

      // TODO If everyone returned null, and the read CH has changed, retry the remote get.
      // Otherwise our get command might be processed by the old owners after they have invalidated their data
      // and we'd return a null even though the key exists on
      return null;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Future<?> f = flushL1Caches(ctx);
         retVal = sendGMUCommitCommand(retVal, command, getCommitNodes(ctx));
         blockOnL1FutureIfNeeded(f);
      } else if (isL1CacheEnabled && !ctx.isOriginLocal() && !ctx.getLockedKeys().isEmpty()) {
         // We fall into this block if we are a remote node, happen to be the primary data owner and have locked keys.
         // it is still our responsibility to invalidate L1 caches in the cluster.
         blockOnL1FutureIfNeeded(flushL1Caches(ctx));
      }
      return retVal;
   }

}
