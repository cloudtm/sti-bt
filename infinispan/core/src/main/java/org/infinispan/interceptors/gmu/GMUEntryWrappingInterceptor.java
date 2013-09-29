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

import org.infinispan.CacheException;
import org.infinispan.CacheImpl;
import org.infinispan.DelayedComputation;
import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.GMUCommitCommand;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUCacheEntryVersion;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.container.TransactionStatistics;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.GMUHelper;
import org.infinispan.transaction.gmu.manager.CommittedTransaction;
import org.infinispan.transaction.gmu.manager.TransactionCommitManager;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.ResponseFuture;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.stats.ExposedStatistic.*;
import static org.infinispan.transaction.gmu.GMUHelper.*;
import static org.infinispan.transaction.gmu.manager.SortedTransactionQueue.TransactionEntry;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "GMUTransactionManager", description = "Shows information about the transactions committed or prepared")
public class GMUEntryWrappingInterceptor extends EntryWrappingInterceptor {

   private static final Log log = LogFactory.getLog(GMUEntryWrappingInterceptor.class);
   protected GMUVersionGenerator versionGenerator;
   private TransactionCommitManager transactionCommitManager;
   private InvocationContextContainer invocationContextContainer;
   private BlockingTaskAwareExecutorService gmuExecutor;
   private TransactionManager transactionManager;
   private CacheLoaderManager cacheLoaderManager;
   private CacheStore store;
   private CommitLog commitLog;

   @Inject
   public void inject(TransactionCommitManager transactionCommitManager, DataContainer dataContainer,
                      CommitLog commitLog, VersionGenerator versionGenerator, InvocationContextContainer invocationContextContainer,
                      @ComponentName(value = KnownComponentNames.GMU_EXECUTOR) BlockingTaskAwareExecutorService gmuExecutor,
                      TransactionManager transactionManager, CacheLoaderManager cacheLoaderManager) {
      this.transactionCommitManager = transactionCommitManager;
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
      this.invocationContextContainer = invocationContextContainer;
      this.gmuExecutor = gmuExecutor;
      this.transactionManager = transactionManager;
      this.cacheLoaderManager = cacheLoaderManager;
      this.commitLog = commitLog;
   }

   @Start(priority = 16) //after cache store interceptor
   public void enableCacheStore() {
      if (cacheLoaderManager.isShared()) {
         throw new IllegalStateException("Shared cache store is not supported.");
      }
      store = cacheLoaderManager.isUsingPassivation() ? null : cacheLoaderManager.getCacheStore();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (command.isOnePhaseCommit()) {
         throw new IllegalStateException("GMU does not support one phase commits.");
      }

      GMUPrepareCommand spc = convert(command, GMUPrepareCommand.class);

      if (ctx.isOriginLocal()) {
         GMUVersion transactionVersion = toGMUVersion(ctx.getTransactionVersion());
         spc.setVersion(transactionVersion);
         spc.setReadSet(ctx.getReadSet());
         spc.setAlreadyReadFrom(toAlreadyReadFromMask(ctx.getAlreadyReadFrom(), versionGenerator,
                                                      transactionVersion.getViewId()));
      } else {
         ctx.setTransactionVersion(spc.getPrepareVersion());
      }

      wrapEntriesForPrepare(ctx, command);
      performValidation(ctx, spc);

      Object retVal = invokeNextInterceptor(ctx, command);

      if (ctx.isOriginLocal() && command.getModifications().length > 0) {
         EntryVersion commitVersion = calculateCommitVersion(ctx.getTransactionVersion(), versionGenerator,
                                                             cdl.getWriteOwners(ctx.getCacheTransaction()));
         ctx.setTransactionVersion(commitVersion);
      } else {
         retVal = ctx.getTransactionVersion();
      }

      return retVal;
   }

   public static transient final ThreadLocal<TxInvocationContext> CONTEXT_FOR_DELAYED = new ThreadLocal<TxInvocationContext>();
   
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      GMUCommitCommand gmuCommitCommand = convert(command, GMUCommitCommand.class);
      TransactionEntry transactionEntry = null;

      if (ctx.isOriginLocal()) {
         gmuCommitCommand.setCommitVersion(ctx.getTransactionVersion());
         gmuCommitCommand.setDelayedComputations(ctx.getCacheTransaction().getDelayedComputations());
         transactionEntry = transactionCommitManager.commitTransaction(gmuCommitCommand.getGlobalTransaction(),
                                                                       gmuCommitCommand.getCommitVersion());
         //see org.infinispan.tx.gmu.DistConsistencyTest3.testNoCommitDeadlock
         //the commitTransaction() can re-order the queue. we need to check for pending commit commands.
         //if not, the queue can be blocked forever.
         gmuExecutor.checkForReadyTasks();
      } else {
         DelayedComputation[] computations = gmuCommitCommand.getDelayedComputations();
         Map<Object, DelayedComputation> map = new HashMap<Object, DelayedComputation>(computations.length);
         for (DelayedComputation computation : computations) {
            map.put(computation.getAffectedKey(), computation);
         }
         ctx.getCacheTransaction().setDelayedComputations(map);         
      }

      Object retVal = null;
      try {
         retVal = invokeNextIgnoringTimeout(ctx, command);
         //in remote context, the commit command will be enqueue, so it does not need to wait
         if (transactionEntry != null) {     //Only local
            final TransactionStatistics transactionStatistics = TransactionsStatisticsRegistry.getTransactionStatistics();
            boolean waited = transactionStatistics != null && !transactionEntry.isReadyToCommit();
            long waitTime = 0L;
            if (waited) {
               waitTime = System.nanoTime();
            }
            transactionEntry.awaitUntilIsReadyToCommit();
            if (waited) {
               transactionStatistics.incrementValue(NUM_WAITS_IN_COMMIT_QUEUE);
               transactionStatistics.addValue(WAIT_TIME_IN_COMMIT_QUEUE, System.nanoTime() - waitTime);
            }
         } else if (!ctx.isOriginLocal()) {
            transactionEntry = gmuCommitCommand.getTransactionEntry();
         }

         if (transactionEntry == null || transactionEntry.isCommitted()) {
            if (ctx.getCacheTransaction().getAllModifications().isEmpty()) {
               //this is a read-only tx... we need to store the loaded data
               for (CacheEntry entry : ctx.getLookedUpEntries().values()) {
                  if (entry.isLoaded()) {
                     commitContextEntry(entry, ctx, false);
                  }
               }
            }
            updateWaitingTime(transactionEntry);
            awaitCommitCommandAcksIfNeeded(ctx, retVal);
            return retVal;
         }

         Collection<TransactionEntry> transactionsToCommit = transactionCommitManager.getTransactionsToCommit();
         if (log.isTraceEnabled()) {
            log.tracef("Transactions to commit: %s", transactionsToCommit);
         }
         if (transactionsToCommit.isEmpty()) {
            //nothing to commit
            awaitCommitCommandAcksIfNeeded(ctx, retVal);
            return retVal;
         }

         List<CommittedTransaction> committedTransactions = new ArrayList<CommittedTransaction>(transactionsToCommit.size());
         List<TransactionEntry> committedTransactionEntries = new ArrayList<TransactionEntry>(transactionsToCommit.size());
         int subVersion = 0;

         //in case of transaction has the same version... should be rare...
         for (TransactionEntry toCommit : transactionsToCommit) {
            if (!toCommit.committing()) {
               if (log.isTraceEnabled()) {
                  log.tracef("Other thread already started to commit %s. Waiting...", toCommit);
               }
               toCommit.awaitUntilCommitted();
               continue;
            }
            CacheTransaction cacheTransaction = toCommit.getCacheTransactionForCommit();
            CommittedTransaction committedTransaction = new CommittedTransaction(cacheTransaction, subVersion,
                                                                                 toCommit.getConcurrentClockNumber());
            InvocationContext context;
            if (transactionEntry.getGlobalTransaction().equals(toCommit.getGlobalTransaction())) {
               updateCommitVersion(ctx, cacheTransaction, subVersion);
               context = ctx;
            } else {
               context = createInvocationContext(cacheTransaction, subVersion);
            }
            if (log.isTraceEnabled()) {
               log.tracef("Committing %s", toCommit);
            }
            commitContextEntries(context, false, isFromStateTransfer(context));
            committedTransactions.add(committedTransaction);
            committedTransactionEntries.add(toCommit);
            updateWaitingTime(toCommit);
            subVersion++;
         }
         for (TransactionEntry txEntry : committedTransactionEntries) {
            store(txEntry.getGlobalTransaction());
         }
         transactionCommitManager.transactionCommitted(committedTransactions, committedTransactionEntries);
      } catch (Throwable throwable) {
         //let ignore the exception. we cannot have some nodes applying the write set and another not another one
         //receives the rollback and don't applies the write set
         log.fatal("Error while committing transaction", throwable);
         transactionCommitManager.rollbackTransaction(ctx.getCacheTransaction());
      } finally {
         if (ctx.isOriginLocal()) {
            gmuExecutor.checkForReadyTasks();
         }
      }

      awaitCommitCommandAcksIfNeeded(ctx, retVal);

      return retVal;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         transactionCommitManager.rollbackTransaction(ctx.getCacheTransaction());
         if (ctx.isOriginLocal()) {
            gmuExecutor.checkForReadyTasks();
         }
      }
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      Object retVal = super.visitGetKeyValueCommand(ctx, command);
      updateTransactionVersion(ctx, command);
      return retVal;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      final boolean previousAccessed = ctx.getLookedUpEntries().containsKey(command.getKey());
      Object retVal = super.visitPutKeyValueCommand(ctx, command);
      checkWriteCommand(ctx, previousAccessed, command);
      updateTransactionVersion(ctx, command);
      return retVal;
   }

   /*
    * NOTE: these are the only commands that passes values to the application and these keys needs to be validated
    * and added to the transaction read set.
    */

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      final boolean previousAccessed = ctx.getLookedUpEntries().containsKey(command.getKey());
      Object retVal = super.visitRemoveCommand(ctx, command);
      checkWriteCommand(ctx, previousAccessed, command);
      updateTransactionVersion(ctx, command);
      return retVal;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      ctx.clearKeyReadInCommand();
      final boolean previousAccessed = ctx.getLookedUpEntries().containsKey(command.getKey());
      Object retVal = super.visitReplaceCommand(ctx, command);
      checkWriteCommand(ctx, previousAccessed, command);
      updateTransactionVersion(ctx, command);
      return retVal;
   }

   @ManagedAttribute(description = "Returns the commit queue size.",
                     displayName = "Commit Queue Size")
   public final int getCommitQueueSize() {
      return transactionCommitManager.size();
   }

   @ManagedAttribute(description = "Returns the number of tasks (Commit or Remote gets) enqueued",
                     displayName = "Executor Queue Size")
   public final int getExecutorQueueSize() {
      return gmuExecutor.size();
   }

   @ManagedOperation(description = "Prints the commit queue state",
                     displayName = "Commit Queue")
   public final List<String> printCommitQueue() {
      return transactionCommitManager.printQueue();
   }

   @ManagedOperation(description = "Prints the executor service queue state",
                     displayName = "Executor Service Queue")
   public final List<String> printExecutorQueue() {
      return gmuExecutor.printQueue();
   }

   @Override
   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, boolean skipOwnershipCheck) {
      if (ctx.isInTxScope() && !entry.isLoaded()) {
         cdl.commitEntry(entry, ((TxInvocationContext) ctx).getTransactionVersion(), skipOwnershipCheck, ctx);
      } else {
         cdl.commitEntry(entry, entry.getVersion(), skipOwnershipCheck, ctx);
      }
   }

   /**
    * validates the read set and returns the prepare version from the commit queue
    *
    * @param ctx     the context
    * @param command the prepare command
    * @throws InterruptedException if interrupted
    */
   protected void performValidation(TxInvocationContext ctx, GMUPrepareCommand command) throws InterruptedException {
      boolean hasToUpdateLocalKeys = hasLocalKeysToUpdate(command.getModifications());
      CacheTransaction cacheTx = ctx.getCacheTransaction();
      boolean isReadOnly;
      if (cacheTx instanceof LocalTransaction) {
         LocalTransaction localTx = (LocalTransaction) cacheTx;
         isReadOnly = command.getModifications().length == 0 && (localTx.getRemoteDEFs() == null || !localTx.wroteInRemoteDEF());   
      } else {
         isReadOnly = command.getModifications().length == 0;
      }
      

      for (Object key : command.getAffectedKeys()) {
         if (cdl.localNodeIsOwner(key)) {
            hasToUpdateLocalKeys = true;
            break;
         }
      }

      if (!hasToUpdateLocalKeys) {
         for (WriteCommand writeCommand : command.getModifications()) {
            if (writeCommand instanceof ClearCommand) {
               hasToUpdateLocalKeys = true;
               break;
            }
         }
      }

      if (!isReadOnly) {
         GMUVersion transactionVersion = toGMUVersion(command.getPrepareVersion());
         List<Address> addressList = fromAlreadyReadFromMask(command.getAlreadyReadFrom(), versionGenerator,
                                                             transactionVersion.getViewId());
         GMUVersion maxGMUVersion = versionGenerator.calculateMaxVersionToRead(transactionVersion, addressList);
         cdl.performReadSetValidation(ctx, command, commitLog.getAvailableVersionLessThan(maxGMUVersion));
         if (hasToUpdateLocalKeys) {
            transactionCommitManager.prepareTransaction(ctx.getCacheTransaction());
         } else {
            transactionCommitManager.prepareReadOnlyTransaction(ctx.getCacheTransaction());
         }
      }

      if (log.isDebugEnabled()) {
         log.debugf("Transaction %s can commit on this node. Prepare Version is %s",
                    command.getGlobalTransaction().globalId(), ctx.getTransactionVersion());
      }
   }

   private void awaitCommitCommandAcksIfNeeded(TxInvocationContext ctx, Object retVal) throws Exception {
      if (ctx.isOriginLocal() && cacheConfiguration.transaction().syncCommitPhase() &&
            !ctx.getCacheTransaction().getAllModifications().isEmpty()) {
         //if no modifications, than it makes no sense to wait.
         if (retVal instanceof ResponseFuture) {
            Map<Address, Response> map = ((ResponseFuture) retVal).get();
            if (log.isTraceEnabled()) {
               log.tracef("Commit response map is " + map);
            }
         } else {
            throw new IllegalStateException("Synchronous Commit Phase didn't return a ResponseFuture");
         }
      }
   }

   private Object invokeNextIgnoringTimeout(TxInvocationContext context, CommitCommand commitCommand) throws Throwable {
      try {
         return invokeNextInterceptor(context, commitCommand);
      } catch (TimeoutException timeout) {
         //ignored
         return null;
      }
   }

   private void checkWriteCommand(InvocationContext context, boolean previousAccessed, WriteCommand command) {
      if (previousAccessed || command.isConditional()) {
         //if previous accessed, we have nothing to update in transaction version
         //if conditional, it is forced to read the key
         return;
      }
      if (command.hasFlag(Flag.IGNORE_RETURN_VALUES)) {
         context.getKeysReadInCommand().clear(); //remove all the keys read!
      }
   }

   private void store(GlobalTransaction globalTransaction) {
      if (store == null) {
         return;
      }
      Transaction tx = safeSuspend();
      try {
         store.commit(globalTransaction);
      } catch (CacheLoaderException e) {
         //ignored
      } finally {
         safeResume(tx);
      }
   }

   private Transaction safeSuspend() {
      if (transactionManager != null) {
         try {
            return transactionManager.suspend();
         } catch (Exception e) {
            //ignored
         }
      }
      return null;
   }

   private void safeResume(Transaction tx) {
      if (transactionManager != null && tx != null) {
         try {
            transactionManager.resume(tx);
         } catch (Exception e) {
            //ignored
         }
      }
   }

   private void updateTransactionVersion(InvocationContext context, AbstractFlagAffectedCommand command) {
      if (!context.isInTxScope() && !context.isOriginLocal()) {
         return;
      }

      if (context instanceof SingleKeyNonTxInvocationContext) {
         if (log.isDebugEnabled()) {
            log.debugf("Received a SingleKeyNonTxInvocationContext... This should be a single read operation");
         }
         return;
      }

      TxInvocationContext txInvocationContext = (TxInvocationContext) context;
      List<EntryVersion> entryVersionList = new LinkedList<EntryVersion>();
      entryVersionList.add(txInvocationContext.getTransactionVersion());

      if (log.isTraceEnabled()) {
         log.tracef("[%s] Keys read in this command: %s", txInvocationContext.getGlobalTransaction().globalId(),
                    txInvocationContext.getKeysReadInCommand());
      }

      for (InternalGMUCacheEntry internalGMUCacheEntry : txInvocationContext.getKeysReadInCommand().values()) {
         if (!command.hasFlag(Flag.READ_WITHOUT_REGISTERING) &&  
        	 txInvocationContext.hasModifications() && !internalGMUCacheEntry.isMostRecent()) {
            throw new CacheException("Read-Write transaction read an old value and should rollback");
         }

         if (internalGMUCacheEntry.getMaximumTransactionVersion() != null) {
            entryVersionList.add(internalGMUCacheEntry.getMaximumTransactionVersion());
         }
         if (!command.hasFlag(Flag.READ_WITHOUT_REGISTERING)) {
            txInvocationContext.getCacheTransaction().addReadKey(internalGMUCacheEntry.getKey());
         }
         if (cdl.localNodeIsOwner(internalGMUCacheEntry.getKey())) {
            txInvocationContext.setAlreadyReadOnThisNode(true);
            txInvocationContext.addReadFrom(cdl.getAddress());
         }
      }

      if (entryVersionList.size() > 1) {
         EntryVersion[] txVersionArray = new EntryVersion[entryVersionList.size()];
         txInvocationContext.setTransactionVersion(versionGenerator.mergeAndMax(entryVersionList.toArray(txVersionArray)));
      }
   }

   private boolean hasLocalKeysToUpdate(WriteCommand[] modifications) {
      for (WriteCommand writeCommand : modifications) {
         if (writeCommand instanceof ClearCommand) {
            return true;
         } else if (writeCommand instanceof ApplyDeltaCommand) {
            if (cdl.localNodeIsOwner(((ApplyDeltaCommand) writeCommand).getKey())) {
               return true;
            }
         } else {
            for (Object key : writeCommand.getAffectedKeys()) {
               if (cdl.localNodeIsOwner(key)) {
                  return true;
               }
            }
         }
      }
      return false;
   }

   private void updateCommitVersion(TxInvocationContext context, CacheTransaction cacheTransaction, int subVersion) {
      GMUCacheEntryVersion newVersion = versionGenerator.convertVersionToWrite(cacheTransaction.getTransactionVersion(),
                                                                               subVersion);
      context.getCacheTransaction().setTransactionVersion(newVersion);
   }

   private TxInvocationContext createInvocationContext(CacheTransaction cacheTransaction, int subVersion) {
      GMUCacheEntryVersion cacheEntryVersion = versionGenerator.convertVersionToWrite(cacheTransaction.getTransactionVersion(),
                                                                                      subVersion);
      cacheTransaction.setTransactionVersion(cacheEntryVersion);
      if (cacheTransaction instanceof LocalTransaction) {
         LocalTxInvocationContext localTxInvocationContext = invocationContextContainer.createTxInvocationContext();
         localTxInvocationContext.setLocalTransaction((LocalTransaction) cacheTransaction);
         return localTxInvocationContext;
      } else if (cacheTransaction instanceof RemoteTransaction) {
         return invocationContextContainer.createRemoteTxInvocationContext((RemoteTransaction) cacheTransaction, null);
      }
      throw new IllegalStateException("Expected a remote or local transaction and not " + cacheTransaction);
   }

   private void updateWaitingTime(TransactionEntry transactionEntry) {
      if (!TransactionsStatisticsRegistry.isGmuWaitingActive() || transactionEntry == null || !transactionEntry.hasWaited()) {
         return;
      }
      long commitTimestamp = transactionEntry.getCommitReceivedTimestamp();
      long firstInQueueTimeStamp = transactionEntry.getFirstInQueueTimestamp();
      long readyToCommitTimestamp = transactionEntry.getReadyToCommitTimestamp();
      boolean pendingTx = transactionEntry.isWaitBecauseOfPendingTx();
      if (log.isTraceEnabled()) {
         log.tracef("Updating statistics for tx %s. Commit=%s, Queue head=%s, Ready to commit=%s, Pending Tx=%s",
                    transactionEntry.getGlobalTransaction().globalId(),
                    commitTimestamp, firstInQueueTimeStamp, readyToCommitTimestamp, pendingTx);
      }
      if (commitTimestamp == -1 || firstInQueueTimeStamp == -1) {
         log.errorf("Commit Timestamp or Queue Head Timestamp cannot be -1");
         return;
      }
      TransactionStatistics transactionStatistics = TransactionsStatisticsRegistry.getTransactionStatistics();
      if (transactionStatistics != null) {
         if (pendingTx) {
            transactionStatistics.addValue(GMU_WAITING_IN_QUEUE_DUE_PENDING, firstInQueueTimeStamp - commitTimestamp);
            transactionStatistics.incrementValue(NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING);
         } else {
            transactionStatistics.addValue(GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS, firstInQueueTimeStamp - commitTimestamp);
            transactionStatistics.incrementValue(NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS);
         }
         if (readyToCommitTimestamp > firstInQueueTimeStamp) {
            transactionStatistics.addValue(GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION, readyToCommitTimestamp - firstInQueueTimeStamp);
            transactionStatistics.incrementValue(NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION);
         }
      }
   }

}
