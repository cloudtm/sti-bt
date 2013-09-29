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
package org.infinispan.distribution.wrappers;

import org.infinispan.commands.SetTransactionClassCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.stats.ExposedStatistic;
import org.infinispan.stats.LockRelatedStatsHelper;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.container.TransactionStatistics;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.transaction.gmu.ValidationException;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.reflect.Field;
import java.util.Collection;

import static org.infinispan.stats.ExposedStatistic.*;

/**
 * Massive hack for a noble cause!
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "ExtendedStatistics", description = "Component that manages and exposes extended statistics " +
      "relevant to transactions.")
public final class CustomStatsInterceptor extends BaseCustomInterceptor {
   //TODO what about the transaction implicit vs transaction explicit? should we take in account this and ignore
   //the implicit stuff?

   private final Log log = LogFactory.getLog(getClass());
   private TransactionTable transactionTable;
   private Configuration configuration;
   private RpcManager rpcManager;
   private DistributionManager distributionManager;

   @Inject
   public void inject(TransactionTable transactionTable, Configuration config, DistributionManager distributionManager) {
      this.transactionTable = transactionTable;
      this.configuration = config;
      this.distributionManager = distributionManager;
   }

   @Start(priority = 99)
   public void start() {
      // we want that this method is the last to be invoked, otherwise the start method is not invoked
      // in the real components
      replace();
      log.info("Initializing the TransactionStatisticsRegistry");
      TransactionsStatisticsRegistry.init(this.configuration);
      if (configuration.versioning().scheme().equals(VersioningScheme.GMU))
         LockRelatedStatsHelper.enable();
   }

   @Override
   public Object visitSetTransactionClassCommand(InvocationContext ctx, SetTransactionClassCommand command) throws Throwable {
      final String transactionClass = command.getTransactionalClass();

      invokeNextInterceptor(ctx, command);
      if (ctx.isInTxScope() && transactionClass != null && !transactionClass.isEmpty()) {
         final GlobalTransaction globalTransaction = ((TxInvocationContext) ctx).getGlobalTransaction();
         if (log.isTraceEnabled()) {
            log.tracef("Setting the transaction class %s for %s", transactionClass, globalTransaction);
         }
         final TransactionStatistics txs = initStatsIfNecessary(ctx);
         TransactionsStatisticsRegistry.setTransactionalClass(transactionClass, txs);
         globalTransaction.setTransactionClass(command.getTransactionalClass());
         return Boolean.TRUE;
      } else if (log.isTraceEnabled()) {
         log.tracef("Did not set transaction class %s. It is not inside a transaction or the transaction class is null",
                    transactionClass);
      }
      return Boolean.FALSE;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Put Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                    ctx.isInTxScope(), ctx.isOriginLocal());
      }
      Object ret;
      if (TransactionsStatisticsRegistry.isActive() && ctx.isInTxScope()) {
         final TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
         transactionStatistics.setUpdateTransaction();
         long initTime = System.nanoTime();
         transactionStatistics.incrementValue(NUM_PUT);
         transactionStatistics.addNTBCValue(initTime);
         try {
            ret = invokeNextInterceptor(ctx, command);
         } catch (TimeoutException e) {
            if (ctx.isOriginLocal()) {
               transactionStatistics.incrementValue(NUM_LOCK_FAILED_TIMEOUT);
            }
            throw e;
         } catch (DeadlockDetectedException e) {
            if (ctx.isOriginLocal()) {
               transactionStatistics.incrementValue(NUM_LOCK_FAILED_DEADLOCK);
            }
            throw e;
         } catch (WriteSkewException e) {
            if (ctx.isOriginLocal()) {
               transactionStatistics.incrementValue(NUM_WRITE_SKEW);
            }
            throw e;
         }
         if (isRemote(command.getKey())) {
            transactionStatistics.addValue(REMOTE_PUT_EXECUTION, System.nanoTime() - initTime);
            transactionStatistics.incrementValue(NUM_REMOTE_PUT);
         }
         transactionStatistics.setLastOpTimestamp(System.nanoTime());
         return ret;
      } else
         return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Get Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                    ctx.isInTxScope(), ctx.isOriginLocal());
      }
      Object ret;
      if (TransactionsStatisticsRegistry.isActive() && ctx.isInTxScope()) {

         final TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
         //transactionStatistics.addNTBCValue(currTimeForAllGetCommand);         
         transactionStatistics.notifyRead();
         long currCpuTime = 0;
         boolean isRemoteKey = isRemote(command.getKey());
         if (isRemoteKey) {
            currCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
         }
         long currTimeForAllGetCommand = System.nanoTime();
         ret = invokeNextInterceptor(ctx, command);
         long lastTimeOp = System.nanoTime();
         if (isRemoteKey) {
            transactionStatistics.incrementValue(NUM_LOCAL_REMOTE_GET);
            transactionStatistics.addValue(LOCAL_REMOTE_GET_R, lastTimeOp - currTimeForAllGetCommand);
            if (TransactionsStatisticsRegistry.isSampleServiceTime())
               transactionStatistics.addValue(LOCAL_REMOTE_GET_S, TransactionsStatisticsRegistry.getThreadCPUTime() - currCpuTime);
         }
         transactionStatistics.addValue(ALL_GET_EXECUTION, lastTimeOp - currTimeForAllGetCommand);
         transactionStatistics.incrementValue(NUM_GET);
         transactionStatistics.setLastOpTimestamp(lastTimeOp);
      } else {
         ret = invokeNextInterceptor(ctx, command);
      }
      return ret;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Commit command %s. Is it local?. Transaction is %s", command,
                    ctx.isOriginLocal(), command.getGlobalTransaction().globalId());
      }
      if (!TransactionsStatisticsRegistry.isActive()) {
         return invokeNextInterceptor(ctx, command);
      }

      TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
      long currCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
      long currTime = System.nanoTime();
      transactionStatistics.addNTBCValue(currTime);
      transactionStatistics.attachId(ctx.getGlobalTransaction());
      if (LockRelatedStatsHelper.shouldAppendLocks(configuration, true, !ctx.isOriginLocal())) {
         if (log.isTraceEnabled())
            log.trace("Appending locks for " + ((!ctx.isOriginLocal()) ? "remote " : "local ") + "transaction " + ctx.getGlobalTransaction().getId());
         TransactionsStatisticsRegistry.appendLocks();
      } else {
         if (log.isTraceEnabled())
            log.trace("Not appending locks for " + ((!ctx.isOriginLocal()) ? "remote " : "local ") + "transaction " + ctx.getGlobalTransaction().getId());
      }
      Object ret = invokeNextInterceptor(ctx, command);

      handleCommitCommand(transactionStatistics, currTime, currCpuTime, ctx);

      transactionStatistics.setTransactionOutcome(true);
      //We only terminate a local transaction, since RemoteTransactions are terminated from the InboundInvocationHandler
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction(transactionStatistics);
      }

      return ret;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Prepare command %s. Is it local?. Transaction is %s", command,
                    ctx.isOriginLocal(), command.getGlobalTransaction().globalId());
      }

      if (!TransactionsStatisticsRegistry.isActive()) {
         return invokeNextInterceptor(ctx, command);
      }

      TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
      transactionStatistics.onPrepareCommand();
      if (command.hasModifications()) {
         transactionStatistics.setUpdateTransaction();
      }


      boolean success = false;
      try {
         long currTime = System.nanoTime();
         long currCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
         Object ret = invokeNextInterceptor(ctx, command);
         success = true;
         handlePrepareCommand(transactionStatistics, currTime, currCpuTime, ctx, command);
         return ret;
      } catch (TimeoutException e) {
         if (ctx.isOriginLocal()) {
            transactionStatistics.incrementValue(NUM_LOCK_FAILED_TIMEOUT);
         }
         throw e;
      } catch (DeadlockDetectedException e) {
         if (ctx.isOriginLocal()) {
            transactionStatistics.incrementValue(NUM_LOCK_FAILED_DEADLOCK);
         }
         throw e;
      } catch (WriteSkewException e) {
         if (ctx.isOriginLocal()) {
            transactionStatistics.incrementValue(NUM_WRITE_SKEW);
         }
         throw e;
      } catch (ValidationException e) {
         if (ctx.isOriginLocal()) {
            transactionStatistics.incrementValue(NUM_ABORTED_TX_DUE_TO_VALIDATION);
         }
         //Increment the killed xact only if the murder has happened on this node (whether the xact is local or remote)
         transactionStatistics.incrementValue(NUM_KILLED_TX_DUE_TO_VALIDATION);
         throw e;

      }
      //We don't care about cacheException for earlyAbort

      //Remote exception we care about
      catch (Exception e) {
         if (ctx.isOriginLocal()) {
            if (e.getCause() instanceof TimeoutException) {
               transactionStatistics.incrementValue(NUM_LOCK_FAILED_TIMEOUT);
            } else if (e.getCause() instanceof ValidationException) {
               transactionStatistics.incrementValue(NUM_ABORTED_TX_DUE_TO_VALIDATION);

            }
         }

         throw e;
      } finally {
         if (command.isOnePhaseCommit()) {
            transactionStatistics.setTransactionOutcome(success);
            if (ctx.isOriginLocal()) {
               transactionStatistics.terminateTransaction();
            }
         }
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Rollback command %s. Is it local?. Transaction is %s", command,
                    ctx.isOriginLocal(), command.getGlobalTransaction().globalId());
      }

      if (!TransactionsStatisticsRegistry.isActive()) {
         return invokeNextInterceptor(ctx, command);
      }

      TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
      long currentCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
      long initRollbackTime = System.nanoTime();
      Object ret = invokeNextInterceptor(ctx, command);
      transactionStatistics.setTransactionOutcome(false);

      handleRollbackCommand(transactionStatistics, initRollbackTime, currentCpuTime, ctx);
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction(transactionStatistics);
      }

      return ret;
   }

   @ManagedAttribute(description = "Average number of puts performed by a successful local transaction",
                     displayName = "Number of puts per successful local transaction")
   public double getAvgNumPutsBySuccessfulLocalTx() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(PUTS_PER_LOCAL_TX));
   }

   @ManagedAttribute(description = "Average Prepare Round-Trip Time duration (in microseconds)",
                     displayName = "Average Prepare RTT")
   public long getAvgPrepareRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_PREPARE))));
   }

   @ManagedAttribute(description = "Average Commit Round-Trip Time duration (in microseconds)",
                     displayName = "Average Commit RTT")
   public long getAvgCommitRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(RTT_COMMIT)));
   }

   @ManagedAttribute(description = "Average Remote Get Round-Trip Time duration (in microseconds)",
                     displayName = "Average Remote Get RTT")
   public long getAvgRemoteGetRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_GET))));
   }

   @ManagedAttribute(description = "Average Rollback Round-Trip Time duration (in microseconds)",
                     displayName = "Average Rollback RTT")
   public long getAvgRollbackRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_ROLLBACK))));
   }

   @ManagedAttribute(description = "Average asynchronous Prepare duration (in microseconds)",
                     displayName = "Average Prepare Async")
   public long getAvgPrepareAsync() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((ASYNC_PREPARE))));
   }

   @ManagedAttribute(description = "Average asynchronous Commit duration (in microseconds)",
                     displayName = "Average Commit Async")
   public long getAvgCommitAsync() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((ASYNC_COMMIT))));
   }

   @ManagedAttribute(description = "Average asynchronous Complete Notification duration (in microseconds)",
                     displayName = "Average Complete Notification Async")
   public long getAvgCompleteNotificationAsync() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((ASYNC_COMPLETE_NOTIFY))));
   }

   @ManagedAttribute(description = "Average asynchronous Rollback duration (in microseconds)",
                     displayName = "Average Rollback Async")
   public long getAvgRollbackAsync() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((ASYNC_ROLLBACK))));
   }

   @ManagedAttribute(description = "Average number of nodes in Commit destination set",
                     displayName = "Average Number of Nodes in Commit Destination Set")
   public long getAvgNumNodesCommit() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_COMMIT))));
   }

   @ManagedAttribute(description = "Average number of nodes of async commit messages sent per xact",
                     displayName = "Average number of nodes of async commit messages sent per xact")
   public long getAvgNumAsyncSentCommitMsgs() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((SENT_ASYNC_COMMIT))));
   }

   @ManagedAttribute(description = "Average number of nodes of Sync commit messages sent per xact",
                     displayName = "Average number of nodes of Sync commit messages sent per xact")
   public long getAvgNumSyncSentCommitMsgs() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((SENT_SYNC_COMMIT))));
   }

   @ManagedAttribute(description = "Average number of nodes in Complete Notification destination set",
                     displayName = "Average Number of Nodes in Complete Notification Destination Set")
   public long getAvgNumNodesCompleteNotification() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_COMPLETE_NOTIFY))));
   }

   @ManagedAttribute(description = "Average number of nodes in Remote Get destination set",
                     displayName = "Average Number of Nodes in Remote Get Destination Set")
   public long getAvgNumNodesRemoteGet() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_GET))));
   }

   @ManagedAttribute(description = "Average number of nodes in Prepare destination set",
                     displayName = "Average Number of Nodes in Prepare Destination Set")
   public long getAvgNumNodesPrepare() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_PREPARE))));
   }

   @ManagedAttribute(description = "Average number of nodes in Rollback destination set",
                     displayName = "Average Number of Nodes in Rollback Destination Set")
   public long getAvgNumNodesRollback() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_ROLLBACK))));
   }

   @ManagedAttribute(description = "Application Contention Factor",
                     displayName = "Application Contention Factor")
   public double getApplicationContentionFactor() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((APPLICATION_CONTENTION_FACTOR)));
   }

   @Deprecated
   @ManagedAttribute(description = "Local Contention Probability",
                     displayName = "Local Conflict Probability")
   public double getLocalContentionProbability() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((LOCAL_CONTENTION_PROBABILITY)));
   }

   @Deprecated
   @ManagedAttribute(description = "Remote Contention Probability",
                     displayName = "Remote Conflict Probability")
   public double getRemoteContentionProbability() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((REMOTE_CONTENTION_PROBABILITY)));
   }

   @ManagedAttribute(description = "Lock Contention Probability",
                     displayName = "Lock Contention Probability")
   public double getLockContentionProbability() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((LOCK_CONTENTION_PROBABILITY)));
   }

   @ManagedAttribute(description = "Avg lock holding time (in microseconds)", displayName = "Avg lock holding time (in microseconds)")
   public long getAvgLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_HOLD_TIME));
   }

   @ManagedAttribute(description = "Average lock local holding time (in microseconds)",
                     displayName = "Average Lock Local Holding Time")
   public long getAvgLocalLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_HOLD_TIME_LOCAL));
   }

   @ManagedAttribute(description = "Average lock local holding time of committed xacts (in microseconds)",
                     displayName = "Average lock local holding time of committed xacts")
   public long getAvgLocalSuccessfulLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(SUX_LOCK_HOLD_TIME));
   }

   @ManagedAttribute(description = "Average lock local holding time of locally aborted xacts (in microseconds)",
                     displayName = "Average lock local holding time of locally aborted xacts")
   public long getAvgLocalLocalAbortLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_ABORT_LOCK_HOLD_TIME));
   }

   @ManagedAttribute(description = "Average lock local holding time of remotely aborted xacts (in microseconds)",
                     displayName = "Average lock local holding time of remotely aborted xacts (in microseconds)")
   public long getAvgLocalRemoteAbortLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_ABORT_LOCK_HOLD_TIME));
   }

   @ManagedAttribute(description = "Average lock remote holding time (in microseconds)",
                     displayName = "Average Lock Remote Holding Time")
   public long getAvgRemoteLockHoldTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_HOLD_TIME_REMOTE));
   }

   @ManagedAttribute(description = "Avg prepare command size (in bytes)", displayName = "Avg prepare command size (in bytes)")
   public long getAvgPrepareCommandSize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(PREPARE_COMMAND_SIZE));
   }

   @ManagedAttribute(description = "Avg rollback command size (in bytes)", displayName = "Avg rollback command size (in bytes)")
   public long getAvgRollbackCommandSize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(ROLLBACK_COMMAND_SIZE));
   }

   @ManagedAttribute(description = "Avg size of a remote get reply (in bytes)", displayName = "Avg size of a remote get reply (in bytes)")
   public long getAvgClusteredGetCommandReplySize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_REMOTE_GET_REPLY_SIZE));
   }

   @ManagedAttribute(description = "Average commit command size (in bytes)",
                     displayName = "Average Commit Command Size")
   public long getAvgCommitCommandSize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(COMMIT_COMMAND_SIZE));
   }

   @ManagedAttribute(description = "Average clustered get command size (in bytes)",
                     displayName = "Average Clustered Get Command Size")
   public long getAvgClusteredGetCommandSize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(CLUSTERED_GET_COMMAND_SIZE));
   }

   @ManagedAttribute(description = "Average time waiting for the lock acquisition (in microseconds)",
                     displayName = "Average Lock Waiting Time")
   public long getAvgLockWaitingTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_WAITING_TIME));
   }

   @ManagedAttribute(description = "Average transaction arrival rate, originated locally and remotely (in transaction " +
         "per second)",
                     displayName = "Average Transaction Arrival Rate")
   public double getAvgTxArrivalRate() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(ARRIVAL_RATE));
   }

   @ManagedAttribute(description = "Percentage of Write transaction executed locally (committed and aborted)",
                     displayName = "Percentage of Write Transactions")
   public double getPercentageWriteTransactions() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(TX_WRITE_PERCENTAGE));
   }

   @ManagedAttribute(description = "Percentage of Write transaction executed in all successfully executed " +
         "transactions (local transaction only)",
                     displayName = "Percentage of Successfully Write Transactions")
   public double getPercentageSuccessWriteTransactions() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(SUCCESSFUL_WRITE_PERCENTAGE));
   }

   @ManagedAttribute(description = "The numberh of ndlePreborted transactions due to deadlock",
                     displayName = "Number of Aborted Transaction due to Deadlock")
   public long getNumAbortedTxDueDeadlock() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_FAILED_DEADLOCK));
   }

   @ManagedAttribute(description = "Average successful read-only transaction duration (in microseconds)",
                     displayName = "Average Read-Only Transaction Duration")
   public long getAvgReadOnlyTxDuration() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(RO_TX_SUCCESSFUL_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average successful write transaction duration (in microseconds)",
                     displayName = "Average Write Transaction Duration")
   public long getAvgWriteTxDuration() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WR_TX_SUCCESSFUL_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average aborted write transaction duration (in microseconds)",
                     displayName = "Average Aborted Write Transaction Duration")
   public long getAvgAbortedWriteTxDuration() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WR_TX_ABORTED_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Average number of locks per write local transaction",
                     displayName = "Average Number of Lock per Local Transaction")
   public long getAvgNumOfLockLocalTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_PER_LOCAL_TX));
   }

   @ManagedAttribute(description = "Average number of locks per write remote transaction",
                     displayName = "Average Number of Lock per Remote Transaction")
   public long getAvgNumOfLockRemoteTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_PER_REMOTE_TX));
   }

   @ManagedAttribute(description = "Average number of locks per successfully write local transaction",
                     displayName = "Average Number of Lock per Successfully Local Transaction")
   public long getAvgNumOfLockSuccessLocalTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_PER_SUCCESS_LOCAL_TX));
   }

   @ManagedAttribute(description = "Avg time it takes to execute the rollback command remotely (in microseconds)", displayName = "Avg time it takes to execute the rollback command remotely (in microseconds)")
   public long getAvgRemoteTxCompleteNotifyTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TX_COMPLETE_NOTIFY_EXECUTION_TIME));
   }

   @ManagedAttribute(description = "Abort Rate",
                     displayName = "Abort Rate")
   public double getAbortRate() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(ABORT_RATE));
   }

   @ManagedAttribute(description = "Throughput (in transactions per second)",
                     displayName = "Throughput")
   public double getThroughput() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(THROUGHPUT));
   }

   @ManagedAttribute(description = "Number of xact dead in remote prepare",
                     displayName = "Number of xact dead in remote prepare")
   public double getRemotelyDeadXact() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTELY_ABORTED));
   }

   @ManagedAttribute(description = "Number of early aborts",
                     displayName = "Number of early aborts")
   public double getNumEarlyAborts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_EARLY_ABORTS));
   }

   @ManagedAttribute(description = "Number of xact dead while preparing locally",
                     displayName = "Number of xact dead while preparing locally")
   public double getNumLocalPrepareAborts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCALPREPARE_ABORTS));
   }

   @ManagedAttribute(description = "Number of update xacts gone up to prepare phase",
                     displayName = "Number of update xacts gone up to prepare phase")
   public double getUpdateXactToPrepare() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_UPDATE_TX_GOT_TO_PREPARE));
   }

   @ManagedAttribute(description = "Average number of get operations per local read transaction",
                     displayName = "Average number of get operations per local read transaction")
   public long getAvgGetsPerROTransaction() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_GETS_RO_TX));
   }

   @ManagedAttribute(description = "Average number of get operations per local write transaction",
                     displayName = "Average number of get operations per local write transaction")
   public long getAvgGetsPerWrTransaction() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_GETS_WR_TX));
   }

   @ManagedAttribute(description = "Average number of remote get operations per local write transaction",
                     displayName = "Average number of remote get operations per local write transaction")
   public double getAvgRemoteGetsPerWrTransaction() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_GETS_WR_TX));
   }

   @ManagedAttribute(description = "Average number of remote get operations per local read transaction",
                     displayName = "Average number of remote get operations per local read transaction")
   public double getAvgRemoteGetsPerROTransaction() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_GETS_RO_TX));
   }

   @ManagedAttribute(description = "Average cost of a remote get",
                     displayName = "Remote get cost")
   public long getRemoteGetResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_REMOTE_GET_R));
   }

   @ManagedAttribute(description = "Average number of put operations per local write transaction",
                     displayName = "Average number of put operations per local write transaction")
   public double getAvgPutsPerWrTransaction() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_PUTS_WR_TX));
   }

   @ManagedAttribute(description = "Average number of remote put operations per local write transaction",
                     displayName = "Average number of remote put operations per local write transaction")
   public double getAvgRemotePutsPerWrTransaction() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX));
   }

   @ManagedAttribute(description = "Average cost of a remote put",
                     displayName = "Remote put cost")
   public long getRemotePutExecutionTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_PUT_EXECUTION));
   }

   @ManagedAttribute(description = "Number of gets performed since last reset",
                     displayName = "Number of Gets")
   public long getNumberOfGets() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GET));
   }

   @ManagedAttribute(description = "Number of remote gets performed since last reset",
                     displayName = "Number of Remote Gets")
   public long getNumberOfRemoteGets() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCAL_REMOTE_GET));
   }

   @ManagedAttribute(description = "Number of puts performed since last reset",
                     displayName = "Number of Puts")
   public long getNumberOfPuts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_PUT));
   }

   @ManagedAttribute(description = "Number of remote puts performed since last reset",
                     displayName = "Number of Remote Puts")
   public long getNumberOfRemotePuts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTE_PUT));
   }

   @ManagedAttribute(description = "Number of committed transactions since last reset",
                     displayName = "Number Of Commits")
   public long getNumberOfCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_COMMITS));
   }

   @ManagedAttribute(description = "Number of local committed transactions since last reset",
                     displayName = "Number Of Local Commits")
   public long getNumberOfLocalCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCAL_COMMITS));
   }

   @ManagedAttribute(description = "Write skew probability",
                     displayName = "Write Skew Probability")
   public double getWriteSkewProbability() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(WRITE_SKEW_PROBABILITY));
   }

   @ManagedOperation(description = "Reset all the statistics collected",
                     displayName = "Reset All Statistics")
   public void resetStatistics() {
      TransactionsStatisticsRegistry.reset();
   }

   @ManagedAttribute(description = "Average Local processing Get time (in microseconds)",
                     displayName = "Average Local Get time")
   public long getAvgLocalGetTime() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((LOCAL_GET_EXECUTION))));
   }

   @ManagedAttribute(description = "Average TCB time (in microseconds)",
                     displayName = "Average TCB time")
   public long getAvgTCBTime() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((TBC))));
   }

   @ManagedAttribute(description = "Average NTCB time (in microseconds)",
                     displayName = "Average NTCB time")
   public long getAvgNTCBTime() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NTBC))));
   }

   @ManagedAttribute(description = "Number of nodes in the cluster",
                     displayName = "Number of nodes")
   public long getNumNodes() {
      try {
         if (rpcManager != null) {
            Collection<Address> members = rpcManager.getMembers();
            //member can be null if we haven't received the initial topology
            return members == null ? 1 : members.size();
         }
         //if rpc manager is null, we are in local mode.
         return 1;
      } catch (Throwable throwable) {
         log.error("Error obtaining Number of Nodes. returning 1", throwable);
         return 1;
      }
   }

   @ManagedAttribute(description = "Number of replicas for each key",
                     displayName = "Replication Degree")
   public long getReplicationDegree() {
      try {
         return distributionManager == null ? 1 : distributionManager.getConsistentHash().getNumOwners();
      } catch (Throwable throwable) {
         log.error("Error obtaining Replication Degree. returning 0", throwable);
         return 0;
      }
   }

   @ManagedAttribute(description = "Cost of terminating a xact (debug only)",
                     displayName = "Cost of terminating a xact (debug only)")
   public long getTerminationCost() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TERMINATION_COST));
   }

   @ManagedAttribute(description = "Number of concurrent transactions executing on the current node",
                     displayName = "Local Active Transactions")
   public long getLocalActiveTransactions() {
      try {
         return transactionTable == null ? 0 : transactionTable.getLocalTxCount();
      } catch (Throwable throwable) {
         log.error("Error obtaining Local Active Transactions. returning 0", throwable);
         return 0;
      }
   }




    /*Local Update*/

   @ManagedAttribute(description = "Avg CPU demand to execute a local xact up to the prepare phase", displayName = "Avg CPU demand to execute a local xact up to the prepare phase")
   public long getLocalUpdateTxLocalServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_S));
   }

   @ManagedAttribute(description = "Avg response time of the local part of a xact up to the prepare phase", displayName = "Avg response time of the local part of a xact up to the prepare phase")
   public long getLocalUpdateTxLocalResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_R));
   }

   @ManagedAttribute(description = "Avg response time of a local PrepareCommand", displayName = "Avg response time of a local PrepareCommand")
   public long getLocalUpdateTxPrepareResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_PREPARE_R));
   }

   @ManagedAttribute(description = "Avg CPU demand to execute a local PrepareCommand", displayName = "Avg CPU demand to execute a local PrepareCommand")
   public long getLocalUpdateTxPrepareServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_PREPARE_S));
   }

   @ManagedAttribute(description = "Avg CPU demand to execute a local PrepareCommand", displayName = "Avg CPU demand to execute a local PrepareCommand")
   public long getLocalUpdateTxCommitServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_COMMIT_S));
   }

   @ManagedAttribute(description = "Avg response time of a local CommitCommand", displayName = "Avg response time of a local CommitCommand")
   public long getLocalUpdateTxCommitResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_COMMIT_R));
   }

   @ManagedAttribute(description = "Avg CPU demand to execute a local RollbackCommand without remote nodes", displayName = "Avg CPU demand to execute a local RollbackCommand without remote nodes")
   public long getLocalUpdateTxLocalRollbackServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_LOCAL_ROLLBACK_S));
   }

   @ManagedAttribute(description = "Avg response time to execute a remote RollbackCommand", displayName = "Avg response time to execute a remote RollbackCommand")
   public long getLocalUpdateTxLocalRollbackResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_LOCAL_ROLLBACK_R));
   }

   @ManagedAttribute(description = "Avg CPU demand to execute a local RollbackCommand with remote nodes", displayName = "Avg CPU demand to execute a local RollbackCommand with remote nodes")
   public long getLocalUpdateTxRemoteRollbackServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_REMOTE_ROLLBACK_S));
   }

   @ManagedAttribute(description = "Avg response time to execute a local RollbackCommand with remote nodes", displayName = "Avg response time to execute a local RollbackCommand with remote nodes")
   public long getLocalUpdateTxRemoteRollbackResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_REMOTE_ROLLBACK_R));
   }


   /*Remote*/

   @ManagedAttribute(description = "Avg response time of the prepare of a remote xact", displayName = "Avg response time of the prepare of a remote xact")
   public long getRemoteUpdateTxPrepareResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_PREPARE_R));
   }

   @ManagedAttribute(description = "Avg CPU demand to execute the prepare of a remote xact", displayName = "Avg CPU demand to execute the prepare of a remote xact")
   public long getRemoteUpdateTxPrepareServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_PREPARE_S));
   }

   @ManagedAttribute(description = "Avg CPU demand to execute the commitCommand of a remote xact", displayName = "Avg CPU demand to execute the commitCommand of a remote xact")
   public long getRemoteUpdateTxCommitServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_COMMIT_S));
   }

   @ManagedAttribute(description = "Avg response time of the execution of the commitCommand of a remote xact", displayName = "Avg response time of the execution of the commitCommand of a remote xact")
   public long getRemoteUpdateTxCommitResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_COMMIT_R));
   }

   @ManagedAttribute(description = "Avg CPU demand to perform the rollback of a remote xact", displayName = "Avg CPU demand to perform the rollback of a remote xact")
   public long getRemoteUpdateTxRollbackServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_ROLLBACK_S));
   }

   @ManagedAttribute(description = "Avg response time of the rollback of a remote xact", displayName = "Avg response time of the rollback of a remote xact")
   public long getRemoteUpdateTxRollbackResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_REMOTE_ROLLBACK_R));
   }

   /* Read Only*/

   @ManagedAttribute(description = "Avg CPU demand to perform the local execution of a read only xact", displayName = "Avg CPU demand to perform the local execution of a read only xact")
   public long getLocalReadOnlyTxLocalServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_LOCAL_S));
   }

   @ManagedAttribute(description = "Avg response time of the local execution of a read only xact", displayName = "Avg response time of the local execution of a read only xact")
   public long getLocalReadOnlyTxLocalResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_LOCAL_R));
   }

   @ManagedAttribute(description = "Avg response time of the prepare of a read only xact", displayName = "Avg response time of the prepare of a read only xact")
   public long getLocalReadOnlyTxPrepareResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_PREPARE_R));
   }

   @ManagedAttribute(description = "Avg CPU demand to perform the prepare of a read only xact", displayName = "Avg CPU demand to perform the prepare of a read only xact")
   public long getLocalReadOnlyTxPrepareServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_PREPARE_S));
   }

   @ManagedAttribute(description = "Avg CPU demand to perform the commit of a read only xact", displayName = "Avg CPU demand to perform the commit of a read only xact")
   public long getLocalReadOnlyTxCommitServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_COMMIT_S));
   }

   @ManagedAttribute(description = "Avg response time of the commit of a read only xact", displayName = "Avg response time of the commit of a read only xact")
   public long getLocalReadOnlyTxCommitResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_COMMIT_R));
   }

   @ManagedAttribute(description = "Avg CPU demand to serve a remote get", displayName = "Avg CPU demand to serve a remote get")
   public long getRemoteGetServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_REMOTE_GET_S));
   }

   @ManagedAttribute(description = "Avg number of local items in a local prepareCommand", displayName = "Avg number or data items in a local prepareCommand to write")
   public double getNumOwnedRdItemsInLocalPrepare() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_OWNED_RD_ITEMS_IN_LOCAL_PREPARE));
   }

   @ManagedAttribute(description = "Avg number of local data items in a local prepareCommand to read-validate", displayName = "Avg number or data items in a local prepareCommand to read-validate")
   public double getNumOwnedWrItemsInLocalPrepare() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_OWNED_WR_ITEMS_IN_LOCAL_PREPARE));
   }

   @ManagedAttribute(description = "Avg number of local data items in a remote prepareCommand to read-validate", displayName = "Avg number or data items in a remote prepareCommand to read-validate")
   public double getNumOwnedRdItemsInRemotePrepare() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_OWNED_RD_ITEMS_IN_REMOTE_PREPARE));
   }

   @ManagedAttribute(description = "Avg number of local data items in a remote prepareCommand to write", displayName = "Avg number or data items in a remote prepareCommand to write")
   public double getNumOwnedWrItemsInRemotePrepare() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_OWNED_WR_ITEMS_IN_REMOTE_PREPARE));
   }

   @ManagedAttribute(description = "Avg waiting time before serving a ClusteredGetCommand", displayName = "Avg waiting time before serving a ClusteredGetCommand")
   public long getWaitedTimeInRemoteCommitQueue() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WAIT_TIME_IN_REMOTE_COMMIT_QUEUE));
   }

   @ManagedAttribute(displayName = "Avg time spent in the commit queue by a xact", description = "Avg time spent in the commit queue by a xact")
   public long getWaitedTimeInLocalCommitQueue() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WAIT_TIME_IN_COMMIT_QUEUE));
   }

   @ManagedAttribute(displayName = "The number of xacts killed on the node due to unsuccessful validation", description = "The number of xacts killed on the node due to unsuccessful validation")
   public long getNumKilledTxDueToValidation() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_KILLED_TX_DUE_TO_VALIDATION));
   }

   @ManagedAttribute(description = "The number of aborted xacts due to unsuccessful validation", displayName = "The number of aborted xacts due to unsuccessful validation")
   public long getNumAbortedTxDueToValidation() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_ABORTED_TX_DUE_TO_VALIDATION));
   }

   @ManagedAttribute(description = "The number of aborted xacts due to timeout in readlock acquisition", displayName = "The number of aborted xacts due to timeout in readlock acquisition")
   public long getNumAbortedTxDueToReadLock() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_READLOCK_FAILED_TIMEOUT));
   }

   @ManagedAttribute(displayName = "The number of aborted xacts due to timeout in lock acquisition", description = "The number of aborted xacts due to timeout in lock acquisition")
   public long getNumAbortedTxDueToWriteLock() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_FAILED_TIMEOUT));
   }

   @ManagedAttribute(displayName = "The number of aborted xacts due to stale read", description = "The number of aborted xacts due to stale read")
   public long getNumAbortedTxDueToNotLastValueAccessed() {
      return (long) getNumEarlyAborts();
   }

   @ManagedAttribute(displayName = "Number of reads before first write", description = "Number of reads before first writed")
   public double getNumReadsBeforeWrite() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(FIRST_WRITE_INDEX));
   }

   @ManagedAttribute(displayName = "Avg waiting time for a GMUClusteredGetCommand", description = "Avg waiting time for a GMUClusteredGetCommand")
   public double getGMUClusteredGetCommandWaitingTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_REMOTE_GET_WAITING_TIME));
   }

   @ManagedAttribute(displayName = "Avg response time for a GMUClusteredGetCommand (no queue)", description = "Avg response time for a GMUClusteredGetCommand (no queue)")
   public double getGMUClusteredGetCommandResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_REMOTE_GET_R));
   }

   @ManagedAttribute(displayName = "Avg cpu time for a GMUClusteredGetCommand", description = "Avg cpu time for a GMUClusteredGetCommand")
   public double getGMUClusteredGetCommandServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_REMOTE_GET_S));
   }

   @ManagedAttribute(displayName = "Avg CPU demand of a local update xact", description = "Avg CPU demand of a local update xact")
   public double getLocalUpdateTxTotalCpuTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_TOTAL_S));
   }

   @ManagedAttribute(displayName = "Avg response time of a local update xact", description = "Avg response time of a local update xact")
   public double getLocalUpdateTxTotalResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_TOTAL_R));
   }

   @ManagedAttribute(displayName = "Avg CPU demand of a read only xact", description = "Avg CPU demand of a read only xact")
   public double getReadOnlyTxTotalResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_TOTAL_R));
   }

   @ManagedAttribute(displayName = "Avg response time of a read only xact", description = "Avg response time of a read only xact")
   public double getReadOnlyTxTotalCpuTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_TOTAL_S));
   }

   @ManagedAttribute(description = "Avg Response Time", displayName = "Avg Response Time")
   public long getAvgResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(RESPONSE_TIME));
   }

   @ManagedOperation(description = "Average number of puts performed by a successful local transaction per class",
                     displayName = "Number of puts per successful local transaction per class")
   public double getAvgNumPutsBySuccessfulLocalTxParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(PUTS_PER_LOCAL_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average Prepare Round-Trip Time duration (in microseconds) per class",
                     displayName = "Average Prepare RTT per class")
   public long getAvgPrepareRttParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_PREPARE), transactionalClass)));
   }

   @ManagedOperation(description = "Average Commit Round-Trip Time duration (in microseconds) per class",
                     displayName = "Average Commit RTT per class")
   public long getAvgCommitRttParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_COMMIT), transactionalClass)));
   }

   @ManagedOperation(description = "Average Remote Get Round-Trip Time duration (in microseconds) per class",
                     displayName = "Average Remote Get RTT per class")
   public long getAvgRemoteGetRttParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_GET), transactionalClass)));
   }

   @ManagedOperation(description = "Average Rollback Round-Trip Time duration (in microseconds) per class",
                     displayName = "Average Rollback RTT per class")
   public long getAvgRollbackRttParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_ROLLBACK), transactionalClass)));
   }

   @ManagedOperation(description = "Average asynchronous Prepare duration (in microseconds) per class",
                     displayName = "Average Prepare Async per class")
   public long getAvgPrepareAsyncParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((ASYNC_PREPARE), transactionalClass)));
   }

   @ManagedOperation(description = "Average asynchronous Commit duration (in microseconds) per class",
                     displayName = "Average Commit Async per class")
   public long getAvgCommitAsyncParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((ASYNC_COMMIT), transactionalClass)));
   }

   @ManagedOperation(description = "Average asynchronous Complete Notification duration (in microseconds) per class",
                     displayName = "Average Complete Notification Async per class")
   public long getAvgCompleteNotificationAsyncParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((ASYNC_COMPLETE_NOTIFY), transactionalClass)));
   }

   @ManagedOperation(description = "Average asynchronous Rollback duration (in microseconds) per class",
                     displayName = "Average Rollback Async per class")
   public long getAvgRollbackAsyncParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((ASYNC_ROLLBACK), transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Commit destination set per class",
                     displayName = "Average Number of Nodes in Commit Destination Set per class")
   public long getAvgNumNodesCommitParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_COMMIT), transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Complete Notification destination set per class",
                     displayName = "Average Number of Nodes in Complete Notification Destination Set per class")
   public long getAvgNumNodesCompleteNotificationParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_COMPLETE_NOTIFY), transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Remote Get destination set per class",
                     displayName = "Average Number of Nodes in Remote Get Destination Set per class")
   public long getAvgNumNodesRemoteGetParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_GET), transactionalClass)));
   }

   //JMX with Transactional class xxxxxxxxx

   @ManagedOperation(description = "Average number of nodes in Prepare destination set per class",
                     displayName = "Average Number of Nodes in Prepare Destination Set per class")
   public long getAvgNumNodesPrepareParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_PREPARE), transactionalClass)));
   }

   @ManagedOperation(description = "Average number of nodes in Rollback destination set per class",
                     displayName = "Average Number of Nodes in Rollback Destination Set per class")
   public long getAvgNumNodesRollbackParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_NODES_ROLLBACK), transactionalClass)));
   }

   @ManagedOperation(description = "Application Contention Factor per class",
                     displayName = "Application Contention Factor per class")
   public double getApplicationContentionFactorParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((APPLICATION_CONTENTION_FACTOR), transactionalClass));
   }

   @Deprecated
   @ManagedOperation(description = "Local Contention Probability per class",
                     displayName = "Local Conflict Probability per class")
   public double getLocalContentionProbabilityParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((LOCAL_CONTENTION_PROBABILITY), transactionalClass));
   }

   @ManagedOperation(description = "Remote Contention Probability per class",
                     displayName = "Remote Conflict Probability per class")
   public double getRemoteContentionProbabilityParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((REMOTE_CONTENTION_PROBABILITY), transactionalClass));
   }

   @ManagedOperation(description = "Lock Contention Probability per class",
                     displayName = "Lock Contention Probability per class")
   public double getLockContentionProbabilityParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute((LOCK_CONTENTION_PROBABILITY), transactionalClass));
   }

   @ManagedOperation(description = "Local execution time of a transaction without the time waiting for lock acquisition per class",
                     displayName = "Local Execution Time Without Locking Time per class")
   public long getLocalExecutionTimeWithoutLockParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_EXEC_NO_CONT, transactionalClass));
   }

   @ManagedOperation(description = "Average lock holding time (in microseconds) per class",
                     displayName = "Average Lock Holding Time per class")
   public long getAvgLockHoldTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_HOLD_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average lock local holding time (in microseconds) per class",
                     displayName = "Average Lock Local Holding Time per class")
   public long getAvgLocalLockHoldTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_HOLD_TIME_LOCAL, transactionalClass));
   }

   @ManagedOperation(description = "Average lock remote holding time (in microseconds) per class",
                     displayName = "Average Lock Remote Holding Time per class")
   public long getAvgRemoteLockHoldTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_HOLD_TIME_REMOTE, transactionalClass));
   }

   @ManagedOperation(description = "Average local rollback duration time (2nd phase only) (in microseconds) per class",
                     displayName = "Average Rollback Time per class")
   public long getAvgRollbackTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(ROLLBACK_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average prepare command size (in bytes) per class",
                     displayName = "Average Prepare Command Size per class")
   public long getAvgPrepareCommandSizeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(PREPARE_COMMAND_SIZE, transactionalClass));
   }

   @ManagedOperation(description = "Average commit command size (in bytes) per class",
                     displayName = "Average Commit Command Size per class")
   public long getAvgCommitCommandSizeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(COMMIT_COMMAND_SIZE, transactionalClass));
   }

   @ManagedOperation(description = "Average clustered get command size (in bytes) per class",
                     displayName = "Average Clustered Get Command Size per class")
   public long getAvgClusteredGetCommandSizeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(CLUSTERED_GET_COMMAND_SIZE, transactionalClass));
   }

   @ManagedOperation(description = "Average time waiting for the lock acquisition (in microseconds) per class",
                     displayName = "Average Lock Waiting Time per class")
   public long getAvgLockWaitingTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCK_WAITING_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average transaction arrival rate, originated locally and remotely (in transaction " +
         "per second) per class",
                     displayName = "Average Transaction Arrival Rate per class")
   public double getAvgTxArrivalRateParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(ARRIVAL_RATE, transactionalClass));
   }

   @ManagedOperation(description = "Percentage of Write xact executed locally (committed and aborted) per class",
                     displayName = "Percentage of Write Transactions per class")
   public double getPercentageWriteTransactionsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(TX_WRITE_PERCENTAGE, transactionalClass));
   }

   @ManagedOperation(description = "Percentage of Write xact executed in all successfully executed " +
         "xacts (local xact only) per class",
                     displayName = "Percentage of Successfully Write Transactions per class")
   public double getPercentageSuccessWriteTransactionsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(SUCCESSFUL_WRITE_PERCENTAGE, transactionalClass));
   }

   @ManagedOperation(description = "The number of aborted xacts due to timeout in lock acquisition per class",
                     displayName = "Number of Aborted Transaction due to Lock Acquisition Timeout per class")
   public long getNumAbortedTxDueTimeoutParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(ExposedStatistic.NUM_LOCK_FAILED_TIMEOUT, transactionalClass));
   }

   @ManagedOperation(description = "The number of aborted xacts due to deadlock per class",
                     displayName = "Number of Aborted Transaction due to Deadlock per class")
   public long getNumAbortedTxDueDeadlockParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_FAILED_DEADLOCK, transactionalClass));
   }

   @ManagedOperation(description = "Average successful read-only xact duration (in microseconds) per class",
                     displayName = "Average Read-Only Transaction Duration per class")
   public long getAvgReadOnlyTxDurationParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(RO_TX_SUCCESSFUL_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average successful write xact duration (in microseconds) per class",
                     displayName = "Average Write Transaction Duration per class")
   public long getAvgWriteTxDurationParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(WR_TX_SUCCESSFUL_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average number of locks per write local xact per class",
                     displayName = "Average Number of Lock per Local Transaction per class")
   public long getAvgNumOfLockLocalTxParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_PER_LOCAL_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average number of locks per write remote xact per class",
                     displayName = "Average Number of Lock per Remote Transaction per class")
   public long getAvgNumOfLockRemoteTxParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_PER_REMOTE_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average number of locks per successfully write local transaction per class",
                     displayName = "Average Number of Lock per Successfully Local Transaction per class")
   public long getAvgNumOfLockSuccessLocalTxParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCK_PER_SUCCESS_LOCAL_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the prepare command locally (in microseconds) per class",
                     displayName = "Average Local Prepare Execution Time per class")
   public long getAvgLocalPrepareTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_PREPARE_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the prepare command remotely (in microseconds) per class",
                     displayName = "Average Remote Prepare Execution Time per class")
   public long getAvgRemotePrepareTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_PREPARE_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the commit command locally (in microseconds) per class",
                     displayName = "Average Local Commit Execution Time per class")
   public long getAvgLocalCommitTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_COMMIT_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the commit command remotely (in microseconds) per class",
                     displayName = "Average Remote Commit Execution Time per class")
   public long getAvgRemoteCommitTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_COMMIT_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the rollback command locally (in microseconds) per class",
                     displayName = "Average Local Rollback Execution Time per class")
   public long getAvgLocalRollbackTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_ROLLBACK_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the rollback command remotely (in microseconds) per class",
                     displayName = "Average Remote Rollback Execution Time per class")
   public long getAvgRemoteRollbackTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_ROLLBACK_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Average time it takes to execute the rollback command remotely (in microseconds) per class",
                     displayName = "Average Remote Transaction Completion Notify Execution Time per class")
   public long getAvgRemoteTxCompleteNotifyTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TX_COMPLETE_NOTIFY_EXECUTION_TIME, transactionalClass));
   }

   @ManagedOperation(description = "Abort Rate per class",
                     displayName = "Abort Rate per class")
   public double getAbortRateParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(ABORT_RATE, transactionalClass));
   }

   @ManagedOperation(description = "Throughput (in xacts per second) per class",
                     displayName = "Throughput per class")
   public double getThroughputParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(THROUGHPUT, transactionalClass));
   }

   @ManagedOperation(description = "Average number of get operations per local read xact per class",
                     displayName = "Average number of get operations per local read xact per class")
   public long getAvgGetsPerROTransactionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_GETS_RO_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average number of get operations per local write xact per class",
                     displayName = "Average number of get operations per local write xact per class")
   public long getAvgGetsPerWrTransactionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_GETS_WR_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average number of remote get operations per local write xact per class",
                     displayName = "Average number of remote get operations per local write xact per class")
   public double getAvgRemoteGetsPerWrTransactionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_GETS_WR_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average number of remote get operations per local read xact per class",
                     displayName = "Average number of remote get operations per local read xact per class")
   public double getAvgRemoteGetsPerROTransactionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_GETS_RO_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average cost of a remote get per class",
                     displayName = "Remote get cost per class")
   public long getRemoteGetExecutionTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(LOCAL_REMOTE_GET_R, transactionalClass));
   }

   @ManagedOperation(description = "Average number of put operations per local write xact per class",
                     displayName = "Average number of put operations per local write xact per class")
   public double getAvgPutsPerWrTransactionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_PUTS_WR_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average number of remote put operations per local write xact per class",
                     displayName = "Average number of remote put operations per local write xact per class")
   public double getAvgRemotePutsPerWrTransactionParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX, transactionalClass));
   }

   @ManagedOperation(description = "Average cost of a remote put per class",
                     displayName = "Remote put cost per class")
   public long getRemotePutExecutionTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(REMOTE_PUT_EXECUTION, transactionalClass));
   }

   @ManagedOperation(description = "Number of gets performed since last reset per class",
                     displayName = "Number of Gets per class")
   public long getNumberOfGetsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GET, transactionalClass));
   }

   @ManagedOperation(description = "Number of remote gets performed since last reset per class",
                     displayName = "Number of Remote Gets per class")
   public long getNumberOfRemoteGetsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCAL_REMOTE_GET, transactionalClass));
   }

   @ManagedOperation(description = "Number of puts performed since last reset per class",
                     displayName = "Number of Puts per class")
   public long getNumberOfPutsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_PUT, transactionalClass));
   }

   @ManagedOperation(description = "Number of remote puts performed since last reset per class",
                     displayName = "Number of Remote Puts per class")
   public long getNumberOfRemotePutsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTE_PUT, transactionalClass));
   }

   @ManagedOperation(description = "Number of committed xacts since last reset per class",
                     displayName = "Number Of Commits per class")
   public long getNumberOfCommitsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_COMMITS, transactionalClass));
   }

   @ManagedOperation(description = "Number of local committed xacts since last reset per class",
                     displayName = "Number Of Local Commits per class")
   public long getNumberOfLocalCommitsParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCAL_COMMITS, transactionalClass));
   }

   @ManagedOperation(description = "Write skew probability per class",
                     displayName = "Write Skew Probability per class")
   public double getWriteSkewProbabilityParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(WRITE_SKEW_PROBABILITY, transactionalClass));
   }

   @ManagedOperation(description = "K-th percentile of local read-only xacts execution time per class",
                     displayName = "K-th Percentile Local Read-Only Transactions per class")
   public double getPercentileLocalReadOnlyTransactionParam(@Parameter(name = "Percentile") int percentile, @Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(RO_LOCAL_PERCENTILE, percentile, transactionalClass));
   }

   @ManagedOperation(description = "K-th percentile of remote read-only xacts execution time per class",
                     displayName = "K-th Percentile Remote Read-Only Transactions per class")
   public double getPercentileRemoteReadOnlyTransactionParam(@Parameter(name = "Percentile") int percentile, @Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(RO_REMOTE_PERCENTILE, percentile, transactionalClass));
   }

   @ManagedOperation(description = "K-th percentile of local write xacts execution time per class",
                     displayName = "K-th Percentile Local Write Transactions per class")
   public double getPercentileLocalRWriteTransactionParam(@Parameter(name = "Percentile") int percentile, @Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(WR_LOCAL_PERCENTILE, percentile, transactionalClass));
   }

   @ManagedOperation(description = "K-th percentile of remote write xacts execution time per class",
                     displayName = "K-th Percentile Remote Write Transactions per class")
   public double getPercentileRemoteWriteTransactionParam(@Parameter(name = "Percentile") int percentile, @Parameter(name = "Transaction Class") String transactionalClass) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(WR_REMOTE_PERCENTILE, percentile, transactionalClass));
   }

   @ManagedOperation(description = "Average Local processing Get time (in microseconds) per class",
                     displayName = "Average Local Get time per class")
   public long getAvgLocalGetTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((LOCAL_GET_EXECUTION), transactionalClass)));
   }

   @ManagedOperation(description = "K-th percentile of local read-only transactions execution time",
                     displayName = "K-th Percentile Local Read-Only Transactions")
   public double getPercentileLocalReadOnlyTransaction(@Parameter(name = "Percentile") int percentile) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(RO_LOCAL_PERCENTILE, percentile, null));
   }

   @ManagedOperation(description = "K-th percentile of remote read-only transactions execution time",
                     displayName = "K-th Percentile Remote Read-Only Transactions")
   public double getPercentileRemoteReadOnlyTransaction(@Parameter(name = "Percentile") int percentile) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(RO_REMOTE_PERCENTILE, percentile, null));
   }

   @ManagedOperation(description = "K-th percentile of local write transactions execution time",
                     displayName = "K-th Percentile Local Write Transactions")
   public double getPercentileLocalRWriteTransaction(@Parameter(name = "Percentile") int percentile) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(WR_LOCAL_PERCENTILE, percentile, null));
   }

   @ManagedOperation(description = "K-th percentile of remote write transactions execution time",
                     displayName = "K-th Percentile Remote Write Transactions")
   public double getPercentileRemoteWriteTransaction(@Parameter(name = "Percentile") int percentile) {
      return handleDouble((Double) TransactionsStatisticsRegistry.getPercentile(WR_REMOTE_PERCENTILE, percentile, null));
   }

   @ManagedOperation(description = "Average TCB time (in microseconds) per class",
                     displayName = "Average TCB time per class")
   public long getAvgTCBTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((TBC), transactionalClass)));
   }

   @ManagedOperation(description = "Average NTCB time (in microseconds) per class",
                     displayName = "Average NTCB time per class")
   public long getAvgNTCBTimeParam(@Parameter(name = "Transaction Class") String transactionalClass) {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NTBC), transactionalClass)));
   }

   @ManagedAttribute(description = "Number of local aborted xacts",
                     displayName = "Number of local aborted xacts")
   public long getNumAbortedXacts() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_ABORTED_WR_TX))));
   }

   @ManagedAttribute(description = "Number of remote gets that have waited",
                     displayName = "Number of remote gets that have waited")
   public long getNumWaitedRemoteGets() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_WAITS_REMOTE_REMOTE_GETS))));
   }

   @ManagedAttribute(description = "Number of local commits that have waited",
                     displayName = "Number of local commits that have waited")
   public long getNumWaitedLocalCommits() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_WAITS_IN_COMMIT_QUEUE))));
   }

   @ManagedAttribute(description = "Number of remote commits that have waited",
                     displayName = "Number of remote commits that have waited")
   public long getNumWaitedRemoteCommits() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((NUM_WAITS_IN_REMOTE_COMMIT_QUEUE))));
   }

   @ManagedAttribute(description = "Local Local Contentions",
                     displayName = "Local Local Contentions")
   public long getNumLocalLocalLockContentions() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((LOCK_CONTENTION_TO_LOCAL))));
   }

   @ManagedAttribute(description = "Local Remote Contentions",
                     displayName = "Local Remote Contentions")
   public long getNumLocalRemoteLockContentions() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((LOCK_CONTENTION_TO_REMOTE))));
   }

   @ManagedAttribute(description = "Remote Local Contentions",
                     displayName = "Remote Local Contentions")
   public long getNumRemoteLocalLockContentions() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((REMOTE_LOCK_CONTENTION_TO_LOCAL))));
   }

   @ManagedAttribute(description = "Remote Remote Contentions",
                     displayName = "Remote Remote Contentions")
   public long getNumRemoteRemoteLockContentions() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((REMOTE_LOCK_CONTENTION_TO_REMOTE))));
   }

   @ManagedAttribute(description = "Average local Tx Waiting time due to pending Tx",
                     displayName = "Average local Tx Waiting time due to pending Tx")
   public final long getAvgLocalTxWaitingTimeInGMUQueueDuePendingTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_PENDING_LOCAL));
   }

   @ManagedAttribute(description = "Average remote Tx Waiting time due to pending Tx",
                     displayName = "Average remote Tx Waiting time due to pending Tx")
   public final long getAvgRemoteTxWaitingTimeInGMUQueueDuePendingTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_PENDING_REMOTE));
   }

   @ManagedAttribute(description = "Average local Tx Waiting time due to slow commits",
                     displayName = "Average local Tx Waiting time due to slow commits")
   public final long getAvgLocalTxWaitingTimeInGMUQueueDueToSlowCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_LOCAL));
   }

   @ManagedAttribute(description = "Average remote Tx Waiting time due to slow commits",
                     displayName = "Average remote Tx Waiting time due to slow commits")
   public final long getAvgRemoteTxWaitingTimeInGMUQueueDueToSlowCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_REMOTE));
   }

   @ManagedAttribute(description = "Average local Tx Waiting time due to commit conflicts",
                     displayName = "Average local Tx Waiting time due to commit conflicts")
   public final long getAvgLocalTxWaitingTimeInGMUQueueDueToCommitsConflicts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_LOCAL));
   }

   @ManagedAttribute(description = "Average remote Tx Waiting time due to commit conflicts",
                     displayName = "Average remote Tx Waiting time due to commit conflicts")
   public final long getAvgRemoteTxWaitingTimeInGMUQueueDueToCommitsConflicts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_REMOTE));
   }

   @ManagedAttribute(description = "Number of local Tx that waited due to pending Tx",
                     displayName = "Number of local Tx that waited due to pending Tx")
   public final long getNumLocalTxWaitingTimeInGMUQueueDuePendingTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING_LOCAL));
   }

   @ManagedAttribute(description = "Number of remote Tx that waited due to pending Tx",
                     displayName = "Number of remote Tx that waited due to pending Tx")
   public final long getNumRemoteTxWaitingTimeInGMUQueueDuePendingTx() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING_REMOTE));
   }

   @ManagedAttribute(description = "Number of local Tx that waited due to slow commits",
                     displayName = "Number of local Tx that waited due to slow commits")
   public final long getNumLocalTxWaitingTimeInGMUQueueDueToSlowCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_LOCAL));
   }

   @ManagedAttribute(description = "Number of remote Tx that waited due to slow commits",
                     displayName = "Number of remote Tx that waited due to slow commits")
   public final long getNumRemoteTxWaitingTimeInGMUQueueDueToSlowCommits() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_REMOTE));
   }

   @ManagedAttribute(description = "Number of local Tx that waited due to commit conflicts",
                     displayName = "Number of local Tx that waited due to commit conflicts")
   public final long getNumLocalTxWaitingTimeInGMUQueueDueToCommitsConflicts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_LOCAL));
   }

   @ManagedAttribute(description = "Number of remote Tx that waited due to commit conflicts",
                     displayName = "Number of remote Tx that waited due to commit conflicts")
   public final long getNumRemoteTxWaitingTimeInGMUQueueDueToCommitsConflicts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_REMOTE));
   }

   @ManagedAttribute(description = "Returns true if the statistics are enabled, false otherwise",
                     displayName = "Enabled")
   public final boolean isEnabled() {
      return TransactionsStatisticsRegistry.isActive();
   }

   @ManagedOperation(description = "Enabled/Disables the statistic collection.",
                     displayName = "Enable/Disable Statistic")
   public final void setEnabled(@Parameter boolean enabled) {
      TransactionsStatisticsRegistry.setActive(enabled);
   }

   @ManagedAttribute(description = "Returns true if the waiting time in GMU queue statistics are collected",
                     displayName = "Waiting time in GMU enabled")
   public final boolean isGMUWaitingTimeEnabled() {
      return TransactionsStatisticsRegistry.isGmuWaitingActive();
   }

   @ManagedOperation(description = "Enables/Disables the waiting in time GMU queue collection",
                     displayName = "Enable/Disable GMU Waiting statistics")
   public final void setGMUWaitingTimeEnabled(@Parameter boolean enabled) {
      TransactionsStatisticsRegistry.setGmuWaitingActive(enabled);
   }

   @ManagedAttribute(description = "Avg Conditional Waiting time experience by TO-GMU prepare command",
                     displayName = "Avg Conditional Waiting time experience by TO-GMU prepare command")
   public final long getTOPrepareWaitTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_AVG_WAIT_TIME));
   }

   @ManagedAttribute(description = "Average TO-GMU conditional max replay time at cohorts",
                     displayName = "Average TO-GMU conditional max replay time at cohorts")
   public final long getTOMaxSuccessfulValidationWaitingTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_MAX_WAIT_TIME));
   }

   @ManagedAttribute(description = "Average TO-GMU validation conditional wait time on a single node",
                     displayName = "Average TO-GMU validation conditional wait time on a single node")
   public final long getTOAvgValidationConditionalWaitTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_REMOTE_WAIT));
   }

   @ManagedAttribute(description = "Average TO-GMU validations that waited on a node",
                     displayName = "Average TO-GMU validations that waited on a node")
   public final long getTONumValidationWaited() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_TO_GMU_PREPARE_COMMAND_REMOTE_WAITED));
   }

   @ManagedAttribute(description = "Probability that a TO-GMU prepare experiences waiting time",
                     displayName = "Probability that a TO-GMU prepare experiences waiting time")
   public final long getTOGMUPrepareWaitProbability() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_TO_GMU_PREPARE_COMMAND_AT_LEAST_ONE_WAIT));
   }

   @ManagedAttribute(description = "Average TO-GMU prepare Rtt minus avg replay time at cohorts for successful xact",
                     displayName = "Average TO-GMU prepare Rtt minus avg replay time at cohorts for successful xact")
   public final long getTOGMUPrepareRttMinusAvgValidationTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RTT_MINUS_AVG));
   }

   @ManagedAttribute(description = "Average TO-GMU prepare Rtt minus max replay time at cohorts for successful xact",
                     displayName = "Average TO-GMU prepare Rtt minus max replay time at cohorts for successful xact")
   public final long getTOGMUPrepareRttMinusMaxValidationTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RTT_MINUS_MAX));
   }

   @ManagedAttribute(description = "Average TO-GMU mean replay time at cohorts for successful xact",
                     displayName = "Average TO-GMU mean replay time at cohorts for successful xact")
   public final long getTOGMUAvgSuccessfulValidationResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RESPONSE_TIME));
   }

   @ManagedAttribute(description = "Average TO-GMU mean replay time at cohorts for successful xact",
                     displayName = "Average TO-GMU mean replay time at cohorts for successful xact")
   public final long getTOGMUAvgSuccessfulValidationServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_SERVICE_TIME));
   }

   @ManagedAttribute(description = "Number of successful TO-GMU prepares that did not wait",
                     displayName = "Number of successful TO-GMU prepares that did not wait")
   public final long getNumTOGMUSuccessfulPrepareNoWait() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_TO_GMU_PREPARE_COMMAND_RTT_NO_WAITED));
   }

   @ManagedAttribute(description = "Avg rtt for TO-GMU prepares that did not wait",
                     displayName = "Avg rtt for TO-GMU prepares that did not wait")
   public final long getAvgTOGMUPrepareNoWaitRtt() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RTT_NO_WAIT));
   }

   @ManagedAttribute(description = "Avg rtt for GMU remote get that do not wait",
                     displayName = "Avg rtt for GMU remote get that do not wait")
   public final long getAvgGmuClusteredGetCommandRttNoWait() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(RTT_GET_NO_WAIT));
   }

    /*
     "handleCommand" methods could have been more compact (since they do very similar stuff)
     But I wanted smaller, clear sub-methods
    */

   //NB: readOnly transactions are never aborted (RC, RR, GMU)
   private void handleRollbackCommand(TransactionStatistics transactionStatistics, long initTime, long initCpuTime, TxInvocationContext ctx) {
      ExposedStatistic stat, cpuStat, counter;
      if (ctx.isOriginLocal()) {
         if (transactionStatistics.isPrepareSent() || ctx.getCacheTransaction().wasPrepareSent()) {
            cpuStat = UPDATE_TX_LOCAL_REMOTE_ROLLBACK_S;
            stat = UPDATE_TX_LOCAL_REMOTE_ROLLBACK_R;
            counter = NUM_UPDATE_TX_LOCAL_REMOTE_ROLLBACK;
            transactionStatistics.incrementValue(NUM_REMOTELY_ABORTED);
         } else {
            if (transactionStatistics.stillLocalExecution()) {
               transactionStatistics.incrementValue(NUM_EARLY_ABORTS);
            } else {
               transactionStatistics.incrementValue(NUM_LOCALPREPARE_ABORTS);
            }
            cpuStat = UPDATE_TX_LOCAL_LOCAL_ROLLBACK_S;
            stat = UPDATE_TX_LOCAL_LOCAL_ROLLBACK_R;
            counter = NUM_UPDATE_TX_LOCAL_LOCAL_ROLLBACK;
         }
      } else {
         cpuStat = UPDATE_TX_REMOTE_ROLLBACK_S;
         stat = UPDATE_TX_REMOTE_ROLLBACK_R;
         counter = NUM_UPDATE_TX_REMOTE_ROLLBACK;
      }
      updateWallClockTime(transactionStatistics, stat, counter, initTime);
      if (TransactionsStatisticsRegistry.isSampleServiceTime())
         updateServiceTimeWithoutCounter(transactionStatistics, cpuStat, initCpuTime);
   }

   private void handleCommitCommand(TransactionStatistics transactionStatistics, long initTime, long initCpuTime, TxInvocationContext ctx) {
      ExposedStatistic stat, cpuStat, counter;
      //In TO some xacts can be marked as RO even though remote
      if (ctx.isOriginLocal() && transactionStatistics.isReadOnly()) {
         cpuStat = READ_ONLY_TX_COMMIT_S;
         counter = NUM_READ_ONLY_TX_COMMIT;
         stat = READ_ONLY_TX_COMMIT_R;
      }
      //This is valid both for local and remote. The registry will populate the right container
      else {
         if (ctx.isOriginLocal()) {
            cpuStat = UPDATE_TX_LOCAL_COMMIT_S;
            counter = NUM_UPDATE_TX_LOCAL_COMMIT;
            stat = UPDATE_TX_LOCAL_COMMIT_R;
         } else {
            cpuStat = UPDATE_TX_REMOTE_COMMIT_S;
            counter = NUM_UPDATE_TX_REMOTE_COMMIT;
            stat = UPDATE_TX_REMOTE_COMMIT_R;
         }
      }

      updateWallClockTime(transactionStatistics, stat, counter, initTime);
      if (TransactionsStatisticsRegistry.isSampleServiceTime())
         updateServiceTimeWithoutCounter(transactionStatistics, cpuStat, initCpuTime);
   }

   /**
    * Increases the service and responseTime; This is invoked *only* if the prepareCommand is executed correctly
    */
   private void handlePrepareCommand(TransactionStatistics transactionStatistics, long initTime, long initCpuTime, TxInvocationContext ctx, PrepareCommand command) {
      ExposedStatistic stat, cpuStat, counter;
      if (transactionStatistics.isReadOnly()) {
         stat = READ_ONLY_TX_PREPARE_R;
         cpuStat = READ_ONLY_TX_PREPARE_S;
         updateWallClockTimeWithoutCounter(transactionStatistics, stat, initTime);
         if (TransactionsStatisticsRegistry.isSampleServiceTime())
            updateServiceTimeWithoutCounter(transactionStatistics, cpuStat, initCpuTime);
      }
      //This is valid both for local and remote. The registry will populate the right container
      else {
         if (ctx.isOriginLocal()) {
            stat = UPDATE_TX_LOCAL_PREPARE_R;
            cpuStat = UPDATE_TX_LOCAL_PREPARE_S;

         } else {
            stat = UPDATE_TX_REMOTE_PREPARE_R;
            cpuStat = UPDATE_TX_REMOTE_PREPARE_S;
         }
         counter = NUM_UPDATE_TX_PREPARED;
         updateWallClockTime(transactionStatistics, stat, counter, initTime);
         if (TransactionsStatisticsRegistry.isSampleServiceTime())
            updateServiceTimeWithoutCounter(transactionStatistics, cpuStat, initCpuTime);
      }

      //Take stats relevant to the avg number of read and write data items in the message
      //NB: these stats are taken only for prepare that will turn into a commit
      transactionStatistics.addValue(NUM_OWNED_RD_ITEMS_IN_OK_PREPARE, localRd(command));
      transactionStatistics.addValue(NUM_OWNED_WR_ITEMS_IN_OK_PREPARE, localWr(command));
   }

   private int localWr(PrepareCommand command) {
      WriteCommand[] wrSet = command.getModifications();
      int localWr = 0;
      for (WriteCommand wr : wrSet) {
         for (Object k : wr.getAffectedKeys()) {
            if (!isRemote(k))
               localWr++;
         }
      }
      return localWr;
   }

   private int localRd(PrepareCommand command) {
      if (!(command instanceof GMUPrepareCommand))
         return 0;
      int localRd = 0;

      Object[] rdSet = ((GMUPrepareCommand) command).getReadSet();
      for (Object rd : rdSet) {
         if (!isRemote(rd))
            localRd++;
      }
      return localRd;
   }

   private void replace() {
      log.info("CustomStatsInterceptor Enabled!");
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      this.wireConfiguration();
      GlobalComponentRegistry globalComponentRegistry = componentRegistry.getGlobalComponentRegistry();
      InboundInvocationHandlerWrapper invocationHandlerWrapper = rewireInvocationHandler(globalComponentRegistry);
      globalComponentRegistry.rewire();

      replaceFieldInTransport(componentRegistry, invocationHandlerWrapper);

      replaceRpcManager(componentRegistry);
      replaceLockManager(componentRegistry);
      componentRegistry.rewire();
   }

   private void wireConfiguration() {
      this.configuration = cache.getAdvancedCache().getCacheConfiguration();
   }

   private void replaceFieldInTransport(ComponentRegistry componentRegistry, InboundInvocationHandlerWrapper invocationHandlerWrapper) {
      JGroupsTransport t = (JGroupsTransport) componentRegistry.getComponent(Transport.class);
      CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
      try {
         Field f = card.getClass().getDeclaredField("inboundInvocationHandler");
         f.setAccessible(true);
         f.set(card, invocationHandlerWrapper);
      } catch (NoSuchFieldException e) {
         e.printStackTrace();
      } catch (IllegalAccessException e) {
         e.printStackTrace();
      }
   }

   private InboundInvocationHandlerWrapper rewireInvocationHandler(GlobalComponentRegistry globalComponentRegistry) {
      InboundInvocationHandler inboundHandler = globalComponentRegistry.getComponent(InboundInvocationHandler.class);
      InboundInvocationHandlerWrapper invocationHandlerWrapper = new InboundInvocationHandlerWrapper(inboundHandler,
                                                                                                     transactionTable);
      globalComponentRegistry.registerComponent(invocationHandlerWrapper, InboundInvocationHandler.class);
      return invocationHandlerWrapper;
   }

   private void replaceLockManager(ComponentRegistry componentRegistry) {
      LockManager lockManager = componentRegistry.getComponent(LockManager.class);
      LockManagerWrapper lockManagerWrapper = new LockManagerWrapper(lockManager, StreamLibContainer.getOrCreateStreamLibContainer(cache), true);// this.configuration.customStatsConfiguration().sampleServiceTimes());
      componentRegistry.registerComponent(lockManagerWrapper, LockManager.class);
   }

   private void replaceRpcManager(ComponentRegistry componentRegistry) {
      RpcManager rpcManager = componentRegistry.getComponent(RpcManager.class);
      RpcManagerWrapper rpcManagerWrapper = new RpcManagerWrapper(rpcManager);
      componentRegistry.registerComponent(rpcManagerWrapper, RpcManager.class);
      this.rpcManager = rpcManagerWrapper;
   }

   private TransactionStatistics initStatsIfNecessary(InvocationContext ctx) {
      if (ctx.isInTxScope()) {
         TransactionStatistics transactionStatistics = TransactionsStatisticsRegistry
               .initTransactionIfNecessary((TxInvocationContext) ctx);
         if (transactionStatistics == null) {
            throw new IllegalStateException("Transaction Statistics cannot be null with transactional context");
         }
         return transactionStatistics;
      }
      return null;
   }

   private boolean isLockTimeout(TimeoutException e) {
      return e.getMessage().startsWith("Unable to acquire lock after");
   }

   private void updateWallClockTime(TransactionStatistics transactionStatistics, ExposedStatistic duration, ExposedStatistic counter, long initTime) {
      transactionStatistics.addValue(duration, System.nanoTime() - initTime);
      transactionStatistics.incrementValue(counter);
   }

   private void updateWallClockTimeWithoutCounter(TransactionStatistics transactionStatistics, ExposedStatistic duration, long initTime) {
      transactionStatistics.addValue(duration, System.nanoTime() - initTime);
   }

   private void updateServiceTimeWithoutCounter(TransactionStatistics transactionStatistics, ExposedStatistic time, long initTime) {
      transactionStatistics.addValue(time, TransactionsStatisticsRegistry.getThreadCPUTime() - initTime);
   }

   /*private void updateServiceTime(ExposedStatistic duration, ExposedStatistic counter, long initTime) {
      transactionTable.addValue(duration, threadMXBean.getCurrentThreadCpuTime() - initTime);
      transactionTable.incrementValue(counter);
   }*/

   private long handleLong(Long value) {
      return value == null ? 0 : value;
   }

   private double handleDouble(Double value) {
      return value == null ? 0 : value;
   }

   private boolean isRemote(Object key) {
      return distributionManager != null && !distributionManager.getLocality(key).isLocal();
   }

}
