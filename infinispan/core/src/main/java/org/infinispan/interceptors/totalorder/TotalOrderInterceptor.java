/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

package org.infinispan.interceptors.totalorder;

import org.infinispan.CacheException;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TotalOrderRemoteTransactionState;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Created to control the total order validation. It disable the possibility of acquiring locks during execution through
 * the cache API
 *
 * @author Pedro Ruivo
 * @author Mircea.Markus@jboss.com
 * @since 5.3
 */
public class TotalOrderInterceptor extends CommandInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderInterceptor.class);
   private TransactionTable transactionTable;
   private TotalOrderManager totalOrderManager;

   @Inject
   public void inject(TransactionTable transactionTable, TotalOrderManager totalOrderManager) {
      this.transactionTable = transactionTable;
      this.totalOrderManager = totalOrderManager;
   }

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (log.isDebugEnabled()) {
         log.debugf("Prepare received. Transaction=%s, Affected keys=%s, Local=%s",
                    command.getGlobalTransaction().globalId(),
                    command.getAffectedKeys(),
                    ctx.isOriginLocal());
      }
      if (!(command instanceof TotalOrderPrepareCommand)) {
         throw new IllegalStateException("TotalOrderInterceptor can only handle TotalOrderPrepareCommand");
      }

      try {
         if (ctx.isOriginLocal()) {
            return invokeNextInterceptor(ctx, command);
         } else {
            TotalOrderRemoteTransactionState state = getTransactionState(ctx, false);

            try {
               state.preparing();
               if (state.isRollbackReceived()) {
                  //this means that rollback has already been received
                  transactionTable.removeRemoteTransaction(command.getGlobalTransaction());
                  throw new CacheException("Cannot prepare transaction" + command.getGlobalTransaction().globalId() +
                                                 ". it was already marked as rollback");
               }

               if (state.isCommitReceived()) {
                  log.tracef("Transaction %s marked for commit, skipping the write skew check and forcing 1PC",
                             command.getGlobalTransaction().globalId());
                  ((TotalOrderPrepareCommand) command).markSkipWriteSkewCheck();
                  ((TotalOrderPrepareCommand) command).markAsOnePhaseCommit();
               }


               if (log.isTraceEnabled()) {
                  log.tracef("Validating transaction %s ", command.getGlobalTransaction().globalId());
               }

               //invoke next interceptor in the chain
               Object result = invokeNextInterceptor(ctx, command);

               if (command.isOnePhaseCommit()) {
                  totalOrderManager.release(state);
               }
               return result;
            } finally {
               state.prepared();
            }

         }
      } catch (Throwable exception) {
         if (log.isDebugEnabled()) {
            log.debugf(exception, "Exception while preparing for transaction %s",
                       command.getGlobalTransaction().globalId());
         }
         if (command.isOnePhaseCommit()) {
            transactionTable.remoteTransactionRollback(command.getGlobalTransaction());
         }
         throw exception;
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return visitSecondPhaseCommand(ctx, command, false);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return visitSecondPhaseCommand(ctx, command, true);
   }

   @Override
   public final Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      throw new UnsupportedOperationException("Lock interface not supported with total order protocol");
   }

   private Object visitSecondPhaseCommand(TxInvocationContext context, AbstractTransactionBoundaryCommand command, boolean commit) throws Throwable {
      GlobalTransaction gtx = command.getGlobalTransaction();
      if (log.isTraceEnabled()) {
         log.tracef("Second phase command received. Commit?=%s Transaction=%s, Local=%s", commit, gtx.globalId(),
                    context.isOriginLocal());
      }

      TotalOrderRemoteTransactionState state = getTransactionState(context, !commit);

      try {
         if (!processSecondCommand(state, commit) && !context.isOriginLocal()) {
            //we can return here, because we set onePhaseCommit to prepare and it will release all the resources
            return null;
         }

         return invokeNextInterceptor(context, command);
      } catch (Throwable exception) {
         if (log.isDebugEnabled()) {
            log.debugf(exception, "Exception while rollback transaction %s", gtx.globalId());
         }
         throw exception;
      } finally {
         if (state != null && state.isFinished()) {
            totalOrderManager.release(state);
            if (commit) {
               transactionTable.remoteTransactionCommitted(command.getGlobalTransaction());
            } else {
               transactionTable.remoteTransactionRollback(command.getGlobalTransaction());
            }
         }
      }
   }

   private TotalOrderRemoteTransactionState getTransactionState(TxInvocationContext context, boolean forRollback) {
      if (!context.isOriginLocal()) {
         return ((RemoteTransaction) context.getCacheTransaction()).getTransactionState();
      }
      RemoteTransaction remoteTransaction = transactionTable.getRemoteTransaction(context.getGlobalTransaction());
      if (forRollback) {
         LocalTransaction localTransaction = (LocalTransaction) context.getCacheTransaction();
         if (localTransaction.isPrepareSent() && !localTransaction.isCommitOrRollbackSent()) {
            remoteTransaction = transactionTable.getOrCreateRemoteTransaction(context.getGlobalTransaction(),
                                                                              context.getModifications().toArray(new WriteCommand[1]));
         }
      }
      return remoteTransaction == null ? null : remoteTransaction.getTransactionState();
   }

   private boolean processSecondCommand(TotalOrderRemoteTransactionState state, boolean commit) {
      //it means that the transaction was not prepared (i.e. a suicide transaction)
      if (state == null) {
         return true;
      }

      try {
         return state.waitUntilPrepared(commit);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         log.timeoutWaitingUntilTransactionPrepared(state.getGlobalTransaction().globalId());
      }
      return false;
   }
}
