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

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.Response;

import static org.infinispan.stats.ExposedStatistic.NUM_TX_COMPLETE_NOTIFY_COMMAND;
import static org.infinispan.stats.ExposedStatistic.TX_COMPLETE_NOTIFY_EXECUTION_TIME;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Pedro Ruivo
 * @since 5.2
 */
public class InboundInvocationHandlerWrapper implements InboundInvocationHandler {

   private static final Log log = LogFactory.getLog(InboundInvocationHandlerWrapper.class);
   private final InboundInvocationHandler actual;
   private final TransactionTable transactionTable;

   public InboundInvocationHandlerWrapper(InboundInvocationHandler actual, TransactionTable transactionTable) {
      this.actual = actual;
      this.transactionTable = transactionTable;
   }

   @Override
   public void handle(CacheRpcCommand command, Address origin, Response response) throws Throwable {
      if (!TransactionsStatisticsRegistry.isActive()) {
         actual.handle(command, origin, response);
         return;
      }

      if (log.isTraceEnabled()) {
         log.tracef("Handle remote command [%s] by the invocation handle wrapper from %s", command, origin);
      }
      GlobalTransaction globalTransaction = getGlobalTransaction(command);
      try {
         if (globalTransaction != null) {
            if (log.isDebugEnabled()) {
               log.debugf("The command %s is transactional and the global transaction is %s", command,
                          globalTransaction.globalId());
            }
            TransactionsStatisticsRegistry.attachRemoteTransactionStatistic(globalTransaction, command instanceof PrepareCommand ||
                  command instanceof CommitCommand);
         } else {
            if (log.isDebugEnabled()) {
               log.debugf("The command %s is NOT transactional", command);
            }
         }

         boolean txCompleteNotify = command instanceof TxCompletionNotificationCommand;
         long currTime = 0;
         if (txCompleteNotify) {
            currTime = System.nanoTime();
         }

         actual.handle(command, origin, response);

         if (txCompleteNotify) {
            TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(TX_COMPLETE_NOTIFY_EXECUTION_TIME,
                                                                    System.nanoTime() - currTime, false);
            TransactionsStatisticsRegistry.incrementValueAndFlushIfNeeded(NUM_TX_COMPLETE_NOTIFY_COMMAND, false);
         }
      } finally {
         if (globalTransaction != null) {
            if (log.isDebugEnabled()) {
               log.debugf("Detach statistics for command %s", command, globalTransaction.globalId());
            }
            TransactionsStatisticsRegistry.detachRemoteTransactionStatistic(globalTransaction,
                                                                            !transactionTable.containRemoteTx(globalTransaction));
         }
      }
   }

   private GlobalTransaction getGlobalTransaction(CacheRpcCommand cacheRpcCommand) {
      if (cacheRpcCommand instanceof TransactionBoundaryCommand) {
         return ((TransactionBoundaryCommand) cacheRpcCommand).getGlobalTransaction();
      } else if (cacheRpcCommand instanceof TxCompletionNotificationCommand) {
         for (Object obj : cacheRpcCommand.getParameters()) {
            if (obj instanceof GlobalTransaction) {
               return (GlobalTransaction) obj;
            }
         }
      }
      return null;
   }
}
