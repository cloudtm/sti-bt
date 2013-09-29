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
package org.infinispan.interceptors.base;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Interceptor that wraps the protocol dependent interceptors and redirects the transaction boundary commands (prepare,
 * commit and rollback) to the correct interceptor
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ReconfigurableProtocolAwareWrapperInterceptor extends CommandInterceptor {

   private static final Log log = LogFactory.getLog(ReconfigurableProtocolAwareWrapperInterceptor.class);
   private final ConcurrentMap<String, CommandInterceptor> protocolDependentInterceptor = new ConcurrentHashMap<String, CommandInterceptor>();
   private final InterceptorChain.InterceptorType type;

   public ReconfigurableProtocolAwareWrapperInterceptor(InterceptorChain.InterceptorType type) {
      this.type = type;
   }

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return handleTxBoundaryCommand(ctx, command);
   }

   @Override
   public final Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return handleTxBoundaryCommand(ctx, command);
   }

   @Override
   public final Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return handleTxBoundaryCommand(ctx, command);
   }

   @Override
   public void setNext(CommandInterceptor next) {
      super.setNext(next);
      for (CommandInterceptor ci : protocolDependentInterceptor.values()) {
         ci.setNext(next);
      }
   }

   /**
    * set the command interceptor that a transaction boundary command created in a specific transaction protocol should
    * visit
    *
    * @param protocolId  the transaction protocol identifier
    * @param interceptor the command interceptor
    */
   public final void setProtocolDependentInterceptor(String protocolId, CommandInterceptor interceptor) {
      protocolDependentInterceptor.put(protocolId, interceptor);
      interceptor.setNext(getNext());
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Added a new replication protocol dependent interceptor. Interceptors are %s", type,
                    protocolDependentInterceptor);
      }
   }

   public final String routeTableToString() {
      return protocolDependentInterceptor.toString();
   }

   public final boolean isSameClass(Class<?> clazz, boolean matchSubclass, String protocolId, InterceptorChain ic) {
      if (protocolId != null) {
         CommandInterceptor interceptor = protocolDependentInterceptor.get(protocolId);
         return interceptor != null && ic.isSameClass(interceptor, clazz, matchSubclass, protocolId);
      }
      for (CommandInterceptor interceptor : protocolDependentInterceptor.values()) {
         if (ic.isSameClass(interceptor, clazz, matchSubclass, protocolId)) {
            return true;
         }
      }
      return false;
   }

   public final void addInterceptorsWithClass(List<CommandInterceptor> list, Class clazz, InterceptorChain ic) {
      for (CommandInterceptor commandInterceptor : protocolDependentInterceptor.values()) {
         ic.addInterceptorsWithClass(list, commandInterceptor, clazz);
      }
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return command.acceptVisitor(ctx, getNext(ctx.getProtocolId()));
   }

   private CommandInterceptor getNext(GlobalTransaction globalTransaction) {
      return getNext(globalTransaction.getProtocolId());
   }

   private CommandInterceptor getNext(String protocolId) {
      CommandInterceptor next = protocolDependentInterceptor.get(protocolId);
      if (next == null) {
         next = getNext();
      }
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Executing command in protocol %s. Next interceptor is %s", type, protocolId, next);
      }
      return next;
   }

   private Object handleTxBoundaryCommand(TxInvocationContext ctx, TransactionBoundaryCommand command) throws Throwable {
      return command.acceptVisitor(ctx, getNext(ctx.getGlobalTransaction()));
   }

}
