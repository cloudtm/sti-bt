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

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;

import java.util.Map;

import static org.infinispan.transaction.gmu.GMUHelper.joinAndSetTransactionVersion;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderGMUReplicationInterceptor extends GMUReplicationInterceptor {

   @Override
   protected void broadcastPrepare(TxInvocationContext context, PrepareCommand command) {
      if (!context.isOriginLocal()) {
         throw new IllegalStateException("Expected a local context before TO-Broadcast prepare command");
      }

      if (!(command instanceof GMUPrepareCommand)) {
         throw new IllegalStateException("Expected a Versioned Prepare Command in version aware component");
      }

      try {
         Map<Address, Response> responseMap = totalOrderBroadcastPrepare(command, null);
         joinAndSetTransactionVersion(responseMap.values(), context, versionGenerator, null);
      } finally {
         totalOrderTxPrepare(context);
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!shouldTotalOrderRollbackBeInvokedRemotely(ctx)) {
         return invokeNextInterceptor(ctx, command);
      }
      totalOrderTxRollback(ctx);
      return super.visitRollbackCommand(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      totalOrderTxCommit(ctx);
      return super.visitCommitCommand(ctx, command);
   }
}
