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

package org.infinispan.interceptors;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.*;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.rpc.RpcManager;

/**
 * This interceptor has the behavior needed for passive replication, namely, it now allow the processing of write 
 * operations / transactions in non-primary node (i.e. in backup nodes)
 *
 * The Passive Replication protocol was developed by Diego Didona and Sebastiano Peluso
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class PassiveReplicationInterceptor extends CommandInterceptor {

   private RpcManager rpcManager;

   @Inject
   public void inject(RpcManager rpcManager) {
      this.rpcManager = rpcManager;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !isMaster() && (ctx.hasModifications() || command.hasModifications())) {
         throw new IllegalStateException("Write transaction not allowed in Passive Replication, in backup node");
      }
      return invokeNextInterceptor(ctx, command);
   }

   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !isMaster()) {
         throw new IllegalStateException("Write operation not allowed in Passive Replication, in backup nodes");
      }
      return invokeNextInterceptor(ctx, command);
   }

   private boolean isMaster() {
      return rpcManager.getAddress().equals(rpcManager.getMembers().get(0));
   }
}
