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
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.ReplicationInterceptor;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;

import static org.infinispan.transaction.gmu.GMUHelper.joinAndSetTransactionVersion;
import static org.infinispan.transaction.gmu.GMUHelper.toGMUVersionGenerator;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class GMUReplicationInterceptor extends ReplicationInterceptor {

   private static final Log log = LogFactory.getLog(GMUReplicationInterceptor.class);
   protected GMUVersionGenerator versionGenerator;

   @Inject
   public void inject(VersionGenerator versionGenerator) {
      this.versionGenerator = toGMUVersionGenerator(versionGenerator);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!ctx.isInTxScope()) throw new IllegalStateException("This should not be possible!");
      Object retVal = invokeNextInterceptor(ctx, command);
      if (shouldInvokeRemoteTxCommand(ctx)) {
         return sendGMUCommitCommand(retVal, command, null);
      }
      return retVal;
   }

   @Override
   protected void broadcastPrepare(TxInvocationContext context, PrepareCommand command) {
      Map<Address, Response> responses = rpcManager.invokeRemotely(null, command, true, false, false);
      log.debugf("broadcast prepare command for transaction %s. responses are: %s",
                 command.getGlobalTransaction().globalId(), responses.toString());

      joinAndSetTransactionVersion(responses.values(), context, versionGenerator, null);
   }
}
