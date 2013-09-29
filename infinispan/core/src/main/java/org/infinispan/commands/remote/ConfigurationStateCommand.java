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
package org.infinispan.commands.remote;

import org.infinispan.context.InvocationContext;
import org.infinispan.reconfigurableprotocol.manager.ProtocolManager;
import org.infinispan.reconfigurableprotocol.manager.ReconfigurableReplicationManager;
import org.infinispan.statetransfer.ConfigurationState;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ConfigurationStateCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 48;
   private static final Object[] EMPTY_ARRAY = new Object[0];
   private ReconfigurableReplicationManager reconfigurableReplicationManager;

   public ConfigurationStateCommand(String cacheName) {
      super(cacheName);
   }

   public ConfigurationStateCommand() {
      super(null); //for org.infinispan.commands.CommandIdUniquenessTest
   }

   public final void initialize(ReconfigurableReplicationManager reconfigurableReplicationManager) {
      this.reconfigurableReplicationManager = reconfigurableReplicationManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      ProtocolManager.CurrentProtocolInfo protocolName = reconfigurableReplicationManager.getProtocolManager().getCurrentProtocolInfo();
      return new ConfigurationState(protocolName.getCurrent().getUniqueProtocolName(), protocolName.getEpoch());
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return EMPTY_ARRAY;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      //nothing
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
