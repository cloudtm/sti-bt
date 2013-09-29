/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.commands.tx.totalorder;

import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.transaction.TotalOrderRemoteTransactionState;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderGMUPrepareCommand extends GMUPrepareCommand implements TotalOrderPrepareCommand {

   public static final byte COMMAND_ID = 45;

   public TotalOrderGMUPrepareCommand(String cacheName, GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhase) {
      super(cacheName, gtx, modifications, null, onePhase);
   }

   public TotalOrderGMUPrepareCommand(String cacheName) {
      super(cacheName);
   }

   private TotalOrderGMUPrepareCommand() {
      super(null); // For command id uniqueness test
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void markAsOnePhaseCommit() {
      this.onePhaseCommit = true;
   }

   @Override
   public void markSkipWriteSkewCheck() {/*no-op*/}

   @Override
   public boolean skipWriteSkewCheck() {
      return false;
   }

   @Override
   public TotalOrderRemoteTransactionState getOrCreateState() {
      return getRemoteTransaction().getTransactionState();
   }

   @Override
   public Object[][] getKeysToLock() {
      Object[] writeSet = getAffectedKeysToLock(false);
      if (writeSet == null) {
         return null;
      }
      Object[][] keysWriteAndRead = new Object[2][];
      keysWriteAndRead[0] = writeSet;
      keysWriteAndRead[1] = getReadSet();
      return keysWriteAndRead;
   }
}
