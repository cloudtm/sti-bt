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
package org.infinispan.commands.tx;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.DelayedComputation;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * @author Pedro Ruivo 
 * @since 5.2
 */
public class GMUPrepareCommand extends PrepareCommand {

   public static final byte COMMAND_ID = 43;

   private static final Object[] EMPTY_READ_SET_ARRAY = new Object[0];

   private Object[] readSet;
   private EntryVersion version;
   private BitSet alreadyReadFrom;

   public GMUPrepareCommand(String cacheName, GlobalTransaction gtx, boolean onePhaseCommit, WriteCommand... modifications) {
      super(cacheName, gtx, onePhaseCommit, modifications);
   }

   public GMUPrepareCommand(String cacheName, GlobalTransaction gtx, List<WriteCommand> commands, DelayedComputation[] computations, boolean onePhaseCommit) {
      super(cacheName, gtx, commands, onePhaseCommit);
   }

   public GMUPrepareCommand(String cacheName) {
      super(cacheName);
   }

   public GMUPrepareCommand() {
      super(null);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true; //we need the version from the other guys...
   }
   
   @Override
   public Set<Object> getAffectedKeys() {
      int modLength = modifications == null ? 0 : modifications.length;
      Set<Object> keys = new HashSet<Object>(modLength);
      if (modLength != 0) {
	  for (WriteCommand wc: modifications) keys.addAll(wc.getAffectedKeys());
      }
      return keys;
   }

   @Override
   public Object[] getParameters() {
      int numMods = modifications == null ? 0 : modifications.length;
      int numReads = readSet == null ? 0 : readSet.length;
      int i = 0;
      final int params = 6;
      Object[] retVal = new Object[numMods + numReads + params];
      retVal[i++] = globalTx;
      retVal[i++] = onePhaseCommit;
      retVal[i++] = version;
      retVal[i++] = alreadyReadFrom;
      retVal[i++] = numMods;
      retVal[i++] = numReads;
      if (numMods > 0) {
	  System.arraycopy(modifications, 0, retVal, params, numMods);
      }
      if (numReads > 0) {
	  System.arraycopy(readSet, 0, retVal, params + numMods, numReads);
      }
      return retVal;
   }

   @Override
   @SuppressWarnings({"unchecked", "SuspiciousSystemArraycopy"})
   public void setParameters(int commandId, Object[] args) {
      int i = 0;
      globalTx = (GlobalTransaction) args[i++];
      onePhaseCommit = (Boolean) args[i++];
      version = (EntryVersion) args[i++];
      alreadyReadFrom = (BitSet) args[i++];
      int numMods = (Integer) args[i++];
      int numReads = (Integer) args[i++];
      if (numMods > 0) {
         modifications = new WriteCommand[numMods];
         System.arraycopy(args, i, modifications, 0, numMods);
      }
      if(numReads > 0){
         readSet = new Object[numReads];
         System.arraycopy(args, i + numMods, readSet, 0, numReads);
      }
   }

   @Override
   public GMUPrepareCommand copy() {
      GMUPrepareCommand copy = new GMUPrepareCommand(cacheName);
      copy.globalTx = globalTx;
      copy.modifications = modifications == null ? null : modifications.clone();
      copy.onePhaseCommit = onePhaseCommit;
      copy.readSet = readSet == null ? null : readSet.clone();
      copy.version = version;
      copy.alreadyReadFrom = alreadyReadFrom;
      return copy;
   }

   @Override
   public String toString() {
      return "GMUPrepareCommand {" +
            "version=" + version +
            ", onePhaseCommit=" + onePhaseCommit +
            ", gtx=" + globalTx +
            ", cacheName='" + cacheName + '\'' +
            ", readSet=" + (readSet == null ? null : Arrays.asList(readSet)) +
            ", modifications=" + getAffectedKeys() +
            '}';
   }

   public void setReadSet(Collection<Object> readSet) {
      this.readSet = readSet == null || readSet.isEmpty() ? null : readSet.toArray();
   }
   
   public void setVersion(EntryVersion version) {
      this.version = version;
   }

   public Object[] getReadSet() {
      return readSet == null ? EMPTY_READ_SET_ARRAY : readSet;
   }
   
   public EntryVersion getPrepareVersion() {
      return version;
   }

   public BitSet getAlreadyReadFrom() {
      return alreadyReadFrom;
   }

   public void setAlreadyReadFrom(BitSet alreadyReadFrom) {
      this.alreadyReadFrom = alreadyReadFrom;
   }
   
}
