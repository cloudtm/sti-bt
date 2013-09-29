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
import org.infinispan.dataplacement.DataPlacementManager;
import org.infinispan.dataplacement.ObjectRequest;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;

/**
 * The command used to send information among nodes
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementCommand extends BaseRpcCommand {

   public static final short COMMAND_ID = 40;
   private static final Log log = LogFactory.getLog(DataPlacementCommand.class);
   private DataPlacementManager dataPlacementManager;
   //message data
   private Type type;
   private long roundId;
   private int intValue;
   private ObjectRequest objectRequest;
   private Map<Integer, ObjectLookup> objectLookup;
   private Address[] members;

   public DataPlacementCommand(String cacheName, Type type, long roundId) {
      super(cacheName);
      this.type = type;
      this.roundId = roundId;
   }


   public DataPlacementCommand(String cacheName) {
      super(cacheName);
   }

   public DataPlacementCommand() {
      super(null);
   }

   public final void initialize(DataPlacementManager dataPlacementManager) {
      this.dataPlacementManager = dataPlacementManager;
   }

   public void setObjectRequest(ObjectRequest objectRequest) {
      this.objectRequest = objectRequest;
   }

   public void setObjectLookup(Map<Integer, ObjectLookup> objectLookup) {
      this.objectLookup = objectLookup;
   }

   public void setIntValue(int intValue) {
      this.intValue = intValue;
   }

   public void setMembers(Address[] members) {
      this.members = members;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      try {
         switch (type) {
            case DATA_PLACEMENT_REQUEST:
               dataPlacementManager.dataPlacementRequest();
               break;
            case DATA_PLACEMENT_START:
               dataPlacementManager.startDataPlacement(roundId, members);
               break;
            case REMOTE_TOP_LIST_PHASE:
               dataPlacementManager.addRequest(getOrigin(), objectRequest, roundId);
               break;
            case OBJECT_LOOKUP_PHASE:
               dataPlacementManager.addObjectLookup(getOrigin(), objectLookup, roundId);
               break;
            case SET_COOL_DOWN_TIME:
               dataPlacementManager.internalSetCoolDownTime(intValue);
               break;
            case REPLICATION_DEGREE:
               dataPlacementManager.handleNewReplicationDegree(intValue);
               break;
         }
      } catch (Exception e) {
         log.errorf(e, "Exception caught while processing command. Type is %s", type);
      }
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      switch (type) {
         case DATA_PLACEMENT_REQUEST:
            return new Object[]{(byte) type.ordinal()};
         case DATA_PLACEMENT_START:
            Object[] retVal = new Object[2 + (members == null ? 0 : members.length)];
            retVal[0] = (byte) type.ordinal();
            retVal[1] = roundId;
            if (members != null && members.length != 0) {
               System.arraycopy(members, 0, retVal, 2, members.length);
            }
            return retVal;
         case REMOTE_TOP_LIST_PHASE:
            return new Object[]{(byte) type.ordinal(), roundId, objectRequest};
         case OBJECT_LOOKUP_PHASE:
            return new Object[]{(byte) type.ordinal(), roundId, objectLookup};
         case SET_COOL_DOWN_TIME:
         case REPLICATION_DEGREE:
            return new Object[]{(byte) type.ordinal(), intValue};
      }
      throw new IllegalStateException("This should never happen!");
   }

   @SuppressWarnings("unchecked")
   @Override
   public void setParameters(int commandId, Object[] parameters) {
      type = Type.values()[(Byte) parameters[0]];

      switch (type) {
         case DATA_PLACEMENT_START:
            roundId = (Long) parameters[1];
            if (parameters.length > 2) {
               members = new Address[parameters.length - 2];
               for (int i = 0; i < members.length; ++i) {
                  members[i] = (Address) parameters[i + 2];
               }
            }
            break;
         case REMOTE_TOP_LIST_PHASE:
            roundId = (Long) parameters[1];
            objectRequest = (ObjectRequest) parameters[2];
            break;
         case OBJECT_LOOKUP_PHASE:
            roundId = (Long) parameters[1];
            objectLookup = (Map<Integer, ObjectLookup>) parameters[2];
            break;
         case SET_COOL_DOWN_TIME:
         case REPLICATION_DEGREE:
            intValue = (Integer) parameters[1];
            break;
      }
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   public static enum Type {
      /**
       * send to the coordinator to request the start of the algorithm
       */
      DATA_PLACEMENT_REQUEST,

      /**
       * coordinator broadcast this to start the algorithm
       */
      DATA_PLACEMENT_START,

      /**
       * contains the remote top list
       */
      REMOTE_TOP_LIST_PHASE,

      /**
       * contains the object lookup
       */
      OBJECT_LOOKUP_PHASE,

      /**
       * sets the new cool down period
       */
      SET_COOL_DOWN_TIME,

      /**
       * sets the new replication degree
       */
      REPLICATION_DEGREE
   }
}
