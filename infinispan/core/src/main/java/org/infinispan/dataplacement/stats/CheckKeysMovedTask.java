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
package org.infinispan.dataplacement.stats;

import org.infinispan.dataplacement.ObjectPlacementManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Task that checks the number of keys wrongly moved out
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class CheckKeysMovedTask implements Runnable {

   private final Set<Object> keysMoved;
   private final Set<Object> keysToMove;
   private final Stats stats;
   private final ConsistentHash consistentHash;
   private final Address localAddress;

   public CheckKeysMovedTask(Collection<Object> keysMoved, ObjectPlacementManager manager, Stats stats,
                             ConsistentHash consistentHash, Address localAddress) {
      this.consistentHash = consistentHash;
      this.localAddress = localAddress;
      this.keysMoved = new HashSet<Object>(keysMoved);
      this.keysToMove = new HashSet<Object>(manager.getKeysToMove());
      this.stats = stats;
   }

   @Override
   public void run() {
      keysMoved.removeAll(keysToMove);
      int errors = 0;
      for (Object key : keysMoved) {
         if (localAddress.equals(consistentHash.locatePrimaryOwner(key))) {
            errors++;
         }
      }
      stats.wrongKeyMovedErrors(errors);
   }
}
