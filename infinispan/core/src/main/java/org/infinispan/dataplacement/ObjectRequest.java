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
package org.infinispan.dataplacement;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Contains the information of the remote and local accesses for a member. This information is sent to the primary owner
 * in orde to calculate the best placement for the objects
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ObjectRequest implements Serializable {

   private final Map<Object, Long> remoteAccesses;
   private final Map<Object, Long> localAccesses;

   public ObjectRequest(Map<Object, Long> remoteAccesses, Map<Object, Long> localAccesses) {
      this.remoteAccesses = remoteAccesses;
      this.localAccesses = localAccesses;
   }

   public Map<Object, Long> getRemoteAccesses() {
      return remoteAccesses == null ? Collections.<Object, Long>emptyMap() : remoteAccesses;
   }

   public Map<Object, Long> getLocalAccesses() {
      return localAccesses == null ? Collections.<Object, Long>emptyMap() : localAccesses;
   }

   @Override
   public String toString() {
      return "ObjectRequest{" +
            "remoteAccesses=" + (remoteAccesses == null ? 0 : remoteAccesses.size()) +
            ", localAccesses=" + (localAccesses == null ? 0 : localAccesses.size()) +
            '}';
   }

   public String toString(boolean detailed) {
      if (detailed) {
         return "ObjectRequest{" +
               "remoteAccesses=" + remoteAccesses +
               ", localAccesses=" + localAccesses +
               '}';
      }
      return toString();
   }
}
