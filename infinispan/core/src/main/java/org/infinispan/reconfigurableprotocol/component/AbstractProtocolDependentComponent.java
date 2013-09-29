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
package org.infinispan.reconfigurableprotocol.component;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.reconfigurableprotocol.ProtocolTable;
import org.infinispan.reconfigurableprotocol.exception.AlreadyExistingComponentProtocolException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Abstract class with the main behaviour to delegate method invocations to the correct component instance, depending of
 * the replication protocol used
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class AbstractProtocolDependentComponent<T> {

   private final ConcurrentMap<String, T> protocolDependentComponent = new ConcurrentHashMap<String, T>();
   private ProtocolTable protocolTable;

   @Inject
   public final void inject(ProtocolTable protocolTable) {
      this.protocolTable = protocolTable;
   }

   /**
    * adds a new component for a replication protocol
    *
    * @param protocolId the protocol Id
    * @param component  the component
    * @throws AlreadyExistingComponentProtocolException
    *          if the protocol already has a component
    */
   public final void add(String protocolId, T component) throws AlreadyExistingComponentProtocolException {
      if (protocolDependentComponent.putIfAbsent(protocolId, component) != null) {
         throw new AlreadyExistingComponentProtocolException("Protocol " + protocolId + " has already a component register");
      }
   }

   /**
    * returns the component depending of the current protocol Id
    *
    * @return the component depending of the current protocol Id
    */
   public final T get() {
      return protocolDependentComponent.get(protocolTable.getThreadProtocolId());
   }
}
