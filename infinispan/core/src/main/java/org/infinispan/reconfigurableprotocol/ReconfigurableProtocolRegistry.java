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
package org.infinispan.reconfigurableprotocol;

import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.reconfigurableprotocol.exception.AlreadyRegisterProtocolException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class responsible to keep all the possible ReconfigurableProtocol for this cache. It manages internally the Id of
 * each protocol
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ReconfigurableProtocolRegistry {

   private static final Log log = LogFactory.getLog(ReconfigurableProtocolRegistry.class);
   private final Map<String, ReconfigurableProtocol> idsToProtocol;
   private InterceptorChain interceptorChain;

   public ReconfigurableProtocolRegistry() {
      this.idsToProtocol = new ConcurrentHashMap<String, ReconfigurableProtocol>();
   }

   /**
    * injects the interceptor chain in order to add new replication protocols
    *
    * @param interceptorChain the interceptor chain
    */
   public final void inject(InterceptorChain interceptorChain) {
      this.interceptorChain = interceptorChain;
   }

   /**
    * returns the current ids and replication protocols currently register
    *
    * @return the current ids and replication protocols currently register
    */
   public final Collection<ReconfigurableProtocol> getAllAvailableProtocols() {
      return Collections.unmodifiableCollection(idsToProtocol.values());
   }

   /**
    * registers a new protocol to this registry and set it an id
    *
    * @param protocol the new protocol
    * @throws AlreadyRegisterProtocolException
    *          if the protocol is already register
    */
   public final synchronized void registerNewProtocol(ReconfigurableProtocol protocol)
         throws AlreadyRegisterProtocolException {

      if (protocol == null) {
         log.warn("Tried to register a new replication protocol, but it is null");
         throw new NullPointerException("Trying to register a null protocol");
      } else if (idsToProtocol.containsKey(protocol.getUniqueProtocolName())) {
         log.warnf("Tried to register a new replication protocol but it is already register. Protocol is %s",
                   protocol.getUniqueProtocolName());
         throw new AlreadyRegisterProtocolException(protocol);
      }

      idsToProtocol.put(protocol.getUniqueProtocolName(), protocol);
      protocol.bootstrapProtocol();
      interceptorChain.registerNewProtocol(protocol);

      if (log.isDebugEnabled()) {
         log.debugf("Register successfully the new replication protocol %s", protocol.getUniqueProtocolName());
      }
   }

   /**
    * returns the protocol associated to this protocol id
    *
    * @param protocolId the protocol id
    * @return the reconfigurable protocol instance or null if the protocol id does not exists
    */
   public final ReconfigurableProtocol getProtocolById(String protocolId) {
      return idsToProtocol.get(protocolId);
   }
}
