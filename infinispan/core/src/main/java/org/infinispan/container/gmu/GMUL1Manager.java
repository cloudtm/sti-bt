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
package org.infinispan.container.gmu;

import org.infinispan.distribution.L1Manager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.NoOpFuture;

import java.util.Collection;
import java.util.concurrent.Future;

/**
 * GMU does not need to keep track of the requestors
 *
 * @author Hugo Pimentel
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUL1Manager implements L1Manager {

   @Override
   public void addRequestor(Object key, Address requestor) {
      //no-op
   }

   @Override
   public Future<Object> flushCacheWithSimpleFuture(Collection<Object> keys, Object retval, Address origin, boolean assumeOriginKeptEntryInL1) {
      return new NoOpFuture<Object>(retval);
   }

   @Override
   public Future<Object> flushCache(Collection<Object> key, Address origin, boolean assumeOriginKeptEntryInL1) {
      return null;
   }
}
