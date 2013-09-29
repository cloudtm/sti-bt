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

package org.infinispan.remoting.responses;

import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.util.concurrent.TimeoutException;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface TotalOrderResponseFilter extends ResponseFilter {

   /**
    * Validates the filter state. If it has no responses or some member didn't reply, it must throw a {@link
    * TimeoutException}
    *
    * @throws TimeoutException when a member didn't give a response.
    */
   void validate() throws TimeoutException;

}
