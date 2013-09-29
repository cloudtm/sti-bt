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
package org.infinispan.transaction.gmu;

import org.infinispan.CacheException;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class ValidationException extends CacheException {

   private static final String UNKNOWN_KEY = "UNKOWN";

   private final Object key;

   public ValidationException() {
      super();
      this.key = UNKNOWN_KEY;
   }

   public ValidationException(String msg, Object key) {
      super(msg);
      this.key = key;
   }

   public ValidationException(Throwable throwable) {
      super(throwable.getMessage());
      if (throwable instanceof ValidationException) {
         this.key = ((ValidationException) throwable).getKey();
      } else {
         this.key = UNKNOWN_KEY;
      }
   }

   public Object getKey() {
      return key;
   }
}
