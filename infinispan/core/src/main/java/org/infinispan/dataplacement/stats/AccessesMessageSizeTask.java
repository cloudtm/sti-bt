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

import org.infinispan.dataplacement.AccessesManager;
import org.infinispan.dataplacement.ObjectRequest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Task that sums the size of all object requests
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class AccessesMessageSizeTask implements Runnable {

   private static final Log log = LogFactory.getLog(AccessesMessageSizeTask.class);
   private final AccessesManager accessesManager;
   private final Stats stats;

   public AccessesMessageSizeTask(Stats stats, AccessesManager accessesManager) {
      this.accessesManager = accessesManager;
      this.stats = stats;
   }

   @Override
   public void run() {
      try {
         int size = 0;
         ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

         for (ObjectRequest objectRequest : accessesManager.getAccesses()) {
            objectOutputStream.writeObject(objectRequest);
            objectOutputStream.flush();

            size += byteArrayOutputStream.toByteArray().length;

            byteArrayOutputStream.reset();
         }

         stats.accessesSize(size);
         objectOutputStream.close();
      } catch (IOException e) {
         log.warn("Error calculating object requests size", e);
      }
   }
}
