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
package org.infinispan.tx.gmu;

import org.infinispan.Cache;
import org.infinispan.distribution.MagicKey;

import java.util.Arrays;
import java.util.Random;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.distribution.DistributionTestHelper.hasOwnersIgnoringOrder;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUMagicKey extends MagicKey {

   public GMUMagicKey(String name, Cache<?, ?>... owners) {
      super();
      if (owners == null || owners.length == 0) {
         throw new IllegalArgumentException("Owners cannot be empty");
      }
      this.address = addressOf(owners[0]).toString();
      Random r = new Random();
      Object dummy;
      int attemptsLeft = 1000;
      do {
         // create a dummy object with this hashcode
         final int hc = r.nextInt();
         dummy = new Integer(hc);
         attemptsLeft--;

      } while (!hasOwnersIgnoringOrder(dummy, owners) && attemptsLeft >= 0);

      if (attemptsLeft < 0) {
         throw new IllegalStateException("Could not find any key owned by " + Arrays.toString(owners));
      }
      // we have found a hashcode that works!
      this.hashcode = dummy.hashCode();
      this.name = name;
   }
}
