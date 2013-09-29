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
package org.infinispan.stats;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Diego Didona
 * @since 5.2
 */
public class PiggyBackStat {

   private long waitTime;

   public long getWaitTime() {
      return waitTime;
   }

   public void setWaitTime(long waitTime) {
      this.waitTime = waitTime;
   }

   public PiggyBackStat(long waitTime) {
      this.waitTime = waitTime;
   }

   public static class Externalizer extends AbstractExternalizer<PiggyBackStat> {
      @Override
      public void writeObject(ObjectOutput output, PiggyBackStat piggyBackStat) throws IOException{
         output.writeLong(piggyBackStat.waitTime);
      }

      @Override
      public PiggyBackStat readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new PiggyBackStat(input.readLong());
      }

      @Override
      public Integer getId() {
         return Ids.PIGGY_BACK_RESPONSE;
      }

      @Override
      public Set<Class<? extends PiggyBackStat>> getTypeClasses() {
         return Util.<Class<? extends PiggyBackStat>>asSet(PiggyBackStat.class);
      }
   }

   @Override
   public String toString() {
      return "PiggyBackStat{" +
            "waitTime=" + waitTime +
            '}';
   }
}
