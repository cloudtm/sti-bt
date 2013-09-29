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

package org.infinispan.configuration.cache;


/**
 * @author Diego Didona - didona@gsd.inesc-id.pt
 */
public class CustomStatsConfiguration {

   private final boolean sampleServiceTimes;
   private final boolean gmuWaitingTimeEnabled;

   CustomStatsConfiguration(boolean sampleServiceTimes, boolean gmuWaitingTimeEnabled) {
      this.sampleServiceTimes = sampleServiceTimes;
      this.gmuWaitingTimeEnabled = gmuWaitingTimeEnabled;
   }

   public boolean sampleServiceTimes() {
      return sampleServiceTimes;
   }

   public boolean gmuWaitingTimeEnabled() {
      return gmuWaitingTimeEnabled;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomStatsConfiguration that = (CustomStatsConfiguration) o;

      return gmuWaitingTimeEnabled == that.gmuWaitingTimeEnabled &&
            sampleServiceTimes == that.sampleServiceTimes;

   }

   @Override
   public int hashCode() {
      int result = (sampleServiceTimes ? 1 : 0);
      result = 31 * result + (gmuWaitingTimeEnabled ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return "CustomStatsConfiguration{" +
            "sampleServiceTimes=" + sampleServiceTimes +
            ", gmuWaitingTimeEnabled=" + gmuWaitingTimeEnabled +
            '}';
   }
}
