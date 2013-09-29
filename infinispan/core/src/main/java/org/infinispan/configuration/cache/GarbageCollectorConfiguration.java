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
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GarbageCollectorConfiguration {

   private final boolean enabled;
   private final int transactionThreshold;
   private final int versionGCMaxIdle;
   private final int l1GCInterval;
   private final int viewGCBackOff;

   public GarbageCollectorConfiguration(boolean enabled, int transactionThreshold, int versionGCMaxIdle,
                                        int l1GCInterval, int viewGCBackOff) {
      this.enabled = enabled;
      this.transactionThreshold = transactionThreshold;
      this.versionGCMaxIdle = versionGCMaxIdle;
      this.l1GCInterval = l1GCInterval;
      this.viewGCBackOff = viewGCBackOff;
   }

   public boolean enabled() {
      return enabled;
   }

   public int transactionThreshold() {
      return transactionThreshold;
   }

   public int versionGCMaxIdle() {
      return versionGCMaxIdle;
   }

   public int l1GCInterval() {
      return l1GCInterval;
   }

   public int viewGCBackOff() {
      return viewGCBackOff;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GarbageCollectorConfiguration that = (GarbageCollectorConfiguration) o;

      return enabled == that.enabled &&
            l1GCInterval == that.l1GCInterval &&
            versionGCMaxIdle == that.versionGCMaxIdle &&
            transactionThreshold == that.transactionThreshold &&
            viewGCBackOff == that.viewGCBackOff;

   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + transactionThreshold;
      result = 31 * result + versionGCMaxIdle;
      result = 31 * result + l1GCInterval;
      result = 31 * result + viewGCBackOff;
      return result;
   }

   @Override
   public String toString() {
      return "GarbageCollectorConfiguration{" +
            "enabled=" + enabled +
            ", transactionThreshold=" + transactionThreshold +
            ", versionGCMaxIdle=" + versionGCMaxIdle +
            ", l1GCInterval=" + l1GCInterval +
            ", viewGCBackOff=" + viewGCBackOff +
            '}';
   }
}
