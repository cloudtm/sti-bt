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

import org.infinispan.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.dataplacement.lookup.ObjectLookupFactory;
import org.infinispan.util.TypedProperties;

/**
 * Configures the Data Placement optimization
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementConfiguration extends AbstractTypedPropertiesConfiguration {

   private final boolean enabled;
   private final int coolDownTime;
   private final ObjectLookupFactory objectLookupFactory;
   private final int maxNumberOfKeysToRequest;

   protected DataPlacementConfiguration(TypedProperties properties, boolean enabled, int coolDownTime,
                                        ObjectLookupFactory objectLookupFactory, int maxNumberOfKeysToRequest) {
      super(properties);
      this.enabled = enabled;
      this.coolDownTime = coolDownTime;
      this.objectLookupFactory = objectLookupFactory;
      this.maxNumberOfKeysToRequest = maxNumberOfKeysToRequest;
   }

   public ObjectLookupFactory objectLookupFactory() {
      return objectLookupFactory;
   }

   public boolean enabled() {
      return enabled;
   }

   public int coolDownTime() {
      return coolDownTime;
   }

   public int maxNumberOfKeysToRequest() {
      return maxNumberOfKeysToRequest;
   }

   @Override
   public String toString() {
      return "DataPlacementConfiguration{" +
            "enabled=" + enabled +
            ", coolDownTime=" + coolDownTime +
            ", objectLookupFactory=" + objectLookupFactory +
            ", maxNumberOfKeysToRequest=" + maxNumberOfKeysToRequest +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof DataPlacementConfiguration)) return false;
      if (!super.equals(o)) return false;

      DataPlacementConfiguration that = (DataPlacementConfiguration) o;

      return coolDownTime == that.coolDownTime && maxNumberOfKeysToRequest == that.maxNumberOfKeysToRequest &&
            enabled == that.enabled &&
            !(objectLookupFactory != null ? !objectLookupFactory.equals(that.objectLookupFactory) :
                    that.objectLookupFactory != null);

   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (enabled ? 1 : 0);
      result = 31 * result + coolDownTime;
      result = 31 * result + maxNumberOfKeysToRequest;
      result = 31 * result + (objectLookupFactory != null ? objectLookupFactory.hashCode() : 0);
      return result;
   }
}
