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

import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.Builder;
import org.infinispan.dataplacement.lookup.ObjectLookupFactory;
import org.infinispan.util.TypedProperties;

import java.util.Properties;

/**
 * Builds the Data Placement configuration
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DataPlacementConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<DataPlacementConfiguration> {

   private boolean enabled = false;
   private ObjectLookupFactory objectLookupFactory;
   private int coolDownTime = 30000; //30 seconds by default
   private int maxNumberOfKeysToRequest = 500; //500 keys by default? is too high? too low?
   private Properties properties = new Properties();

   protected DataPlacementConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public DataPlacementConfigurationBuilder objectLookupFactory(ObjectLookupFactory objectLookupFactory) {
      this.objectLookupFactory = objectLookupFactory;
      return this;
   }

   public DataPlacementConfigurationBuilder withProperties(Properties properties) {
      this.properties = properties;
      return this;
   }

   public DataPlacementConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   public DataPlacementConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   public DataPlacementConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public DataPlacementConfigurationBuilder coolDownTime(int coolDownTime) {
      this.coolDownTime = coolDownTime;
      return this;
   }

   public DataPlacementConfigurationBuilder addProperty(String key, String value) {
      properties.put(key, value);
      return this;
   }

   public DataPlacementConfigurationBuilder maxNumberOfKeysToRequest(int maxNumberOfKeysToRequest) {
      this.maxNumberOfKeysToRequest = maxNumberOfKeysToRequest;
      return this;
   }

   @Override
   public void validate() {
      if (!enabled) {
         return;
      }
      if (!clustering().create().cacheMode().isDistributed()) {
         throw new ConfigurationException("Data Placement Optimizer only works in distributed mode");
      }
      if (objectLookupFactory == null) {
         throw new ConfigurationException("Object Lookup Factory cannot be null");
      }
      if (coolDownTime < 1000) {
         throw new ConfigurationException("Cool Down time must be higher or equals to 1000 milliseconds");
      }
      if (maxNumberOfKeysToRequest < 1) {
         throw new ConfigurationException("The max number of keys to request should be higher than 0");
      }
   }

   @Override
   public DataPlacementConfiguration create() {
      return new DataPlacementConfiguration(TypedProperties.toTypedProperties(properties), enabled, coolDownTime,
                                            objectLookupFactory, maxNumberOfKeysToRequest);
   }

   @Override
   public Builder<DataPlacementConfiguration> read(DataPlacementConfiguration template) {
      this.enabled = template.enabled();
      this.coolDownTime = template.coolDownTime();
      this.maxNumberOfKeysToRequest = template.maxNumberOfKeysToRequest();
      this.objectLookupFactory = template.objectLookupFactory();
      this.properties = template.properties();
      return this;
   }
}
