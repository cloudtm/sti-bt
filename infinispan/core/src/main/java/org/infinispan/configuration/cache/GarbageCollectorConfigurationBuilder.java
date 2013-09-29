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
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GarbageCollectorConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<GarbageCollectorConfiguration> {

   private static final Log log = LogFactory.getLog(GarbageCollectorConfigurationBuilder.class);
   private boolean enabled = false;
   private int transactionThreshold = 1000;
   private int versionGCMaxIdle = 60;
   private int l1GCInterval = 30;
   private int viewGCBackOff = 120;

   public GarbageCollectorConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public GarbageCollectorConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public GarbageCollectorConfigurationBuilder transactionThreshold(int transactionThreshold) {
      this.transactionThreshold = transactionThreshold;
      return this;
   }

   public GarbageCollectorConfigurationBuilder versionGCMaxIdle(int versionGCMaxIdle) {
      this.versionGCMaxIdle = versionGCMaxIdle;
      return this;
   }

   public GarbageCollectorConfigurationBuilder l1GCInterval(int l1GCInterval) {
      this.l1GCInterval = l1GCInterval;
      return this;
   }

   public GarbageCollectorConfigurationBuilder viewGCBackOff(int viewGCBackOff) {
      this.viewGCBackOff = viewGCBackOff;
      return this;
   }

   @Override
   public Builder<GarbageCollectorConfiguration> read(GarbageCollectorConfiguration template) {
      this.enabled = template.enabled();
      this.transactionThreshold = template.transactionThreshold();
      this.versionGCMaxIdle = template.versionGCMaxIdle();
      this.l1GCInterval = template.l1GCInterval();
      this.viewGCBackOff = template.viewGCBackOff();
      return this;
   }

   @Override
   public String toString() {
      return "GarbageCollectorConfigurationBuilder{" +
            "enabled=" + enabled +
            ", transactionThreshold=" + transactionThreshold +
            ", versionGCMaxIdle=" + versionGCMaxIdle +
            ", l1GCInterval=" + l1GCInterval +
            ", viewGCBackOff=" + viewGCBackOff +
            '}';
   }

   @Override
   public void validate() {
      LockingConfiguration lockingConfiguration = locking().create();
      if (lockingConfiguration.isolationLevel() != IsolationLevel.SERIALIZABLE && enabled) {
         log.warnf("Garbage Collector only makes sense with GMU");
         enabled = false;
      }

      if (transactionThreshold <= 0) {
         throw new ConfigurationException("Transaction Threshold should be higher than zero");
      }
      if (versionGCMaxIdle < 0) {
         throw new ConfigurationException("Transaction Threshold should be higher or equals than zero");
      }
      if (l1GCInterval < 0) {
         throw new ConfigurationException("Transaction Threshold should be higher or equals than zero");
      }
      if (viewGCBackOff < 0) {
         throw new ConfigurationException("Transaction Threshold should be higher or equals than zero");
      }
   }

   @Override
   public GarbageCollectorConfiguration create() {
      return new GarbageCollectorConfiguration(enabled, transactionThreshold, versionGCMaxIdle, l1GCInterval,
                                               viewGCBackOff);
   }
}
