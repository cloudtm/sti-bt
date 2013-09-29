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
package org.infinispan.container.entries.gmu;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
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
 * @author Pedro Ruivo
 * @since 5.2
 */
public class InternalGMUValueCacheValue implements InternalGMUCacheValue {

   private final InternalCacheValue internalCacheValue;
   private final EntryVersion creationVersion;
   private final EntryVersion maxTxVersion;
   private final EntryVersion maxValidVersion;
   private final boolean mostRecent;

   public InternalGMUValueCacheValue(InternalCacheValue internalCacheValue, EntryVersion maxTxVersion,
                                     boolean mostRecent, EntryVersion creationVersion, EntryVersion maxValidVersion) {
      this.internalCacheValue = internalCacheValue;
      this.creationVersion = creationVersion;
      this.maxTxVersion = maxTxVersion;
      this.maxValidVersion = maxValidVersion;
      this.mostRecent = mostRecent;
   }

   @Override
   public Object getValue() {
      return internalCacheValue.getValue();
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new InternalGMUValueCacheEntry(internalCacheValue.toInternalCacheEntry(key), maxTxVersion, mostRecent,
                                            creationVersion, maxValidVersion);
   }

   @Override
   public boolean isExpired(long now) {
      return internalCacheValue.isExpired(now);
   }

   @Override
   public boolean isExpired() {
      return internalCacheValue.isExpired();
   }

   @Override
   public boolean canExpire() {
      return internalCacheValue.canExpire();
   }

   @Override
   public long getCreated() {
      return internalCacheValue.getCreated();
   }

   @Override
   public long getLastUsed() {
      return internalCacheValue.getLastUsed();
   }

   @Override
   public long getLifespan() {
      return internalCacheValue.getLifespan();
   }

   @Override
   public long getMaxIdle() {
      return internalCacheValue.getMaxIdle();
   }

   @Override
   public EntryVersion getCreationVersion() {
      return creationVersion;
   }

   @Override
   public EntryVersion getMaximumValidVersion() {
      return maxValidVersion;
   }

   @Override
   public EntryVersion getMaximumTransactionVersion() {
      return maxTxVersion;
   }

   @Override
   public boolean isMostRecent() {
      return mostRecent;
   }

   @Override
   public InternalCacheValue getInternalCacheValue() {
      return internalCacheValue;
   }

   @Override
   public String toString() {
      return "InternalGMUValueCacheValue{" +
            "internalCacheValue=" + internalCacheValue +
            ", creationVersion=" + creationVersion +
            ", maxTxVersion=" + maxTxVersion +
            ", maxValidVersion=" + maxValidVersion +
            ", mostRecent=" + mostRecent +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<InternalGMUValueCacheValue> {

      @Override
      public Set<Class<? extends InternalGMUValueCacheValue>> getTypeClasses() {
         return Util.<Class<? extends InternalGMUValueCacheValue>>asSet(InternalGMUValueCacheValue.class);
      }

      @Override
      public void writeObject(ObjectOutput output, InternalGMUValueCacheValue object) throws IOException {
         output.writeObject(object.internalCacheValue);
         output.writeObject(object.creationVersion);
         output.writeObject(object.maxTxVersion);
         output.writeObject(object.maxValidVersion);
         output.writeBoolean(object.mostRecent);
      }

      @Override
      public InternalGMUValueCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         InternalCacheValue internalCacheValue = (InternalCacheValue) input.readObject();
         EntryVersion creationVersion = (EntryVersion) input.readObject();
         EntryVersion maxTxVersion = (EntryVersion) input.readObject();
         EntryVersion maxValidVersion = (EntryVersion) input.readObject();
         boolean mostRecent = input.readBoolean();
         return new InternalGMUValueCacheValue(internalCacheValue, maxTxVersion, mostRecent, creationVersion,
                                               maxValidVersion
         );
      }

      @Override
      public Integer getId() {
         return Ids.INTERNAL_GMU_VALUE;
      }
   }
}
