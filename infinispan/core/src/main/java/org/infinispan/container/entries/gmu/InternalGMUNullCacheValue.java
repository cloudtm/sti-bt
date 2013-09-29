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
public class InternalGMUNullCacheValue extends InternalGMURemovedCacheValue {

   private final boolean mostRecent;
   private final EntryVersion creationVersion;
   private final EntryVersion maxValidVersion;
   private final EntryVersion maxTxVersion;


   public InternalGMUNullCacheValue(EntryVersion version, EntryVersion maxTxVersion, boolean mostRecent,
                                    EntryVersion creationVersion, EntryVersion maxValidVersion) {
      super(version);
      this.mostRecent = mostRecent;
      this.creationVersion = creationVersion;
      this.maxValidVersion = maxValidVersion;
      this.maxTxVersion = maxTxVersion;
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
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new InternalGMUNullCacheEntry(key, version, maxTxVersion, mostRecent, creationVersion, maxValidVersion);
   }

   @Override
   public String toString() {
      return "InternalGMUNullCacheValue{" +
            "mostRecent=" + mostRecent +
            ", creationVersion=" + creationVersion +
            ", maxValidVersion=" + maxValidVersion +
            ", maxTxVersion=" + maxTxVersion +
            "}";
   }

   public static class Externalizer extends AbstractExternalizer<InternalGMUNullCacheValue> {

      @Override
      public Set<Class<? extends InternalGMUNullCacheValue>> getTypeClasses() {
         return Util.<Class<? extends InternalGMUNullCacheValue>>asSet(InternalGMUNullCacheValue.class);
      }

      @Override
      public void writeObject(ObjectOutput output, InternalGMUNullCacheValue object) throws IOException {
         output.writeObject(object.version);
         output.writeObject(object.creationVersion);
         output.writeObject(object.maxTxVersion);
         output.writeObject(object.maxValidVersion);
         output.writeBoolean(object.mostRecent);
      }

      @Override
      public InternalGMUNullCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         EntryVersion version = (EntryVersion) input.readObject();
         EntryVersion creationVersion = (EntryVersion) input.readObject();
         EntryVersion maxTxVersion = (EntryVersion) input.readObject();
         EntryVersion maxValidVersion = (EntryVersion) input.readObject();
         boolean mostRecent = input.readBoolean();
         return new InternalGMUNullCacheValue(version, maxTxVersion, mostRecent, creationVersion, maxValidVersion);
      }

      @Override
      public Integer getId() {
         return Ids.INTERNAL_GMU_NULL_VALUE;
      }
   }
}
