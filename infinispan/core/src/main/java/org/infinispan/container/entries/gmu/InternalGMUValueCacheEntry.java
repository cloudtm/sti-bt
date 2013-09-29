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

import org.infinispan.container.DataContainer;
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
public class InternalGMUValueCacheEntry implements InternalGMUCacheEntry {

   private final InternalCacheEntry internalCacheEntry;
   private EntryVersion creationVersion;
   private final EntryVersion maxTxVersion;
   private EntryVersion maxValidVersion;
   private final boolean mostRecent;

   public InternalGMUValueCacheEntry(InternalCacheEntry internalCacheEntry, EntryVersion maxTxVersion,
                                     boolean mostRecent, EntryVersion creationVersion, EntryVersion maxValidVersion) {
      this.internalCacheEntry = internalCacheEntry;
      this.creationVersion = creationVersion;
      this.maxTxVersion = maxTxVersion;
      this.maxValidVersion = maxValidVersion;
      this.mostRecent = mostRecent;
   }

   @Override
   public boolean isExpired(long now) {
      return internalCacheEntry.isExpired(now);
   }

   @Override
   public boolean isExpired() {
      return internalCacheEntry.isExpired();
   }

   @Override
   public boolean canExpire() {
      return internalCacheEntry.canExpire();
   }

   @Override
   public void setMaxIdle(long maxIdle) {
      internalCacheEntry.setMaxIdle(maxIdle);
   }

   @Override
   public void setLifespan(long lifespan) {
      internalCacheEntry.setLifespan(lifespan);
   }

   @Override
   public long getCreated() {
      return internalCacheEntry.getCreated();
   }

   @Override
   public long getLastUsed() {
      return internalCacheEntry.getLastUsed();
   }

   @Override
   public long getExpiryTime() {
      return internalCacheEntry.getExpiryTime();
   }

   @Override
   public void touch() {
      internalCacheEntry.touch();
   }

   @Override
   public void touch(long currentTimeMillis) {
      internalCacheEntry.touch(currentTimeMillis);
   }

   @Override
   public void reincarnate() {
      internalCacheEntry.reincarnate();
   }

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return new InternalGMUValueCacheValue(internalCacheEntry.toInternalCacheValue(), maxTxVersion, mostRecent,
                                            creationVersion, maxValidVersion);
   }

   @Override
   public InternalCacheEntry clone() {
      try {
         return (InternalCacheEntry) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("This should never happen");
      }
   }

   @Override
   public boolean isNull() {
      return internalCacheEntry.isNull();
   }

   @Override
   public boolean isChanged() {
      return internalCacheEntry.isChanged();
   }

   @Override
   public boolean isCreated() {
      return internalCacheEntry.isCreated();
   }

   @Override
   public boolean isRemoved() {
      return internalCacheEntry.isRemoved();
   }

   @Override
   public boolean isEvicted() {
      return internalCacheEntry.isEvicted();
   }

   @Override
   public boolean isValid() {
      return internalCacheEntry.isValid();
   }

   @Override
   public boolean isLoaded() {
      return internalCacheEntry.isLoaded();
   }

   @Override
   public Object getKey() {
      return internalCacheEntry.getKey();
   }

   @Override
   public Object getValue() {
      return internalCacheEntry.getValue();
   }

   @Override
   public long getLifespan() {
      return internalCacheEntry.getLifespan();
   }

   @Override
   public long getMaxIdle() {
      return internalCacheEntry.getMaxIdle();
   }

   @Override
   public Object setValue(Object value) {
      return internalCacheEntry.setValue(value);
   }

   @Override
   public void commit(DataContainer container, EntryVersion newVersion) {
      internalCacheEntry.commit(container, newVersion);
   }

   @Override
   public void rollback() {
      internalCacheEntry.rollback();
   }

   @Override
   public void setCreated(boolean created) {
      internalCacheEntry.setCreated(created);
   }

   @Override
   public void setRemoved(boolean removed) {
      internalCacheEntry.setRemoved(removed);
   }

   @Override
   public void setEvicted(boolean evicted) {
      internalCacheEntry.setEvicted(evicted);
   }

   @Override
   public void setValid(boolean valid) {
      internalCacheEntry.setValid(valid);
   }

   @Override
   public void setLoaded(boolean loaded) {
      internalCacheEntry.setLoaded(loaded);
   }

   @Override
   public void setChanged(boolean b) {
      internalCacheEntry.setChanged(b);
   }

   @Override
   public boolean isLockPlaceholder() {
      return internalCacheEntry.isLockPlaceholder();
   }

   @Override
   public boolean undelete(boolean doUndelete) {
      return internalCacheEntry.undelete(doUndelete);
   }

   @Override
   public int hashCode() {
      return internalCacheEntry.hashCode();
   }

   @Override
   public EntryVersion getVersion() {
      return internalCacheEntry.getVersion();
   }

   @Override
   public void setVersion(EntryVersion version) {
      //no-op ==> this is the internal cache entry in data container.
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
   public InternalCacheEntry getInternalCacheEntry() {
      return internalCacheEntry;
   }

   @Override
   public void setCreationVersion(EntryVersion entryVersion) {
      this.creationVersion = entryVersion;
   }

   @Override
   public void setMaximumValidVersion(EntryVersion version) {
      this.maxValidVersion = version;
   }

   @Override
   public String toString() {
      return "InternalGMUValueCacheEntry{" +
            "key=" + internalCacheEntry.getKey() +
            ", version=" + internalCacheEntry.getVersion() +
            ", value=" + internalCacheEntry.getValue() +
            ", creationVersion=" + creationVersion +
            ", maxTxVersion=" + maxTxVersion +
            ", maxValidVersion=" + maxValidVersion +
            ", mostRecent=" + mostRecent +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<InternalGMUValueCacheEntry> {

      @Override
      public Set<Class<? extends InternalGMUValueCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends InternalGMUValueCacheEntry>>asSet(InternalGMUValueCacheEntry.class);
      }

      @Override
      public void writeObject(ObjectOutput output, InternalGMUValueCacheEntry object) throws IOException {
         output.writeObject(object.internalCacheEntry);
         output.writeObject(object.creationVersion);
         output.writeObject(object.maxTxVersion);
         output.writeObject(object.maxValidVersion);
         output.writeBoolean(object.mostRecent);
      }

      @Override
      public InternalGMUValueCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         InternalCacheEntry internalCacheEntry = (InternalCacheEntry) input.readObject();
         EntryVersion creationVersion = (EntryVersion) input.readObject();
         EntryVersion maxTxVersion = (EntryVersion) input.readObject();
         EntryVersion maxValidVersion = (EntryVersion) input.readObject();
         boolean mostRecent = input.readBoolean();
         return new InternalGMUValueCacheEntry(internalCacheEntry, maxTxVersion, mostRecent, creationVersion, maxValidVersion
         );
      }

      @Override
      public Integer getId() {
         return Ids.INTERNAL_GMU_ENTRY;
      }
   }
}
