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

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class InternalGMURemovedCacheValue implements InternalGMUCacheValue {

   protected final EntryVersion version;

   public InternalGMURemovedCacheValue(EntryVersion version) {
      this.version = version;
   }

   @Override
   public EntryVersion getCreationVersion() {
      return null;
   }

   @Override
   public EntryVersion getMaximumValidVersion() {
      return null;
   }

   @Override
   public EntryVersion getMaximumTransactionVersion() {
      return null;
   }

   @Override
   public boolean isMostRecent() {
      return false;
   }

   @Override
   public InternalCacheValue getInternalCacheValue() {
      return null;
   }

   @Override
   public Object getValue() {
      return null;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new InternalGMURemovedCacheEntry(key, version);
   }

   @Override
   public boolean isExpired(long now) {
      return false;
   }

   @Override
   public boolean isExpired() {
      return false;
   }

   @Override
   public boolean canExpire() {
      return false;
   }

   @Override
   public long getCreated() {
      return -1;
   }

   @Override
   public long getLastUsed() {
      return -1;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getMaxIdle() {
      return -1;
   }

   @Override
   public String toString() {
      return "InternalGMURemovedCacheValue{" +
            "version=" + version +
            '}';
   }
}
