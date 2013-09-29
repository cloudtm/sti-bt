/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.loaders.gmu;

import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.loaders.modifications.Store;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUStore extends Store {

   private final SerializableEntry entry;
   private final InternalEntryFactory factory;

   public GMUStore(SerializableEntry entry, InternalEntryFactory factory) {
      super(null);
      this.entry = entry;
      this.factory = factory;
   }

   @Override
   public InternalCacheEntry getStoredEntry() {

      return factory.create(entry);
   }

   @Override
   public String toString() {
      return "GMUStore{" +
            "entry=" + entry +
            ", factory=" + factory +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      GMUStore gmuStore = (GMUStore) o;

      return !(entry != null ? !entry.equals(gmuStore.entry) : gmuStore.entry != null) &&
            !(factory != null ? !factory.equals(gmuStore.factory) : gmuStore.factory != null);

   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (entry != null ? entry.hashCode() : 0);
      result = 31 * result + (factory != null ? factory.hashCode() : 0);
      return result;
   }
}
