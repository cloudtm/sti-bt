/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
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
package org.infinispan.config;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static java.util.Collections.synchronizedCollection;

/**
 * Original author are missing...
 * 
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 */
public class QueryableDataContainer implements DataContainer {
	
	private static DataContainer delegate;
	
	public static void setDelegate(DataContainer delegate) {
	   QueryableDataContainer.delegate = delegate;
   }
	
	private final Collection<String> loggedOperations;
	
	public void setFoo(String foo) {
		loggedOperations.add("setFoo(" + foo + ")");
	}

	public QueryableDataContainer() {
	   this.loggedOperations = synchronizedCollection(new ArrayList<String>());
   }
	
	@Override
	public Iterator<InternalCacheEntry> iterator() {
		loggedOperations.add("iterator()");
		return delegate.iterator();
	}

	@Override
	public InternalCacheEntry get(Object k, EntryVersion version) {
		loggedOperations.add("get(" + k + ")" );
		return delegate.get(k, null);
	}

	@Override
	public InternalCacheEntry peek(Object k, EntryVersion version) {
		loggedOperations.add("peek(" + k + ")" );
		return delegate.peek(k, null);
	}

	@Override
	public void put(Object k, Object v, EntryVersion ev, long lifespan, long maxIdle) {
		loggedOperations.add("put(" + k + ", " + v + ", " + lifespan + ", " + maxIdle + ")");
		delegate.put(k, v, ev, lifespan, maxIdle);
	}

	@Override
	public boolean containsKey(Object k, EntryVersion version) {
		loggedOperations.add("containsKey(" + k + ")" );
		return delegate.containsKey(k, null);
	}

	@Override
	public InternalCacheEntry remove(Object k, EntryVersion version) {
		loggedOperations.add("remove(" + k + ")" );
		return delegate.remove(k, null);
	}

	@Override
	public int size(EntryVersion version) {
		loggedOperations.add("size()" );
		return delegate.size(null);
	}

	@Override
	public void clear() {
		loggedOperations.add("clear()" );
		delegate.clear();
	}

	@Override
	public Set<Object> keySet(EntryVersion version) {
		loggedOperations.add("keySet()" );
		return delegate.keySet(null);
	}

	@Override
	public Collection<Object> values(EntryVersion version) {
		loggedOperations.add("values()" );
		return delegate.values(null);
	}

	@Override
	public Set<InternalCacheEntry> entrySet(EntryVersion version) {
		loggedOperations.add("entrySet()" );
		return delegate.entrySet(null);
	}

	@Override
	public void purgeExpired() {
		loggedOperations.add("purgeExpired()" );
		delegate.purgeExpired();
	}

   @Override
   public void clear(EntryVersion version) {
      loggedOperations.add("clear(" + version + ")");
      delegate.clear(version);
   }
	
   @Override
   public boolean dumpTo(String filePath) {
      loggedOperations.add("dumpTo(" + filePath + ")");
      return delegate.dumpTo(filePath);
   }

   @Override
   public void gc(EntryVersion minimumVersion) {
      delegate.gc(minimumVersion);
   }

   public Collection<String> getLoggedOperations() {
	   return loggedOperations;
   }
}
