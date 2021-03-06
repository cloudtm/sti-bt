/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan;

import org.infinispan.atomic.Delta;
import org.infinispan.batch.BatchContainer;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.stats.Stats;

import org.infinispan.transaction.TransactionTable;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

/**
 * Similar to {@link org.infinispan.AbstractDelegatingCache}, but for {@link AdvancedCache}.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.AbstractDelegatingCache
 */
public class AbstractDelegatingAdvancedCache<K, V> extends AbstractDelegatingCache<K, V> implements AdvancedCache<K, V> {

   protected final AdvancedCache<K, V> cache;
   private final AdvancedCacheWrapper<K, V> wrapper;

   public AbstractDelegatingAdvancedCache(final AdvancedCache<K, V> cache) {
      this(cache, new AdvancedCacheWrapper<K, V>() {
         @Override
         public AdvancedCache<K, V> wrap(AdvancedCache<K, V> cache) {
            return new AbstractDelegatingAdvancedCache<K, V>(cache);
         }
      });
   }

   public AbstractDelegatingAdvancedCache(
         AdvancedCache<K, V> cache, AdvancedCacheWrapper<K, V> wrapper) {
      super(cache);
      this.cache = cache;
      this.wrapper = wrapper;
   }

   @Override
   public void addInterceptor(CommandInterceptor i, int position) {
      cache.addInterceptor(i, position);
   }

   @Override
   public boolean addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor) {
      return cache.addInterceptorAfter(i, afterInterceptor);
   }

   @Override
   public boolean addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor) {
      return cache.addInterceptorBefore(i, beforeInterceptor);
   }

   @Override
   public void removeInterceptor(int position) {
      cache.removeInterceptor(position);
   }

   @Override
   public void removeInterceptor(Class<? extends CommandInterceptor> interceptorType) {
      cache.removeInterceptor(interceptorType);
   }

   @Override
   public AdvancedCache<K, V> getAdvancedCache() {
      //We need to override the super implementation which returns to the decorated cache;
      //otherwise the current operation breaks out of the selected ClassLoader.
      return this;
   }

   @Override
   public List<CommandInterceptor> getInterceptorChain() {
      return cache.getInterceptorChain();
   }

   @Override
   public EvictionManager getEvictionManager() {
      return cache.getEvictionManager();
   }

   @Override
   public ComponentRegistry getComponentRegistry() {
      return cache.getComponentRegistry();
   }

   @Override
   public DistributionManager getDistributionManager() {
      return cache.getDistributionManager();
   }

   @Override
   public RpcManager getRpcManager() {
      return cache.getRpcManager();
   }

   @Override
   public BatchContainer getBatchContainer() {
      return cache.getBatchContainer();
   }

   @Override
   public InvocationContextContainer getInvocationContextContainer() {
      return cache.getInvocationContextContainer();
   }

   @Override
   public DataContainer getDataContainer() {
      return cache.getDataContainer();
   }

   @Override
   public TransactionManager getTransactionManager() {
      return cache.getTransactionManager();
   }

   @Override
   public LockManager getLockManager() {
      return cache.getLockManager();
   }

   @Override
   public XAResource getXAResource() {
      return cache.getXAResource();
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      return this.wrapper.wrap(this.cache.withFlags(flags));
   }

   @Override
   public boolean lock(K... key) {
      return cache.lock(key);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      return cache.lock(keys);
   }

   @Override
   public void applyDelta(K deltaAwareValueKey, Delta delta, Object... locksToAcquire){
      cache.applyDelta(deltaAwareValueKey, delta, locksToAcquire);
   }

   @Override
   public Stats getStats() {
       return cache.getStats();
   }

   @Override
   public ClassLoader getClassLoader() {
      return cache.getClassLoader();
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      return this.wrapper.wrap(this.cache.with(classLoader));
   }

   @Override
   public CacheEntry getCacheEntry(Object key, EnumSet<Flag> explicitFlags, ClassLoader explicitClassLoader) {
      return cache.getCacheEntry(key, explicitFlags, explicitClassLoader);
   }

   protected final void putForExternalRead(K key, V value, EnumSet<Flag> flags, ClassLoader classLoader) {
      ((CacheImpl<K, V>) cache).putForExternalRead(key, value, flags, classLoader);
   }
   
   @Override
    public int addValidationRule(ValidationRule<?> rule) {
	return cache.addValidationRule(rule);
    }
   
   public V getWithRule(Object key, int ruleIndex) {
       return cache.getWithRule(key, ruleIndex);
   }
   
   @Override
    public void delayedComputation(DelayedComputation computation) {
	cache.delayedComputation(computation);
    }
   
   @Override
    public Object delayedGet(Object key) {
	return cache.delayedGet(key);
    }
   
   public void delayedPut(Object key, Object value) {
       cache.delayedPut(key, value);
   }

   public interface AdvancedCacheWrapper<K, V> {
      AdvancedCache<K, V> wrap(AdvancedCache<K, V> cache);
   }

   @Override
   public TransactionTable getTxTable() {
       return cache.getTxTable();
   }

   public final void registerGet(Object key) {
       cache.registerGet(key);
   }

   @Override
   public boolean setTransactionClass(String transactionClass) {
      return cache.setTransactionClass(transactionClass);
   }
}
