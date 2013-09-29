package org.infinispan.transaction;

import java.util.concurrent.Callable;

import org.infinispan.Cache;

public abstract class CacheCallable<T> implements Callable<T> {

   protected Cache cache;
   
   public void setCache(Cache cache) {
      this.cache = cache;
   }
   
}
