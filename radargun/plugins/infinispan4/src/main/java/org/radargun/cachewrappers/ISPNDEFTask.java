package org.radargun.cachewrappers;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.Callable;

import org.infinispan.Cache;
import org.infinispan.distexec.DistributedCallable;
import org.radargun.DEFTask;


public class ISPNDEFTask<K, V, T> implements DistributedCallable<K, V, T>, DEFTask<T>, Serializable {

    private final Callable<T> callable;
  
    public ISPNDEFTask(Callable<T> callable) {
	this.callable = callable;
    }
    
    
    @Override
    public T call() throws Exception {
	return this.callable.call();
    }

    @Override
    public T justExecute() throws Exception {
	return this.callable.call();
    }
    
    @Override
    public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
    }

}
