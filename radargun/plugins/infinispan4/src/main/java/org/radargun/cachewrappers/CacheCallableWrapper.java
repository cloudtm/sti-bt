package org.radargun.cachewrappers;

import java.io.Serializable;
import java.util.concurrent.Callable;

import org.infinispan.transaction.CacheCallable;
import org.radargun.CallableWrapper;

public class CacheCallableWrapper<T> extends CacheCallable<T> implements CallableWrapper<T>, Serializable {

    private CallableWrapper<T> task;
    
    public CacheCallableWrapper(CallableWrapper<T> task) {
	this.task = task;
    }
    
    @Override
    public T call() throws Exception {
	return this.task.doTask();
    }

    @Override
    public T doTask() throws Exception {
	return call();
    }

}
