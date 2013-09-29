package org.radargun;


public interface CallableWrapper<T> {

    public T doTask() throws Exception;
    
}
