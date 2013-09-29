package org.radargun;

import java.io.Serializable;

public interface DEFTask<T> extends Serializable {

    public T justExecute() throws Exception;
    
}
