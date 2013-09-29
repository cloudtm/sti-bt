package org.infinispan;

import java.io.Serializable;


public interface ValidationRule<T extends Serializable> {

    public boolean isStillValid(T newVersion);
    
}
