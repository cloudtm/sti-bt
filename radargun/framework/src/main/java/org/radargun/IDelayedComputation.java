package org.radargun;

import java.io.Serializable;
import java.util.Collection;

public interface IDelayedComputation<T> extends Serializable {

    public abstract Collection<Object> getIAffectedKeys();
    
    public abstract T computeI();
    
}
