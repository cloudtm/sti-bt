package org.infinispan;

import java.io.Serializable;


public abstract class DelayedComputation implements Serializable {

    protected Object key;
    public int count;

    public DelayedComputation(Object key, int count) {
	this.key = key;
	this.count = count;
    }
    
    public Object getAffectedKey() {
	return this.key;
    }

    public abstract void compute();

    public void mergeNewDelayedComputation(DelayedComputation newComputation) {
	this.count += newComputation.count;
    }
    
    @Override
    public boolean equals(Object obj) {
	if (! (obj instanceof DelayedComputation)) {
	    return false;
	}
	DelayedComputation other = (DelayedComputation) obj;
	return this.getAffectedKey().equals(other.getAffectedKey());
    }

    @Override
    public int hashCode() {
	return getAffectedKey().hashCode();
    }

}
