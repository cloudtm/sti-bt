package org.radargun.cachewrappers;

import java.io.Serializable;

import org.infinispan.DelayedComputation;

public class ChangeSizeComputation extends DelayedComputation implements Serializable {

    public ChangeSizeComputation(GroupingKey key, int count) {
	super(key, count);
    }
    
    @Override
    public void compute() {
	Integer number = ((Integer) InfinispanWrapper.STATIC_CACHE.delayedGet(key));
	if (number == null) {
	    number = 0;
	}
	number += count;
	InfinispanWrapper.STATIC_CACHE.delayedPut(this.key,  number);
    }

}
