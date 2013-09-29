package org.radargun.btree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.radargun.CacheWrapper;
import org.radargun.CallableWrapper;
import org.radargun.DEF;
import org.radargun.DEFTask;
import org.radargun.LocatedKey;
import org.radargun.stressors.AbstractCacheWrapperStressor;


public class DistCallable implements Callable<Boolean>, Serializable {

    private static final long serialVersionUID = -7433902731655964700L;

    private final int k;
    private final int id;
    private final int keysSize;
    private final int opPerTx;

    public DistCallable(int k, int id, int keysSize, int opPerTx) {
	this.k = k;
	this.id = id;
	this.keysSize = keysSize;
	this.opPerTx = opPerTx;
    }

    @Override
    public Boolean call() throws Exception {
	for (int i = 0; i < opPerTx; ++i) {
	    LocatedKey key = BTreeStressor.cache.createGroupingKey("key"+ ((k + i) % keysSize), id);
	    String res = (String) BTreeStressor.cache.get(null, key);
	    if (res == null) {
		System.err.println("Problem: " + key + " " + key.getGroup() + " " + key.getKey() + " is null" + " id " + this.id);
		Thread.sleep(1);
		return true;
	    }
	}
	return true;
    }

}
