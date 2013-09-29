package org.radargun.stamp.vacation;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;

public class Vacation {

    public static transient CacheWrapper CACHE;
    
    public static final void put(LocatedKey key, Object value) {
	try {
	    CACHE.put(null, key, value);
	} catch (Exception e) {
	    if (e instanceof RuntimeException) {
		throw (RuntimeException)e;
	    }
	    e.printStackTrace();
	}
    }
    
    public static final <T> T get(LocatedKey key) {
	try {
	    return (T) CACHE.get(null, key);
	} catch (Exception e) {
	    if (e instanceof RuntimeException) {
		throw (RuntimeException)e;
	    }
	    e.printStackTrace();
	    return null;
	}
    }
    
}
