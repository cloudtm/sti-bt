package org.radargun.btree;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.stressors.AbstractCacheWrapperStressor;

public class BTreePopulationStressor extends AbstractCacheWrapperStressor{

    private static Log log = LogFactory.getLog(BTreePopulationStressor.class);
    
    private int keysSize;
    
    public void setKeysSize(int keysSize) {
	this.keysSize = keysSize;
    }
    
    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
        if (wrapper == null) {
            throw new IllegalStateException("Null wrapper not allowed");
        }
        try {
            log.info("Performing Population Operations");
            populate(wrapper);
        } catch (Exception e) {
            e.printStackTrace();
            log.warn("Received exception during cache population" + e.getMessage());
        }
        return null;
    }
 
    private void populate(CacheWrapper wrapper) {
	boolean successful = false;
	int myGroup = wrapper.getLocalGrouping();
	Object myAddr = wrapper.getMyAddress();
//	while (!successful) {
	    try {
//		wrapper.startTransaction(true);
		for (int k = 0; k < this.keysSize; k++) {
		    LocatedKey key = wrapper.createGroupingKey("key" + k, myGroup);
		    wrapper.put(null, key, "" + k);
		}
		wrapper.put(null, myAddr + "-id", "" + myGroup);
		LocatedKey myKey = wrapper.createGroupingKey(myAddr + "-myKey", myGroup);
		wrapper.put(null, myKey, "");
System.err.println("Wrote " + keysSize + " keys; my id is " + myAddr + " " + myGroup);
//		wrapper.endTransaction(true);
//		successful = true;
	    } catch (Throwable e) {
		e.printStackTrace();
//		try {
//		    wrapper.endTransaction(false); 
//		} catch (Throwable e2) {
		    // silent catch again
//		}
	    }
//	}
    }

    @Override
    public void destroy() throws Exception {
        //Don't destroy data in cache!
    }

    
}
