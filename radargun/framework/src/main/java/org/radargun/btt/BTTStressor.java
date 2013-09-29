package org.radargun.btt;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.btt.colocated.BPlusTree;
import org.radargun.stressors.AbstractCacheWrapperStressor;


public class BTTStressor extends AbstractCacheWrapperStressor implements Runnable {

    public static final int TEST_PHASE = 2;
    public static final int SHUTDOWN_PHASE = 3;
    
    public static CacheWrapper cache;
    private int readOnlyPerc;
    private int keysSize;
    private int keysRange;
    private int seconds;
    private String emulation;
    
    public void setEmulation(String emulation) {
	this.emulation = emulation;
    }
    
    public void setKeysRange(int keysRange) {
        this.keysRange = keysRange;
    }

    private Random random = new Random();
    
    private BPlusTree<Long> tree;
    public long lastValue = -1L;
    public long steps;
    public Map<Integer, Long> latencies = new HashMap<Integer, Long>(1000);
    public long aborts;
    
    volatile protected int m_phase = TEST_PHASE;
    
    @Override
    public void run() {
	stress(cache);
    }
    
    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
        if (wrapper == null) {
            throw new IllegalStateException("Null wrapper not allowed");
        }

        int clusterSize = wrapper.getNumMembers();
        LocatedKey treeKey = wrapper.createGroupingKeyWithRepl("tree", 0, clusterSize);
        try {
            wrapper.startTransaction(true);
            this.tree = (BPlusTree<Long>) wrapper.get(treeKey);
            wrapper.endTransaction(true);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        
        while (m_phase == TEST_PHASE) {
            long start = System.nanoTime();
            step(TEST_PHASE);
            long latency = System.nanoTime() - start;
            int latencyMs = (int) (latency / 1000000L);
            Long count = latencies.get(latencyMs);
            if (count == null) {
        	count = 0L;
            }
            count++;
            latencies.put(latencyMs, count);
            steps++;
        }
        
        return new LinkedHashMap<String, String>();
    }
    
    protected void step(int phase) {
	boolean successful = false;
	long value = Math.abs(random.nextLong()) % keysRange;
	boolean query = Math.abs(random.nextInt(100)) < this.readOnlyPerc;
	boolean scan = false;
	boolean rmw = false;
	int scanSize = -1;
	
	if (BTTPopulationStressor.w != null) {
	    String op = BTTPopulationStressor.w.doTransaction();
	    if (op.equals("READ")) {
		query = true;
	    } else {
		query = false;
		if (op.equals("INSERT") || op.equals("UPDATE")) {
		    scan = false;
		} else if (op.equals("SCAN")){
		    scan = true;
		    scanSize = BTTPopulationStressor.w.doTransactionScan();
		} else {
		    rmw = true;
		}
	    }
	    value = BTTPopulationStressor.w.doTransactionRead();
	}
	
	while (!successful && m_phase == TEST_PHASE) {
	    try {
		
		if (query && emulation.equals("minuetFull")) {
		    try {
			cache.startTransaction(false);
			LocatedKey fullKey = cache.createGroupingKeyWithRepl("minuetFull-snapshot-id", 0, cache.getNumMembers());
			Long snapshotId = (Long) cache.get(fullKey);
			cache.put(fullKey, snapshotId + 1);
			cache.endTransaction(true);
		    } catch (Exception e) {
			cache.endTransaction(false);
		    }
		} else if (query && emulation.equals("minuetPartial")) {
		    try {
			cache.startTransaction(false);
			LocatedKey partialKey = cache.createGroupingKey("minuetFull-snapshot-id", cache.getNumMembers());
			Long snapshotId = (Long) cache.get(partialKey);
			cache.put(partialKey, snapshotId + 1);
			cache.endTransaction(true);
		    } catch (Exception e) {
			cache.endTransaction(false);
		    }
		}
		
		
		cache.startTransaction(false);
		
		if (BTTPopulationStressor.w != null) {
		    if (!query && !scan && !rmw) {
			if (lastValue == -1) {
			    this.tree.removeKey(value);
			} else {
			    this.tree.insert(lastValue, lastValue);
			}
		    } else if (query) {
			this.tree.containsKey(value);
		    } else if (scan) {
			this.tree.scan(value, scanSize);
		    } else {
			this.tree.containsKey(value);
			for (int i = 0; i < 60; i++) {
			    if (i % 3 == 0) {
				LocatedKey ranKey = BPlusTree.wrapper.createGroupingKey("ranKey" + Math.abs(random.nextInt(1000)), BPlusTree.myGroup());
				BPlusTree.wrapper.put(ranKey, 0);
			    } else {
				LocatedKey ranKey = BPlusTree.wrapper.createGroupingKey("ranKey" + Math.abs(random.nextInt(1000)), BPlusTree.myGroup());
				BPlusTree.wrapper.get(ranKey);
			    }
			}
		    }
		} else {
		    if (query) {
			this.tree.containsKey(value);
		    } else {
			if (lastValue == -1) {
			    this.tree.removeKey(value);
			} else {
			    this.tree.insert(lastValue, lastValue);
			}
		    }
		}
		
		cache.endTransaction(true);
		successful = true;
		if (!query) {
		    if (lastValue == -1) {
			lastValue = value;
		    } else {
			lastValue = -1;
		    }
		}
	    } catch (Exception e) {
		this.aborts++;
		try {
		    cache.endTransaction(false);
		} catch (Exception e2) {}
	    }
	}
    }
    
    @Override
    public void destroy() throws Exception {
        cache.empty();
        cache = null;
    }
    
    public CacheWrapper getCache() {
        return cache;
    }
    public void setCache(CacheWrapper cacheNew) {
        cache = cacheNew;
    }
    public void setReadOnlyPerc(int readOnlyPerc){
	this.readOnlyPerc = readOnlyPerc;
    }
    public int getReadOnlyPerc() {
	return this.readOnlyPerc;
    }
    public int getKeysSize() {
        return keysSize;
    }
    public void setKeysSize(int keysSize) {
        this.keysSize = keysSize;
    }
    public int getSeconds() {
        return seconds;
    }
    public void setSeconds(int seconds) {
        this.seconds = seconds;
    }
    public int getM_phase() {
        return m_phase;
    }
    public void setM_phase(int m_phase) {
        this.m_phase = m_phase;
    }
}
