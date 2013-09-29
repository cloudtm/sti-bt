package org.radargun.stages;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.btree.BTreeStressor;
import org.radargun.btt.BTTStressor;
import org.radargun.state.MasterState;

public class BTTBenchmarkStage extends AbstractDistStage {

    private static final String SIZE_INFO = "SIZE_INFO";

    private transient CacheWrapper cacheWrapper;
    
    private transient BTTStressor bttStressor;
    
    private int readOnlyPerc;
    private int keysRange;
    private int keysSize;
    private int seconds;
    private String emulation;

    public void setEmulation(String emulation) {
        this.emulation = emulation;
    }
    
    public void setKeysRange(int keysRange) {
        this.keysRange = keysRange;
    }

    @Override
    public DistStageAck executeOnSlave() {
	DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
	this.cacheWrapper = slaveState.getCacheWrapper();
	if (cacheWrapper == null) {
	    log.info("Not running test on this slave as the wrapper hasn't been configured.");
	    return result;
	}

	log.info("Starting BTTBenchmarkStage: " + this.toString());

	bttStressor = new BTTStressor();
	bttStressor.setCache(cacheWrapper);
	bttStressor.setReadOnlyPerc(readOnlyPerc);
	bttStressor.setKeysSize(keysSize);
        bttStressor.setKeysRange(keysRange);
	bttStressor.setSeconds(seconds);
        bttStressor.setEmulation(emulation);
	
	try {
	    Thread worker = new Thread(bttStressor);
	    worker.start();
	    try {
		Thread.sleep(seconds * 1000);
	    } catch (InterruptedException e) {
	    }
	    bttStressor.setM_phase(BTreeStressor.SHUTDOWN_PHASE);
	    worker.join();
	    
	    Map<String, String> results = new LinkedHashMap<String, String>();
	    String sizeInfo = "size info: " + cacheWrapper.getInfo() +
		    ", clusterSize:" + super.getActiveSlaveCount() +
		    ", nodeIndex:" + super.getSlaveIndex() +
		    ", cacheSize: " + cacheWrapper.getCacheSize();
	    results.put(SIZE_INFO, sizeInfo);
	    
	    long steps = bttStressor.steps;
	    long aborts = bttStressor.aborts;
	    
	    results.put("TOTAL_THROUGHPUT", ((steps + 0.0) / (seconds + 0.0)) + "");
	    results.put("TOTAL_RESTARTS", aborts + "");
	    results.putAll(this.cacheWrapper.getAdditionalStats());
	    
	    Map<Integer, Long> latencies = bttStressor.latencies;
            String str = "";
	    for (Map.Entry<Integer, Long> entry : latencies.entrySet()) {
		int latency = entry.getKey();
		double val = entry.getValue();
		str += latency + ":" + val + ";";
	    }
	    results.put("LATENCY", str);
	    
	    log.info(sizeInfo);
	    result.setPayload(results);
	    return result;
	} catch (Exception e) {
	    log.warn("Exception while initializing the test", e);
	    result.setError(true);
	    result.setRemoteException(e);
	    return result;
	}
    }

    public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
	logDurationInfo(acks);
	boolean success = true;
	Map<Integer, Map<String, Object>> results = new HashMap<Integer, Map<String, Object>>();
	masterState.put("results", results);
	for (DistStageAck ack : acks) {
	    DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
	    if (wAck.isError()) {
		success = false;
		log.warn("Received error ack: " + wAck);
	    } else {
		if (log.isTraceEnabled())
		    log.trace(wAck);
	    }
	    Map<String, Object> benchResult = (Map<String, Object>) wAck.getPayload();
	    if (benchResult != null) {
		results.put(ack.getSlaveIndex(), benchResult);
		Object reqPerSes = benchResult.get("TOTAL_THROUGHPUT");
		if (reqPerSes == null) {
		    throw new IllegalStateException("This should be there! TOTAL_THROUGHPUT");
		}
		Object aborts = benchResult.get("TOTAL_RESTARTS");
		if (reqPerSes == null) {
		    throw new IllegalStateException("This should be there! TOTAL_RESTARTS");
		}
		log.info("Received " +  benchResult.remove(SIZE_INFO));
	    } else {
		log.trace("No report received from slave: " + ack.getSlaveIndex());
	    }
	}
	return success;
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
}
