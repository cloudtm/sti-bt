package org.radargun.stages;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.btree.BTreeStressor;
import org.radargun.state.MasterState;

public class BTreeBenchmarkStage extends AbstractDistStage {

    private static final String SIZE_INFO = "SIZE_INFO";

    private transient CacheWrapper cacheWrapper;
    
    private transient BTreeStressor[] btreeStressors;
    
    private String execMode;
    private int remoteProb;
    private int opPerTx;
    private int keysSize;
    private int seconds;
    private int localThreads;
    
    @Override
    public DistStageAck executeOnSlave() {
	DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
	this.cacheWrapper = slaveState.getCacheWrapper();
	if (cacheWrapper == null) {
	    log.info("Not running test on this slave as the wrapper hasn't been configured.");
	    return result;
	}

	log.info("Starting BTreeBenchmarkStage: " + this.toString());
	
	btreeStressors = new BTreeStressor[localThreads];
	
	for (int t = 0; t < btreeStressors.length; t++) {
	    btreeStressors[t] = new BTreeStressor();
	    btreeStressors[t].setCache(cacheWrapper);
	    btreeStressors[t].setExecMode(execMode);
	    btreeStressors[t].setRemoteProb(remoteProb);
	    btreeStressors[t].setOpPerTx(opPerTx);
	    btreeStressors[t].setKeysSize(keysSize);
	    btreeStressors[t].setSeconds(seconds);
	}
	
	try {
	    Thread[] workers = new Thread[btreeStressors.length];
	    for (int t = 0; t < workers.length; t++) {
		workers[t] = new Thread(btreeStressors[t]);
	    }
	    for (int t = 0; t < workers.length; t++) {
		workers[t].start();
	    }
	    try {
		Thread.sleep(seconds * 1000);
	    } catch (InterruptedException e) {
	    }
	    for (int t = 0; t < workers.length; t++) {
		btreeStressors[t].setM_phase(BTreeStressor.SHUTDOWN_PHASE);
	    }
	    
	    for (int t = 0; t < workers.length; t++) {
		workers[t].join();
	    }
	    Map<String, String> results = new LinkedHashMap<String, String>();
	    String sizeInfo = "size info: " + cacheWrapper.getInfo() +
		    ", clusterSize:" + super.getActiveSlaveCount() +
		    ", nodeIndex:" + super.getSlaveIndex() +
		    ", cacheSize: " + cacheWrapper.getCacheSize();
	    results.put(SIZE_INFO, sizeInfo);
	    double steps = 0.0;
	    for (int t = 0; t < workers.length; t++) {
		steps += btreeStressors[t].steps;
	    }
	    results.put("TOTAL_THROUGHPUT", ((steps) / seconds) + "");
	    results.put("TOTAL_RESTARTS", 0 + "");
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
		    throw new IllegalStateException("This should be there!");
		}
		log.info("On slave " + ack.getSlaveIndex() + " had throughput " + Double.parseDouble(reqPerSes.toString()) + " ops/seconds");
		log.info("Received " +  benchResult.remove(SIZE_INFO));
	    } else {
		log.trace("No report received from slave: " + ack.getSlaveIndex());
	    }
	}
	return success;
    }
    
    public String getExecMode() {
        return execMode;
    }
    public void setExecMode(String execMode) {
        this.execMode = execMode;
    }
    public int getRemoteProb() {
        return remoteProb;
    }
    public void setRemoteProb(int remoteProb) {
        this.remoteProb = remoteProb;
    }
    public int getOpPerTx() {
        return opPerTx;
    }
    public void setOpPerTx(int opPerTx) {
        this.opPerTx = opPerTx;
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
    public void setLocalThreads(int localThreads) {
	this.localThreads = localThreads;
    }
    public int getLocalThreads() {
	return this.localThreads;
    }
}
