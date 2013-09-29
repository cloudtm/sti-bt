package org.radargun.stages;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.stamp.vacation.Definitions;
import org.radargun.stamp.vacation.Random;
import org.radargun.stamp.vacation.Vacation;
import org.radargun.stamp.vacation.VacationStressor;
import org.radargun.stamp.vacation.domain.Manager;
import org.radargun.stamp.vacation.transaction.DeleteCustomerOperation;
import org.radargun.stamp.vacation.transaction.MakeReservationOperation;
import org.radargun.stamp.vacation.transaction.UpdateTablesOperation;
import org.radargun.stamp.vacation.transaction.VacationTransaction;
import org.radargun.state.MasterState;


public class VacationBenchmarkStage extends AbstractDistStage {

    private static final String SIZE_INFO = "SIZE_INFO";

    private transient CacheWrapper cacheWrapper;

    private transient VacationStressor vacationStressor;

    private int readOnly;
    private int number;
    private int queries;
    private int relations;
    private int transactions;	// actually it's time now

    public void setReadOnly(int ro) {
	this.readOnly = ro;
    }
    
    @Override
    public DistStageAck executeOnSlave() {
	DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
	this.cacheWrapper = slaveState.getCacheWrapper();
	if (cacheWrapper == null) {
	    log.info("Not running test on this slave as the wrapper hasn't been configured.");
	    return result;
	}

	log.info("Starting VacationBenchmarkStage: " + this.toString());

	vacationStressor = new VacationStressor();

	    int numQueryPerTransaction = number;

	    vacationStressor = new VacationStressor();
	    vacationStressor.setQueryPerTx(numQueryPerTransaction);
	    vacationStressor.setQueryRange(queries);
	    vacationStressor.setReadOnlyPerc(this.readOnly);
	    vacationStressor.setCacheWrapper(cacheWrapper);
	    vacationStressor.setRelations(relations);

	try {
	    Thread worker = new Thread(vacationStressor);
		worker.start();
	    try {
		Thread.sleep(transactions);
	    } catch (InterruptedException e) { }
		vacationStressor.setPhase(VacationStressor.SHUTDOWN_PHASE);
		worker.join();
	    Map<String, String> results = new LinkedHashMap<String, String>();
	    String sizeInfo = "size info: " + cacheWrapper.getInfo() +
		    ", clusterSize:" + super.getActiveSlaveCount() +
		    ", nodeIndex:" + super.getSlaveIndex() +
		    ", cacheSize: " + cacheWrapper.getCacheSize();
	    results.put(SIZE_INFO, sizeInfo);
	    long aborts = 0L;
	    long throughput = 0L;
		aborts += vacationStressor.getRestarts();
		throughput += vacationStressor.getThroughput();
	    results.put("THROUGHPUT", (((throughput + 0.0) * 1000) / transactions) + "");
	    results.put("TOTAL_RESTARTS", aborts + "");
	    results.putAll(this.cacheWrapper.getAdditionalStats());
	    
	    Map<Integer, Long> latencies = vacationStressor.latencies;
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
		Object reqPerSes = benchResult.get("THROUGHPUT");
		if (reqPerSes == null) {
		    throw new IllegalStateException("This should be there!");
		}
		log.info("On slave " + ack.getSlaveIndex() + " it took " + (Double.parseDouble(reqPerSes.toString()) / 1000.0) + " seconds");
		log.info("Received " +  benchResult.remove(SIZE_INFO));
	    } else {
		log.trace("No report received from slave: " + ack.getSlaveIndex());
	    }
	}
	return success;
    }

    public CacheWrapper getCacheWrapper() {
	return cacheWrapper;
    }

    public void setCacheWrapper(CacheWrapper cacheWrapper) {
	this.cacheWrapper = cacheWrapper;
    }

    public int getNumber() {
	return number;
    }

    public void setNumber(int number) {
	this.number = number;
    }

    public int getQueries() {
	return queries;
    }

    public void setQueries(int queries) {
	this.queries = queries;
    }

    public int getRelations() {
	return relations;
    }

    public void setRelations(int relations) {
	this.relations = relations;
    }

    public int getTransactions() {
	return transactions;
    }

    public void setTransactions(int transactions) {
	this.transactions = transactions;
    }

    public static String getSizeInfo() {
	return SIZE_INFO;
    }

}
