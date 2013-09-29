package org.radargun.stamp.vacation;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.transaction.DeleteCustomerOperation;
import org.radargun.stamp.vacation.transaction.MakeReservationOperation;
import org.radargun.stamp.vacation.transaction.UpdateNumberCustomers;
import org.radargun.stamp.vacation.transaction.UpdateTablesOperation;
import org.radargun.stamp.vacation.transaction.VacationTransaction;
import org.radargun.stressors.AbstractCacheWrapperStressor;

public class VacationStressor extends AbstractCacheWrapperStressor implements Runnable {

    private static Log log = LogFactory.getLog(VacationStressor.class);

    public static final int TEST_PHASE = 2;
    public static final int SHUTDOWN_PHASE = 3;
    
    volatile protected int m_phase = TEST_PHASE;
    
    private CacheWrapper cacheWrapper;
    private long restarts = 0;
    private long throughput = 0;
    
    public Map<Integer, Long> latencies = new HashMap<Integer, Long>(1000);
    
    private Random randomPtr;
    private int queryPerTx;
    private int queryRange;
    private int readOnlyPerc;
    private int relations;

    public void setRelations(int relations) {
        this.relations = relations;
    }

    public VacationStressor() {
	randomPtr = new Random();
	randomPtr.random_alloc();
    }
    
    public void setQueryPerTx(int queryPerTx) {
	this.queryPerTx = queryPerTx;
    }
    
    public void setQueryRange(int queryRange) {
	this.queryRange = queryRange;
    }
    
    public void setReadOnlyPerc(int readOnlyPerc) {
	this.readOnlyPerc = readOnlyPerc;
    }
    
    public void setCacheWrapper(CacheWrapper cacheWrapper) {
	this.cacheWrapper = cacheWrapper;
    }

    @Override
    public void run() {
	stress(cacheWrapper);
    }

    private VacationTransaction generateNextTransaction() {
	int r = randomPtr.posrandom_generate() % 100;
	VacationTransaction result = null;
	
//	if (action == Definitions.ACTION_MAKE_RESERVATION) {
	    result = new MakeReservationOperation(randomPtr, queryPerTx, queryRange, relations, readOnlyPerc);
//	} else if (action == Definitions.ACTION_DELETE_CUSTOMER) {
//	    result = new UpdateNumberCustomers(randomPtr, queryRange);
//	} 
	
	return result;
    }

    public int selectAction(int r, int percentUser) {
	if (r < percentUser) {
	    return Definitions.ACTION_MAKE_RESERVATION;
	} else 
	    return Definitions.ACTION_DELETE_CUSTOMER;
    }
    
    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
	this.cacheWrapper = wrapper;

	while (m_phase == TEST_PHASE) {
	    long start = System.nanoTime();
	    processTransaction(wrapper, generateNextTransaction());
	    long latency = System.nanoTime() - start;
            int latencyMs = (int) (latency / 1000000L);
            Long count = latencies.get(latencyMs);
            if (count == null) {
        	count = 0L;
            }
            count++;
            latencies.put(latencyMs, count);
	    this.throughput++;
	}

	Map<String, String> results = new LinkedHashMap<String, String>();

	return results;
    }

    private void processTransaction(CacheWrapper wrapper, VacationTransaction transaction) {
	boolean successful = true;

	while (true) {
	    if (m_phase != TEST_PHASE) {
		this.throughput--;
		break;
	    }
	    cacheWrapper.startTransaction(transaction.isReadOnly());
	    try {
		transaction.executeTransaction();
	    } catch (Throwable e) {
e.printStackTrace();
		successful = false;
	    }

	    try {
		cacheWrapper.endTransaction(successful);

		if (!successful) {
		    setRestarts(getRestarts() + 1);
		}
	    } catch (Throwable rb) {
rb.printStackTrace(); 
		setRestarts(getRestarts() + 1);
		successful = false;
	    }
	    
	    if (! successful) {
		successful = true;
	    } else { 
		break;
	    }
	}
    }

    @Override
    public void destroy() throws Exception {
	cacheWrapper.empty();
	cacheWrapper = null;
    }

    public long getRestarts() {
	return restarts;
    }

    public long getThroughput() {
	return this.throughput;
    }

    public void setRestarts(long restarts) {
	this.restarts = restarts;
    }

    public void setPhase(int shutdownPhase) {
	this.m_phase = shutdownPhase;
    }

}
