package org.radargun.stamp.vacation;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.btt.colocated.BPlusTree;
import org.radargun.stamp.vacation.domain.Manager;

public class VacationPopulation {

    private static Log log = LogFactory.getLog(VacationPopulation.class);

    private final CacheWrapper wrapper;
    private int relations;
    
    public void setThreadMigration(boolean threadMigration) {
        this.threadMigration = threadMigration;
    }

    public void setGhostReads(boolean ghostReads) {
        this.ghostReads = ghostReads;
    }

    public void setColocation(boolean colocation) {
        this.colocation = colocation;
    }

    public void setReplicationDegrees(boolean replicationDegrees) {
        this.replicationDegrees = replicationDegrees;
    }

    public void setIntraNodeConc(boolean intraNodeConc) {
        this.intraNodeConc = intraNodeConc;
    }

    public void setLowerBound(int lowerBound) {
        this.lowerBound = lowerBound;
    }

    private boolean threadMigration;
    private boolean ghostReads;
    private boolean colocation;
    private boolean replicationDegrees;
    private boolean intraNodeConc;
    private int lowerBound;

    public VacationPopulation(CacheWrapper wrapper, int relations, boolean threadMigration, boolean ghostReads, boolean colocation, boolean replicationDegrees, boolean intraNodeConc, int lowerBound) {
	this.wrapper = wrapper;
	this.threadMigration = threadMigration;
	this.ghostReads = ghostReads;
	this.colocation = colocation;
	this.replicationDegrees = replicationDegrees;
	this.intraNodeConc = intraNodeConc;
	this.lowerBound = lowerBound;
	this.relations = relations;
    }

    public void performPopulation(){
	Vacation.CACHE = this.wrapper;
	
	int clusterSize = wrapper.getNumMembers();
	wrapper.initDEF();
	BPlusTree.wrapper = wrapper;
	BPlusTree.LOWER_BOUND = lowerBound;
	BPlusTree.LOWER_BOUND_WITH_LAST_KEY = BPlusTree.LOWER_BOUND + 1;
	// The maximum number of keys in a node NOT COUNTING with the special LAST_KEY. This number should be a multiple of 2.
	BPlusTree.MAX_NUMBER_OF_KEYS = 2 * BPlusTree.LOWER_BOUND;
	BPlusTree.MAX_NUMBER_OF_ELEMENTS = BPlusTree.MAX_NUMBER_OF_KEYS + 1;
	
	BPlusTree.MEMBERS = clusterSize;
	BPlusTree.THREAD_MIGRATION = threadMigration;
	BPlusTree.COLOCATE = colocation;
	BPlusTree.GHOST = ghostReads;
	BPlusTree.REPL_DEGREES = replicationDegrees;
	BPlusTree.INTRA_NODE_CONC = intraNodeConc;
	BPlusTree.POPULATING = true;
	
	if (! this.wrapper.isTheMaster()) {
	    BPlusTree.POPULATING = false;
	    return;
	}
	
	int i;
	int t;

	int numRelation = relations;
	int ids[] = new int[numRelation];
	for (i = 0; i < numRelation; i++) {
	    ids[i] = i + 1;
	}

	
	boolean successful = false;
	while (!successful) {
	    try {
		wrapper.startTransaction(false);
		
		Random randomPtr = new Random();
		randomPtr.random_alloc();
		Manager managerPtr = new Manager(threadMigration, colocation, ghostReads, replicationDegrees);
		
		for (t = 0; t < 4; t++) {

		    /* Shuffle ids */
		    for (i = 0; i < numRelation; i++) {
			int x = randomPtr.posrandom_generate() % numRelation;
			int y = randomPtr.posrandom_generate() % numRelation;
			int tmp = ids[x];
			ids[x] = ids[y];
			ids[y] = tmp;
		    }

		    /* Populate table */
		    for (i = 0; i < numRelation; i++) {
			boolean status = false;
			int id = ids[i];
			int num = ((randomPtr.posrandom_generate() % 5) + 1) * 100;
			int price = ((randomPtr.posrandom_generate() % 5) * 10) + 50;
			if (t == 0) {
			    status = managerPtr.manager_addCar(id, num, price);
			} else if (t == 1) {
			    status = managerPtr.manager_addFlight(id, num, price);
			} else if (t == 2) {
			    status = managerPtr.manager_addRoom(id, num, price);
			} else if (t == 3) {
			    status = managerPtr.manager_addCustomer(id);
			}
			assert (status);
		    }

		} /* for t */
		LocatedKey key = wrapper.createGroupingKeyWithRepl("MANAGER", 0, wrapper.getNumMembers());
		wrapper.put(null, key, managerPtr);
		
		wrapper.endTransaction(true);
		successful = true;
		
		managerPtr.colocate();
		
		Map<String, String> stats = wrapper.getAdditionalStats();
		System.out.println(stats);

		wrapper.resetAdditionalStats();
		
	    }  catch (Throwable e) {
		System.out.println("Exception during population, going to rollback after this");
		e.printStackTrace();
		log.warn(e);
		try {
		    wrapper.endTransaction(false);
		} catch (Throwable e2) {
		    System.out.println("Exception during rollback!");
		    e2.printStackTrace();
		}
	    }
	}

	BPlusTree.POPULATING = false;
	
	System.gc();
    }

}
