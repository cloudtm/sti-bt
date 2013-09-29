package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.stamp.vacation.VacationPopulationStressor;

public class VacationPopulationStage extends AbstractDistStage {

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
    
    public void setRelations(int relations) {
	this.relations = relations;
    }
    
    @Override
    public DistStageAck executeOnSlave() {
	DefaultDistStageAck ack = newDefaultStageAck();
	CacheWrapper wrapper = slaveState.getCacheWrapper();
	if (wrapper == null) {
	    log.info("Not executing any test as the wrapper is not set up on this slave ");
	    return ack;
	}

	long startTime = System.currentTimeMillis();
	populate(wrapper);
	long duration = System.currentTimeMillis() - startTime;
	log.info("The population took: " + (duration / 1000) + " seconds.");
	ack.setPayload(duration);
	return ack;
    }
    
    private void populate(CacheWrapper wrapper) {
	VacationPopulationStressor vacationStressor = new VacationPopulationStressor();
	vacationStressor.setRelations(relations);
	vacationStressor.setThreadMigration(threadMigration);
	vacationStressor.setGhostReads(ghostReads);
	vacationStressor.setColocation(colocation);
	vacationStressor.setReplicationDegrees(replicationDegrees);
	vacationStressor.setIntraNodeConc(intraNodeConc);
	vacationStressor.setLowerBound(lowerBound);
	vacationStressor.stress(wrapper);
    }

}
