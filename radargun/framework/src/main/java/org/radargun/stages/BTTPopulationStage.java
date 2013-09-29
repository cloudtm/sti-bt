package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.btt.BTTPopulationStressor;

public class BTTPopulationStage extends AbstractDistStage {

    private static final long serialVersionUID = -6913577046447056921L;
    
    private int keysSize;
    private int keysRange;
    private String workload;
    private boolean threadMigration;
    private boolean ghostReads;
    private boolean colocation;
    private boolean replicationDegrees;
    private boolean intraNodeConc;
    private int lowerBound;

    public void setWorkload(String workload) {
	this.workload = workload;
    }
    
    public void setIntraNodeConc(boolean intraNodeConc) {
	this.intraNodeConc = intraNodeConc;
    }
    
    public void setKeysRange(int keysRange) {
        this.keysRange = keysRange;
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
	BTTPopulationStressor stressor = new BTTPopulationStressor();
	stressor.setKeysSize(this.keysSize);
	stressor.setColocation(colocation);
	stressor.setGhostReads(ghostReads);
	stressor.setReplicationDegrees(replicationDegrees);
	stressor.setThreadMigration(threadMigration);
	stressor.setLowerBound(lowerBound);
        stressor.setKeysRange(keysRange);
        stressor.setIntraNodeConc(intraNodeConc);
        stressor.setWorkload(workload);
	stressor.stress(wrapper);
    }

    public void setKeysSize(int keysSize) {
	this.keysSize = keysSize;
    }
    
    public int getKeysSize() {
	return this.keysSize;
    }

    public boolean isThreadMigration() {
        return threadMigration;
    }

    public void setThreadMigration(boolean threadMigration) {
        this.threadMigration = threadMigration;
    }

    public boolean isGhostReads() {
        return ghostReads;
    }

    public void setGhostReads(boolean ghostReads) {
        this.ghostReads = ghostReads;
    }

    public boolean isColocation() {
        return colocation;
    }

    public void setColocation(boolean colocation) {
        this.colocation = colocation;
    }
    
    public void setLowerBound(int lowerBound) {
	this.lowerBound = lowerBound;
    }

    public boolean isReplicationDegrees() {
        return replicationDegrees;
    }

    public void setReplicationDegrees(boolean replicationDegrees) {
        this.replicationDegrees = replicationDegrees;
    }
}
