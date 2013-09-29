package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.btree.BTreePopulationStressor;

public class BTreePopulationStage extends AbstractDistStage {

    private static final long serialVersionUID = -6913577046447056921L;
    
    private int keysSize;

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
	BTreePopulationStressor stressor = new BTreePopulationStressor();
	stressor.setKeysSize(this.keysSize);
	stressor.stress(wrapper);
    }

    public void setKeysSize(int keysSize) {
	this.keysSize = keysSize;
    }
    
    public int getKeysSize() {
	return this.keysSize;
    }
}
