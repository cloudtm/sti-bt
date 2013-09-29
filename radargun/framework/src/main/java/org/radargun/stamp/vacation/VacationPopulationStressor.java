package org.radargun.stamp.vacation;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stressors.AbstractCacheWrapperStressor;

public class VacationPopulationStressor extends AbstractCacheWrapperStressor {
    
    private static Log log = LogFactory.getLog(VacationPopulationStressor.class);
    
    private int RELATIONS;
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
    
    public void setRelations(int RELATIONS) {
	this.RELATIONS = RELATIONS;
    }
    
    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
	if (wrapper == null) {
	    throw new IllegalStateException("Null wrapper not allowed");
	}
	try {
	    log.info("Performing Population Operations");
	    new VacationPopulation(wrapper, RELATIONS, threadMigration, ghostReads, colocation, replicationDegrees, intraNodeConc, lowerBound).performPopulation();
	} catch (Exception e) {
	    log.warn("Received exception during cache population" + e.getMessage());
	    e.printStackTrace();
	}
	return null;
    }

    @Override
    public void destroy() throws Exception {
	//Don't destroy data in cache!
    }

}
