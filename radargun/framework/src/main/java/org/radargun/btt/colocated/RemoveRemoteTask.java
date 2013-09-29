package org.radargun.btt.colocated;

import java.io.Serializable;

import org.radargun.CallableWrapper;
import org.radargun.LocatedKey;


public class RemoveRemoteTask implements CallableWrapper<AbstractNode>, Serializable {

    private InnerNode node;
    private Comparable key;
    private int height;
    private String localRootsUUID;
    private LocatedKey cutoffKey;
    
    public RemoveRemoteTask(InnerNode node, Comparable key, int height, String localRootsUUID, LocatedKey cutoffKey) {
	this.node = node;
	this.key = key;
	this.height = height;
	this.localRootsUUID = localRootsUUID;
	this.cutoffKey = cutoffKey;
    }
    
    @Override
    public AbstractNode doTask() throws Exception {
	try {
	    return node.remove(true, key, height, localRootsUUID, cutoffKey);
	} catch (NullPointerException npe) {
	    npe.printStackTrace();
	    throw npe;
	}
    }

}
