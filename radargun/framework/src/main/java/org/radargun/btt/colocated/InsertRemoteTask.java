package org.radargun.btt.colocated;

import java.io.Serializable;

import org.radargun.CallableWrapper;
import org.radargun.LocatedKey;


public class InsertRemoteTask implements CallableWrapper<AbstractNode>, Serializable {

    private InnerNode node;
    private Comparable key;
    private Serializable value;
    private int height;
    private String localRootsUUID;
    private LocatedKey cutoffKey;
    
    public InsertRemoteTask(InnerNode node, Comparable key, Serializable value, int height, String localRootsUUID, LocatedKey cutoffKey) {
	this.node = node;
	this.key = key;
	this.value = value;
	this.height = height;
	this.localRootsUUID = localRootsUUID;
	this.cutoffKey = cutoffKey;
    }
    
    @Override
    public AbstractNode doTask() throws Exception {
//	System.out.println(Thread.currentThread().getId() + "] Current version and hasRead: " + CommitLog.forcedRemoteVersion.get() + "\t" + Arrays.toString(CommitLog.forcedReadFrom.get().toArray()) + 
//	" node: " + node + " key " + key + " value " + value + " height " + height + " localRoots: " + localRootsUUID + " cutoff " + cutoffKey);
	try {
	    return node.insert(true, key, value, height, localRootsUUID, cutoffKey);
	} catch (NullPointerException npe) {
	    npe.printStackTrace();
	    throw npe;
	}
    }

}
