package org.radargun.btt.colocated;

import java.io.Serializable;

import org.radargun.CallableWrapper;


public class ContainsRemoteTask implements CallableWrapper<Boolean>, Serializable {

    private InnerNode node;
    private Comparable key;
    
    public ContainsRemoteTask(InnerNode node, Comparable key) {
	this.node = node;
	this.key = key;
    }
    
    @Override
    public Boolean doTask() throws Exception {
	try {
	    return node.containsKey(true, key);
	} catch (NullPointerException npe) {
	    npe.printStackTrace();
	    throw npe;
	}
    }

}
