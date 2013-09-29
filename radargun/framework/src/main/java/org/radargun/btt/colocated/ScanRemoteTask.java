package org.radargun.btt.colocated;

import java.io.Serializable;

import org.radargun.CallableWrapper;


public class ScanRemoteTask implements CallableWrapper<Void>, Serializable {

    private InnerNode node;
    private Comparable key;
    private int length;
    
    public ScanRemoteTask(InnerNode node, Comparable key, int length) {
	this.node = node;
	this.key = key;
	this.length = length;
    }
    
    @Override
    public Void doTask() throws Exception {
	try {
	    node.scan(true, key, length);
	    return null;
	} catch (NullPointerException npe) {
	    npe.printStackTrace();
	    throw npe;
	}
    }

}
