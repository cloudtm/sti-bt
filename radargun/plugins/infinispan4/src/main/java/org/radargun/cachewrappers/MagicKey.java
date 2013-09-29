package org.radargun.cachewrappers;

import java.io.Serializable;

import org.radargun.LocatedKey;

public class MagicKey implements LocatedKey, Serializable {

    private static final long serialVersionUID = -1072474466685642719L;
    public static int NODE_INDEX;
    
    public final String key;
    public final int node;
    
    public static int local = 0;
    public static int remote = 0;
    
    public MagicKey(String key, int node) {
	this.key = key;
	this.node = node;
	
	if (NODE_INDEX == this.node) {
	    local++;
	} else {
	    remote++;
	}
    }

public String getKey() {
	return null;
}

public int getGroup() {
	return 0;
}
    
    public MagicKey(String key, int baseNode, boolean totalOrder) {
	this.key = key;
	this.node = baseNode;
    }
    
    @Override
    public boolean equals (Object o) {
       if (this == o) return true;
       if (o == null || !getClass().equals(o.getClass())) return false;

       MagicKey other = (MagicKey) o;

       return this.key.equals(other.key) && this.node == other.node;
    }
    
    public int hashCode() {
        return key.hashCode();
    }
    
    @Override
    public String toString() {
        return this.node + " owns " + this.key; 
    }

    @Override
    public int getReplicationDegree() {
	// TODO Auto-generated method stub
	return 0;
    }
}
