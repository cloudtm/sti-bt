package org.radargun.cachewrappers;

import java.io.Serializable;

import org.infinispan.distribution.group.Group;
import org.radargun.LocatedKey;

public class GroupingKey implements LocatedKey, Serializable {

    private final String key;
    private final int group;
    private final int replicationDegree;
    
    public GroupingKey(String key, int group) {
	this.key = key;
	this.group = group;
	this.replicationDegree = -1;
    }
    
    public GroupingKey(String key, int group, int replicationDegree) {
	this.key = key;
	this.group = group;
	this.replicationDegree = replicationDegree;
    }
    
    @Override
    public int hashCode() {
        return key.hashCode();
    }
    
    public String getKey() {
	return this.key;
    }
    
    public int getReplicationDegree() {
	return this.replicationDegree;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GroupingKey)) {
            return false;
        }
        GroupingKey other = (GroupingKey) obj;
        return this.key.equals(other.key) && this.group == other.group && this.replicationDegree == other.replicationDegree;
    }
    
    public String getShortKey() {
	int firstHyphen = this.key.indexOf("-");
	return key.substring(0, firstHyphen);
    }
    
    @Group
    public String group() {
	return "" + group + ((this.replicationDegree != -1) ? (":::" + this.replicationDegree) : "");
    }

    public int getGroup() {
	return this.group;
    }
    
    @Override
    public String toString() {
        return "key: " + this.key + " group: " + this.group + " owners: " + this.replicationDegree;
    }
}
