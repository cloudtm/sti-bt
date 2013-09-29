package org.radargun.btt.colocated;

import java.io.Serializable;

public class RebalanceBoolean implements Serializable {

    public boolean rebalance;
    public AbstractNode node;
    
    public RebalanceBoolean(boolean rebalance, AbstractNode node) {
	this.rebalance = rebalance;
	this.node = node;
    }
    
}
