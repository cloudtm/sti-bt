package org.radargun.btt.colocated;
import java.io.Serializable;
import java.util.Random;
import java.util.UUID;

import org.radargun.CallableWrapper;
import org.radargun.LocatedKey;

/** The keys comparison function should be consistent with equals. */
public abstract class AbstractNode<T extends Serializable> implements Iterable<T>, Serializable {

    protected int group;
    protected LocatedKey parentKey;
    
    public static Random r = new Random();
    
    public AbstractNode(boolean dummy, int group) {
	this.group = group;
    }
    
    public AbstractNode(int group) {
	if (group == -1 || group == 0) {
	    this.group = 0;
	    this.parentKey = BPlusTree.REPL_DEGREES ? BPlusTree.wrapper.createGroupingKeyWithRepl("parent-" + UUID.randomUUID(), 0, BPlusTree.MEMBERS) : BPlusTree.wrapper.createGroupingKey("parent-" + UUID.randomUUID(), 0);
	} else {
	    this.group = BPlusTree.COLOCATE ? group : Math.abs(r.nextInt());
	    this.parentKey = BPlusTree.wrapper.createGroupingKey("parent-" + UUID.randomUUID(), this.group);
	}
    }
    
    public static <E> E executeDEF(CallableWrapper<E> task, Object key) throws Exception {
	return (BPlusTree.COLOCATE ? (E) BPlusTree.wrapper.execDEF(task, key) : task.doTask());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof AbstractNode)) {
            return false;
        }
        AbstractNode other = (AbstractNode) obj;
        return this.group == other.group && this.parentKey == other.parentKey;
    }
    
    protected boolean isFullyReplicated() {
	return this.group == 0; 
    }
    
    protected int getGroup() {
	return this.group;
    }
    
    protected InnerNode getParent(boolean ghostRead) {
	if (ghostRead) {
	    return (InnerNode) BPlusTree.cacheGetShadow(this.parentKey);
	} else {
	    return (InnerNode) BPlusTree.getCache(this.parentKey);
	}
    }
    
    public void setParent(InnerNode parent) {
	BPlusTree.putCache(this.parentKey, parent);
    }
    
    void clean() {
	BPlusTree.putCache(this.parentKey, null);
    }
    
    abstract RebalanceBoolean insert(Comparable key, T value, int height, String localRootsUUID, LocatedKey cutoffKey);
    
    /** Inserts the given key-value pair and returns the (possibly new) root node */
    abstract AbstractNode insert(boolean remote, Comparable key, T value, int height, String localRootsUUID, LocatedKey cutoffKey);

    /** Removes the element with the given key */
    abstract AbstractNode remove(boolean remote, Comparable key, int height, String localRootsUUID, LocatedKey cutoffKey);

    /** Returns the value to which the specified key is mapped, or <code>null</code> if this map contains no mapping for the key. */
    abstract T get(Comparable key);

    /** Returns <code>true</code> if this map contains a mapping for the specified key. */
    abstract boolean containsKey(boolean remote, Comparable key);

    abstract void scan(boolean remote, Comparable key, int length);
    
    /** Returns the number os key-value mappings in this map */
    abstract int size();

    /* **** Uncomment the following to support pretty printing of nodes **** */

    // static final AtomicInteger GLOBAL_COUNTER = new AtomicInteger(0);
    // protected int counter = GLOBAL_COUNTER.getAndIncrement();
    // public String toString() {
    // 	return "" + counter;
    // }

    /* *********** */

    abstract DoubleArray.KeyVal removeBiggestKeyValue();

    abstract DoubleArray.KeyVal removeSmallestKeyValue();

    abstract Comparable getSmallestKey();

    abstract void addKeyValue(DoubleArray.KeyVal keyValue);

    // merge elements from the left node into this node. smf: maybe LeafNodeArray can be a subclass of InnerNodeArray
    abstract void mergeWithLeftNode(AbstractNode leftNode, Comparable splitKey, int treeDepth, int height, String localRootsUUID, int cutoff);

    // the number of _elements_ in this node (not counting sub-nodes)
    abstract int shallowSize();

    abstract AbstractNode changeGroup(int newGroup);
}
