package org.radargun.btt.colocated;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.radargun.LocatedKey;

public class LeafNode<T extends Serializable> extends AbstractNode<T> implements Serializable {

    private LocatedKey keyEntries;
    private LocatedKey keyPrevious;
    private LocatedKey keyNext;
    private LinkedList<Serializable> entriesList;
    
    public static final transient int TRUTH = -2;
    
    protected LeafNode(boolean dummy, int truth) {
	super(dummy, truth);
    }
    
    protected LeafNode(int group) {
	super(group);
	ensureKeys();
	if (BPlusTree.INTRA_NODE_CONC) {
	    this.entriesList = new LinkedList<Serializable>(super.group);
	} else {
	    setEntries(new DoubleArray<Serializable>(Serializable.class));
	}
    }
    
    protected LeafNode(int group, DoubleArray<Serializable> entries) {
	super(group);
	ensureKeys();
	setEntries(entries);
    }
    
    protected LeafNode(int group, LinkedList<Serializable> entries) {
	super(group);
	ensureKeys();
	this.entriesList = entries;
    }
    
    protected LeafNode(LeafNode old, int newGroup) {
	super(newGroup);
	// super.setParent(parent)	// set by the parent
	ensureKeys();
	setPrevious(old.getPrevious());
	setNext(old.getNext());
	if (BPlusTree.INTRA_NODE_CONC) {
	    this.entriesList = old.entriesList.changeGroup(newGroup);
	} else {
	    setEntries(old.getEntries(false));
	}
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LeafNode)) {
            return false;
        }
        LeafNode other = (LeafNode) obj;
        return this.keyEntries.equals(other.keyEntries) && this.group == other.group;
    }
    
    @Override
    void clean() {
        super.clean();
        if (BPlusTree.INTRA_NODE_CONC) {
            this.entriesList.clean();
        } else {
            BPlusTree.putCache(this.keyEntries, null);
        }
        BPlusTree.putCache(this.keyPrevious, null);
        BPlusTree.putCache(this.keyNext, null);
    }
    
    private void ensureKeys() {
	if (this.keyEntries == null) {
	    this.keyEntries = BPlusTree.wrapper.createGroupingKey("entries-" + UUID.randomUUID().toString(), this.group);
	}
	if (this.keyPrevious == null) {
	    this.keyPrevious = BPlusTree.wrapper.createGroupingKey("previous-" + UUID.randomUUID().toString(), this.group);
	}
	if (this.keyNext == null) {
	    this.keyNext = BPlusTree.wrapper.createGroupingKey("next-" + UUID.randomUUID().toString(), this.group);
	}
    }
    
    protected DoubleArray<Serializable> getEntries(boolean ghostRead) {
	if (ghostRead) {
	    return (DoubleArray<Serializable>) BPlusTree.cacheGetShadow(keyEntries);
	} else {
	    return (DoubleArray<Serializable>) BPlusTree.getCache(keyEntries);
	}
    }
    
    protected void setEntries(DoubleArray<Serializable> entries) {
	BPlusTree.putCache(keyEntries, entries);
    }
    
    protected LeafNode<T> getPrevious() {
	return (LeafNode<T>) BPlusTree.getCache(keyPrevious);
    }
    
    protected void setPrevious(LeafNode<T> previous) {
	BPlusTree.putCache(keyPrevious, previous);
    }
    
    protected LeafNode<T> getNext() {
	return (LeafNode<T>) BPlusTree.getCache(keyNext);
    }
    
    protected void setNext(LeafNode<T> next) {
	BPlusTree.putCache(keyNext, next);
    }
    
    @Override
    public AbstractNode insert(boolean remote, Comparable key, Serializable value, int height, String localRootsUUID, LocatedKey cutoffKey) {
	if (BPlusTree.INTRA_NODE_CONC) {
	    return insertWithList(key, value, height, localRootsUUID, cutoffKey);
	} else {
	    return insertWithArray(key, value, height, localRootsUUID, cutoffKey);
	}
    }

    private AbstractNode insertWithList(Comparable key, Serializable value, int height, String localRootsUUID, LocatedKey cutoffKey) {
	boolean inserted = this.entriesList.insert(key, (T) value);
	if (!inserted) {
	    return null;
	}
	
	int size = this.entriesList.getSizeWithRule() + (BPlusTree.INTRA_NODE_CONC && !BPlusTree.POPULATING ? 1 : 0);
	if (size <= BPlusTree.MAX_NUMBER_OF_ELEMENTS) {
	    return BPlusTree.TRUE_NODE;
	} else {
	    List<Comparable> keys = new ArrayList<Comparable>(size);
	    List<Serializable> values = new ArrayList<Serializable>(size);
	    this.entriesList.getAllElements(keys, values);
	    
	    if (keys.size() <= BPlusTree.MAX_NUMBER_OF_ELEMENTS) {
		return BPlusTree.TRUE_NODE;
	    }
	    
	    // find middle position
	    Comparable keyToSplit = keys.get(BPlusTree.LOWER_BOUND + 1);

	    // split node in two
	    int newGroup = this.group;
	    LeafNode leftNode = new LeafNode<T>(newGroup, new LinkedList<Serializable>(newGroup, keys, values, 0, BPlusTree.LOWER_BOUND));
	    LeafNode rightNode = new LeafNode<T>(newGroup, new LinkedList<Serializable>(newGroup, keys, values, BPlusTree.LOWER_BOUND + 1, keys.size() - 1));
	    fixLeafNodeArraysListAfterSplit(leftNode, rightNode);

	    InnerNode parent = this.getParent(true);
	    this.clean();

	    // propagate split to parent
	    if (parent == null) {  // make new root node
		InnerNode newRoot = new InnerNode<T>(-1, leftNode, rightNode, keyToSplit);
		return newRoot;
	    } else {
		return parent.rebase(leftNode, rightNode, keyToSplit, height, 1, localRootsUUID, cutoffKey, BPlusTree.getCutoff(true, cutoffKey));
	    }
	}
    }
    
    public AbstractNode insertWithArray(Comparable key, Serializable value, int height, String localRootsUUID, LocatedKey cutoffKey) {
	DoubleArray<Serializable> localEntries = this.getEntries(false);
	DoubleArray<Serializable> localArr = justInsert(localEntries, key, value);

	if (localArr == null) {
	    return null;
	}
	if (localArr.length() <= BPlusTree.MAX_NUMBER_OF_ELEMENTS) {
	    return BPlusTree.TRUE_NODE;
	} else {
	    // find middle position
	    Comparable keyToSplit = localArr.findRightMiddlePosition();

	    // split node in two
	    int newGroup = this.group;
	    LeafNode leftNode = new LeafNode<T>(newGroup, localArr.leftPart(BPlusTree.LOWER_BOUND + 1));
	    LeafNode rightNode = new LeafNode<T>(newGroup, localArr.rightPart(BPlusTree.LOWER_BOUND + 1));
	    fixLeafNodeArraysListAfterSplit(leftNode, rightNode);

	    InnerNode parent = this.getParent(true);
	    this.clean();

	    // propagate split to parent
	    if (parent == null) {  // make new root node
		InnerNode newRoot = new InnerNode<T>(-1, leftNode, rightNode, keyToSplit);
		return newRoot;
	    } else {
		return parent.rebase(leftNode, rightNode, keyToSplit, height, 1, localRootsUUID, cutoffKey, BPlusTree.getCutoff(true, cutoffKey));
	    }
	}
    }
    
    @Override
    public RebalanceBoolean insert(Comparable key, Serializable value, int height, String localRootsUUID, LocatedKey cutoffKey) {
	if (BPlusTree.INTRA_NODE_CONC) {
	    return insertWithListWithRebalance(key, value, height, localRootsUUID, cutoffKey);
	} else {
	    return insertWithArrayWithRebalance(key, value, height, localRootsUUID, cutoffKey);
	}
    }


    private RebalanceBoolean insertWithListWithRebalance(Comparable key, Serializable value, int height, String localRootsUUID, LocatedKey cutoffKey) {
	boolean inserted = this.entriesList.insert(key, (T) value);
	if (!inserted) {
	    return new RebalanceBoolean(false, null);
	}
	
	int size = this.entriesList.getSizeWithRule() + (BPlusTree.INTRA_NODE_CONC && !BPlusTree.POPULATING ? 1 : 0);
	if (size <= BPlusTree.MAX_NUMBER_OF_ELEMENTS) {
	    return new RebalanceBoolean(false, BPlusTree.TRUE_NODE);
	} else {
	    List<Comparable> keys = new ArrayList<Comparable>(size);
	    List<Serializable> values = new ArrayList<Serializable>(size);
	    this.entriesList.getAllElements(keys, values);
	    
	    if (keys.size() <= BPlusTree.MAX_NUMBER_OF_ELEMENTS) {
		return new RebalanceBoolean(false, BPlusTree.TRUE_NODE);
	    }
	    
	    // find middle position
	    Comparable keyToSplit = keys.get(BPlusTree.LOWER_BOUND + 1);

	    // split node in two
	    int newGroup = this.group;
	    LeafNode leftNode = new LeafNode<T>(newGroup, new LinkedList<Serializable>(newGroup, keys, values, 0, BPlusTree.LOWER_BOUND));
	    LeafNode rightNode = new LeafNode<T>(newGroup, new LinkedList<Serializable>(newGroup, keys, values, BPlusTree.LOWER_BOUND + 1, keys.size() - 1));
	    fixLeafNodeArraysListAfterSplit(leftNode, rightNode);

	    InnerNode parent = this.getParent(true);
	    this.clean();

	    // propagate split to parent
	    if (parent == null) {  // make new root node
		InnerNode newRoot = new InnerNode<T>(-1, leftNode, rightNode, keyToSplit);
		return new RebalanceBoolean(true, newRoot);
	    } else {
		return parent.rebase(false, leftNode, rightNode, keyToSplit, height, 1, localRootsUUID, cutoffKey, BPlusTree.getCutoff(true, cutoffKey));
	    }
	}
    }
    
    public RebalanceBoolean insertWithArrayWithRebalance(Comparable key, Serializable value, int height, String localRootsUUID, LocatedKey cutoffKey) {
	DoubleArray<Serializable> localEntries = this.getEntries(false);
	DoubleArray<Serializable> localArr = justInsert(localEntries, key, value);

	if (localArr == null) {
	    return new RebalanceBoolean(false, null);
	}
	if (localArr.length() <= BPlusTree.MAX_NUMBER_OF_ELEMENTS) {
	    return new RebalanceBoolean(false, BPlusTree.TRUE_NODE);
	} else {
	    // find middle position
	    Comparable keyToSplit = localArr.findRightMiddlePosition();

	    // split node in two
	    int newGroup = this.group;
	    LeafNode leftNode = new LeafNode<T>(newGroup, localArr.leftPart(BPlusTree.LOWER_BOUND + 1));
	    LeafNode rightNode = new LeafNode<T>(newGroup, localArr.rightPart(BPlusTree.LOWER_BOUND + 1));
	    fixLeafNodeArraysListAfterSplit(leftNode, rightNode);

	    InnerNode parent = this.getParent(true);
	    this.clean();

	    // propagate split to parent
	    if (parent == null) {  // make new root node
		InnerNode newRoot = new InnerNode<T>(-1, leftNode, rightNode, keyToSplit);
		return new RebalanceBoolean(true, newRoot);
	    } else {
		return parent.rebase(false, leftNode, rightNode, keyToSplit, height, 1, localRootsUUID, cutoffKey, BPlusTree.getCutoff(true, cutoffKey));
	    }
	}
    }
	
    private DoubleArray<Serializable> justInsert(DoubleArray<Serializable> localEntries, Comparable key, Serializable value) {
        // this test is performed because we need to return a new structure in
        // case an update occurs.  Value types must be immutable.
	Serializable currentValue = localEntries.get(key);
        // this check suffices because we do not allow null values
        if (currentValue != null && currentValue.equals(value)) {
            return null;
        } else {
            DoubleArray<Serializable> newArr = localEntries.addKeyValue(key, value);
            setEntries(newArr);
            return newArr;
        }
    }

    private void fixLeafNodeArraysListAfterSplit(LeafNode leftNode, LeafNode rightNode) {
	LeafNode myPrevious = this.getPrevious();
	LeafNode myNext = this.getNext();
	
	if (myPrevious != null) {
	    leftNode.setPrevious(myPrevious);
	}
        rightNode.setPrevious(leftNode);
        if (myNext != null) {
            rightNode.setNext(myNext);
        }
        leftNode.setNext(rightNode);
        
        if (myPrevious != null) {
            myPrevious.setNext(leftNode);
        }
        if (myNext != null) {
            myNext.setPrevious(rightNode);
        }
    }

    @Override
    public AbstractNode remove(boolean remote, Comparable key, int height, String localRootsUUID, LocatedKey cutoffKey) {
	if (BPlusTree.INTRA_NODE_CONC) {
	    return removeWithList(key, height, localRootsUUID, cutoffKey);
	} else {
	    return removeWithArray(key, height, localRootsUUID, cutoffKey);
	}
    }
    
    private AbstractNode removeWithList(Comparable key, int height, String localRootsUUID, LocatedKey cutoffKey) {
	boolean removed = this.entriesList.remove(key);
	
        if (!removed) {
            return null;	// remove will return false
        }
        
        if (getParent(false) == null) {
            return this;
        } else {
            
            if (this.entriesList.isEmpty()) {
        	Integer cutoff = BPlusTree.getCutoff(true, cutoffKey);
        	return getParent(false).underflowFromLeaf(key, null, height, 0, localRootsUUID, cutoff, true);
            }
            
            int size = this.entriesList.getSizeWithRule() - (BPlusTree.INTRA_NODE_CONC && !BPlusTree.POPULATING? 1 : 0);
            
            // if the removed key was the first we need to replace it in some parent's index
            Comparable replacementKey = null;

            if (size < BPlusTree.LOWER_BOUND) {
        	Integer cutoff = BPlusTree.getCutoff(true, cutoffKey);
        	replacementKey = getReplacementKeyIfNeededWithList(key);
                return getParent(false).underflowFromLeaf(key, replacementKey, height, 0, localRootsUUID, cutoff, false);
            } else if ((replacementKey = getReplacementKeyIfNeededWithList(key)) != null) {
                return getParent(false).replaceDeletedKey(key, replacementKey);
            } else {
                return BPlusTree.TRUE_NODE; // maybe a tiny faster than just getRoot() ?!
            }
        }
    }
    
    public AbstractNode removeWithArray(Comparable key, int height, String localRootsUUID, LocatedKey cutoffKey) {
	DoubleArray<Serializable> localEntries = this.getEntries(false);
	DoubleArray<Serializable> localArr = justRemove(localEntries, key);
	
        if (localArr == null) {
            return null;	// remove will return false
        }
        if (getParent(false) == null) {
            return this;
        } else {
            // if the removed key was the first we need to replace it in some parent's index
            Comparable replacementKey = getReplacementKeyIfNeeded(key);

            if (localArr.length() < BPlusTree.LOWER_BOUND) {
        	Integer cutoff = BPlusTree.getCutoff(true, cutoffKey);
                return getParent(false).underflowFromLeaf(key, replacementKey, height, 0, localRootsUUID, cutoff, false);
            } else if (replacementKey != null) {
                return getParent(false).replaceDeletedKey(key, replacementKey);
            } else {
        	return BPlusTree.TRUE_NODE;
            }
        }
    }

    private DoubleArray<Serializable> justRemove(DoubleArray<Serializable> localEntries, Comparable key) {
        // this test is performed because we need to return a new structure in
        // case an update occurs.  Value types must be immutable.
        if (!localEntries.containsKey(key)) {
            return null;
        } else {
            DoubleArray<Serializable> newArr = localEntries.removeKey(key);
            setEntries(newArr);
            return newArr;
        }
    }

    // This method assumes that there is at least one more key (which is
    // always true if this is not the root node)
    private Comparable getReplacementKeyIfNeeded(Comparable deletedKey) {
        Comparable firstKey = this.getEntries(false).firstKey();
        if (BPlusTree.COMPARATOR_SUPPORTING_LAST_KEY.compare(deletedKey, firstKey) < 0) {
            return firstKey;
        } else {
            return null; // null means that key does not need replacement
        }
    }

    private Comparable getReplacementKeyIfNeededWithList(Comparable deletedKey) {
        Comparable firstKey = this.entriesList.getFirstKey();
        if (BPlusTree.COMPARATOR_SUPPORTING_LAST_KEY.compare(deletedKey, firstKey) < 0) {
            return firstKey;
        } else {
            return null; // null means that key does not need replacement
        }
    }
    
    @Override
    DoubleArray<Serializable>.KeyVal removeBiggestKeyValue() {
	throw new RuntimeException("Not implemented");
    }

    @Override
    DoubleArray<Serializable>.KeyVal removeSmallestKeyValue() {
	throw new RuntimeException("Not implemented");
    }

    @Override
    Comparable getSmallestKey() {
	if (BPlusTree.INTRA_NODE_CONC) {
	    return this.entriesList.getFirstKey();
	} else {
	    return this.getEntries(false).firstKey();
	}
    }

    @Override
    void addKeyValue(DoubleArray.KeyVal keyValue) {
	throw new RuntimeException("Not implemented");
    }

    @Override
    void mergeWithLeftNode(AbstractNode leftNode, Comparable splitKey, int treeDepth, int height, String localRootsUUID, int cutoff) {
        LeafNode left = (LeafNode) leftNode; // this node does not know how to merge with another kind
        if (BPlusTree.INTRA_NODE_CONC) {
            this.entriesList.mergeWith(left.entriesList);
        } else {
            setEntries(getEntries(false).mergeWith(left.getEntries(false)));
        }

        LeafNode nodeBefore = left.getPrevious();
        if (nodeBefore != null) {
            this.setPrevious(nodeBefore);
            nodeBefore.setNext(this);
        }

        left.clean();
    }

    @Override
    public T get(Comparable key) {
	if (BPlusTree.INTRA_NODE_CONC) {
	    return (T) this.entriesList.get(key);
	} else {
	    DoubleArray<Serializable> localEntries = this.getEntries(false);
	    return (T) localEntries.get(key);
	}
    }

    @Override
    public boolean containsKey(boolean remote, Comparable key) {
	if (BPlusTree.INTRA_NODE_CONC) {
	    return this.entriesList.contains(key);
	} else {
	    return this.getEntries(false).containsKey(key);
	}
    }
    
    public void scan(boolean remote, Comparable key, int length) {
	this.getEntries(false);
    }

    @Override
    int shallowSize() {
	if (BPlusTree.INTRA_NODE_CONC) {
	    return this.entriesList.getSizeWithRule();
	} else {
	    return this.getEntries(false).length();
	}
    }

    @Override
    public int size() {
	if (BPlusTree.INTRA_NODE_CONC) {
	    return this.entriesList.getSize(false);
	} else {
	    return this.getEntries(false).length();
	}
    }

    @Override
    public Iterator<T> iterator() {
	if (BPlusTree.INTRA_NODE_CONC) {
	    System.out.println("Not implemented yet");
	    System.exit(-1);
	    return null;
	} else {
	    return new LeafNodeArrayIterator(this);
	}
    }

    private class LeafNodeArrayIterator implements Iterator<T> {
        private int index;
        private Serializable[] values;
        private LeafNode current;

        LeafNodeArrayIterator(LeafNode LeafNodeArray) {
            this.index = 0;
            this.values = LeafNodeArray.getEntries(false).values;
            this.current = LeafNodeArray;
        }

        @Override
        public boolean hasNext() {
            if (index < values.length) {
                return true;
            } else {
                return this.current.getNext() != null;
            }
        }

        @Override
        public T next() {
            if (index >= values.length) {
                LeafNode nextNode = this.current.getNext();
                if (nextNode != null) {
                    this.current = nextNode;
                    this.index = 0;
                    this.values = this.current.getEntries(false).values;
                } else {
                    throw new NoSuchElementException();
                }
            }
            index++;
            return (T) values[index - 1];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This implementation does not allow element removal via the iterator");
        }

    }

    @Override
    public AbstractNode changeGroup(int newGroup) {
//	BPlusTree.wrapper.endTransaction(true);
//	BPlusTree.wrapper.startTransaction(false);
	AbstractNode result = new LeafNode(this, newGroup);
//	BPlusTree.wrapper.endTransaction(true);
//	BPlusTree.wrapper.startTransaction(false);
	return result;
    }

}
