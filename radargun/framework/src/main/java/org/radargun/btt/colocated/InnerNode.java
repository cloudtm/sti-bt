package org.radargun.btt.colocated;

import java.io.Serializable;
import java.util.Iterator;
import java.util.UUID;

import org.radargun.LocatedKey;

public class InnerNode<T extends Serializable> extends AbstractNode<T> implements Serializable {

    // private DoubleArray<AbstractNode> subNodes;
    private LocatedKey keySubNodes;
    
    // called when a root (both inner or leaf-only) node overflows
    // the left and right node are the two nodes created after the split
    InnerNode(int group, AbstractNode leftNode, AbstractNode rightNode, Comparable splitKey) {
	super(group);
	ensureKey();
	setSubNodes(new DoubleArray<AbstractNode>(AbstractNode.class, splitKey, leftNode, rightNode));
        leftNode.setParent(this);
        rightNode.setParent(this);
    }
    
    // called when inner node overflows, this is used to create one of the sides of the split
    InnerNode(int group, DoubleArray<AbstractNode> subNodes) {
	super(group);
	ensureKey();
        setSubNodes(subNodes);
        for (int i = 0; i < subNodes.length(); i++) { // smf: either don't do this or don't setParent when making new
            subNodes.values[i].setParent(this);
        }
    }
    
    InnerNode(InnerNode toClone, boolean full) {
	super(full ? -1 : BPlusTree.myGroup());
	ensureKey();
	setParent(toClone.getParent(false));
	DoubleArray<AbstractNode> subNodes = toClone.getSubNodes(false);
	setSubNodes(subNodes);
        for (int i = 0; i < subNodes.length(); i++) {
            subNodes.values[i].setParent(this);
        }
    }
    
    InnerNode(InnerNode old, int newGroup) {
	super(newGroup);
	ensureKey();
	DoubleArray<AbstractNode> doubleArray = old.getSubNodes(false);
	AbstractNode[] values = new AbstractNode[doubleArray.values.length];
	
	int i = 0;
	for (AbstractNode oldChild : doubleArray.values) {
	    values[i] = oldChild.changeGroup(newGroup);
	    values[i].setParent(this);
	    i++;
	}
	
	setSubNodes(doubleArray.changeReplication(values));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof InnerNode)) {
            return false;
        }
        InnerNode other = (InnerNode) obj;
        return this.keySubNodes.equals(other.keySubNodes) && this.group == other.group;
    }
    
    @Override
    void clean() {
        super.clean();
        BPlusTree.putCache(this.keySubNodes, null);
    }
    
    private void ensureKey() {
	if (this.keySubNodes == null) {
	    if (this.group == -1 || this.group == 0) {
		this.keySubNodes = BPlusTree.REPL_DEGREES ? BPlusTree.wrapper.createGroupingKeyWithRepl("subNodes-" + UUID.randomUUID().toString(), 0, BPlusTree.MEMBERS) : BPlusTree.wrapper.createGroupingKey("subNodes-" + UUID.randomUUID().toString(), 0);
		this.group = 0;
	    } else {
		this.keySubNodes = BPlusTree.wrapper.createGroupingKey("subNodes-" + UUID.randomUUID().toString(), this.group);
	    }
	}
    }
    
    protected DoubleArray<AbstractNode> getSubNodes(boolean ghostRead) {
	if (ghostRead) {
	    return (DoubleArray<AbstractNode>) BPlusTree.cacheGetShadow(this.keySubNodes);
	} else {
	    return (DoubleArray<AbstractNode>) BPlusTree.getCache(this.keySubNodes);
	}
    }
    
    protected void setSubNodes(DoubleArray<AbstractNode> subNodes) {
	BPlusTree.putCache(this.keySubNodes, subNodes);
    }
    
    @Override
    public AbstractNode insert(boolean remote, Comparable key, Serializable value, int height, String localRootsUUID, LocatedKey cutoffKey) {
	if (!this.isFullyReplicated() && !remote && this.getGroup() != BPlusTree.myGroup()) {
	    try {
		return (AbstractNode) AbstractNode.executeDEF(BPlusTree.wrapper.createCacheCallable(new InsertRemoteTask(this, key, value, height, localRootsUUID, cutoffKey)), super.parentKey);
	    } catch (Exception e) {
		if (e instanceof RuntimeException) {
		    throw (RuntimeException) e;
		}
		e.printStackTrace();
		System.exit(-1);
		return null;
	    }
	} else {
	    // normal execution
	    DoubleArray<AbstractNode> subNodes = this.getSubNodes(true);
	    
	    for (int i = 0; i < subNodes.length(); i++) {
		Comparable splitKey = subNodes.keys[i];
		if (BPlusTree.COMPARATOR_SUPPORTING_LAST_KEY.compare(splitKey, key) > 0) { // this will eventually be true because the LAST_KEY is greater than all
		    return subNodes.values[i].insert(remote, key, value, height + 1, localRootsUUID, cutoffKey);
		}
	    }
	    throw new RuntimeException("findSubNode() didn't find a suitable sub-node!?");
	}
    }
    
    @Override
    public RebalanceBoolean insert(Comparable key, Serializable value, int height, String localRootsUUID, LocatedKey cutoffKey) {
	DoubleArray<AbstractNode> subNodes = this.getSubNodes(true);

	for (int i = 0; i < subNodes.length(); i++) {
	    Comparable splitKey = subNodes.keys[i];
	    if (BPlusTree.COMPARATOR_SUPPORTING_LAST_KEY.compare(splitKey, key) > 0) { // this will eventually be true because the LAST_KEY is greater than all
		return subNodes.values[i].insert(key, value, height + 1, localRootsUUID, cutoffKey);
	    }
	}
	throw new RuntimeException("findSubNode() didn't find a suitable sub-node!?");
    }

    // this method is invoked when a node in the next depth level got full, it
    // was split and now needs to pass a new key to its parent (this)
    AbstractNode rebase(AbstractNode subLeftNode, AbstractNode subRightNode, Comparable middleKey, int treeDepth, int height, String localRootsUUID, LocatedKey cutoffKey, int cutoff) {
	DoubleArray<AbstractNode> newArr = justInsertUpdatingParentRelation(middleKey, subLeftNode, subRightNode);
        if (newArr.length() <= BPlusTree.MAX_NUMBER_OF_ELEMENTS) { // this node can accommodate the new split
            return BPlusTree.TRUE_NODE;
        } else { // must split this node
            // find middle position (key to move up amd sub-node to move left)
            Comparable keyToSplit = newArr.keys[BPlusTree.LOWER_BOUND];
            AbstractNode subNodeToMoveLeft = newArr.values[BPlusTree.LOWER_BOUND];

            // Split node in two.  Notice that the 'keyToSplit' is left out of this level.  It will be moved up.
            DoubleArray<AbstractNode> leftSubNodes = newArr.leftPart(BPlusTree.LOWER_BOUND, 1);
            leftSubNodes.keys[leftSubNodes.length() - 1] = BPlusTree.LAST_KEY;
            leftSubNodes.values[leftSubNodes.length() - 1] = subNodeToMoveLeft;
            
            InnerNode leftNode = new InnerNode<T>(this.group, leftSubNodes);
            InnerNode rightNode = new InnerNode<T>(this.group, newArr.rightPart(BPlusTree.LOWER_BOUND + 1));
            int farFromTop = treeDepth - height;
            subNodeToMoveLeft.setParent(leftNode); // smf: maybe it is not necessary because of the code in the constructor

            if (farFromTop == cutoff) {
        	BPlusTree.updateLocalRoots(localRootsUUID, leftNode, rightNode, this);
            }
            
            InnerNode parent = this.getParent(false);
            this.clean();
            
            // propagate split to parent
            if (parent == null) {
        	// (treeDepth + 1) is the new actual depth; the +1 will be created in the following lines
        	// if this value is larger than the cutoff, then we already have partial replication tree nodes
        	// in that case, we have to move the cutoff point by one level 
        	if ((treeDepth + 1) > cutoff) {
        	    InnerNode newRoot = new InnerNode<T>(-1, leftNode, rightNode, keyToSplit);
        	    InnerNode possibleNew = newRoot.fixLocalRootsMoveCutoffUp(localRootsUUID, cutoff, 1, treeDepth);
        	    return (possibleNew != null) ? possibleNew : newRoot;
        	} else {
        	    InnerNode newRoot = new InnerNode<T>(this.group, leftNode, rightNode, keyToSplit);
        	    return newRoot;
        	}
            } else {
                return parent.rebase(leftNode, rightNode, keyToSplit, treeDepth, height + 1, localRootsUUID, cutoffKey, cutoff);
            }
        }
    }
    
    // this method is invoked when a node in the next depth level got full, it
    // was split and now needs to pass a new key to its parent (this)
    RebalanceBoolean rebase(boolean dummy, AbstractNode subLeftNode, AbstractNode subRightNode, Comparable middleKey, int treeDepth, int height, String localRootsUUID, LocatedKey cutoffKey, int cutoff) {
	DoubleArray<AbstractNode> newArr = justInsertUpdatingParentRelation(middleKey, subLeftNode, subRightNode);
	
/*        if (this.isFullyReplicated()) {
            BPlusTree.wrapper.endTransaction(true);
            BPlusTree.wrapper.startTransaction(false);
        }*/
	
        if (newArr.length() <= BPlusTree.MAX_NUMBER_OF_ELEMENTS) { // this node can accommodate the new split
            return new RebalanceBoolean(false, BPlusTree.TRUE_NODE);
        } else { // must split this node
            // find middle position (key to move up amd sub-node to move left)
            Comparable keyToSplit = newArr.keys[BPlusTree.LOWER_BOUND];
            AbstractNode subNodeToMoveLeft = newArr.values[BPlusTree.LOWER_BOUND];

            // Split node in two.  Notice that the 'keyToSplit' is left out of this level.  It will be moved up.
            DoubleArray<AbstractNode> leftSubNodes = newArr.leftPart(BPlusTree.LOWER_BOUND, 1);
            leftSubNodes.keys[leftSubNodes.length() - 1] = BPlusTree.LAST_KEY;
            leftSubNodes.values[leftSubNodes.length() - 1] = subNodeToMoveLeft;
            
            InnerNode leftNode = new InnerNode<T>(this.group, leftSubNodes);
            InnerNode rightNode = new InnerNode<T>(this.group, newArr.rightPart(BPlusTree.LOWER_BOUND + 1));
            int farFromTop = treeDepth - height;
            subNodeToMoveLeft.setParent(leftNode); // smf: maybe it is not necessary because of the code in the constructor

            if (farFromTop == cutoff) {
        	BPlusTree.updateLocalRoots(localRootsUUID, leftNode, rightNode, this);
            }
            
            InnerNode parent = this.getParent(false);
            this.clean();
            
            // propagate split to parent
            if (parent == null) {
        	// (treeDepth + 1) is the new actual depth; the +1 will be created in the following lines
        	// if this value is larger than the cutoff, then we already have partial replication tree nodes
        	// in that case, we have to move the cutoff point by one level 
        	if ((treeDepth + 1) > cutoff) {
        	    InnerNode newRoot = new InnerNode<T>(-1, leftNode, rightNode, keyToSplit);
        	    InnerNode possibleNew = newRoot.fixLocalRootsMoveCutoffUp(localRootsUUID, cutoff, 1, treeDepth);
        	    return new RebalanceBoolean(true, (possibleNew != null) ? possibleNew : newRoot);
        	} else {
        	    InnerNode newRoot = new InnerNode<T>(this.group, leftNode, rightNode, keyToSplit);
        	    return new RebalanceBoolean(true, newRoot);
        	}
            } else {
                return parent.rebase(false, leftNode, rightNode, keyToSplit, treeDepth, height + 1, localRootsUUID, cutoffKey, cutoff);
            }
        }
    }
    
    // distanceToTop starts with 1 and increases as we go down
    private InnerNode fixLocalRootsMoveCutoffUp(String localRootsUUID, int cutoff, int distanceToTop, int treeDepth) {
	if ((distanceToTop) == cutoff) {
	    // fix every child
	    DoubleArray<AbstractNode> myChildren = getSubNodes(false);
	    
	    if (treeDepth > cutoff) {
		for (int i = 0; i < myChildren.values.length; i++) {
		    // treeDepth is -1 of actual value. if we have cutoff 3, and treeDepth 3, then we just crossed the cutoff and there are no previous roots to remove
		    InnerNode toRemove = ((InnerNode) myChildren.values[i]);
		    BPlusTree.removeLocalRoot(localRootsUUID, toRemove);
		}
	    }

	    InnerNode toRet = this;
	    if (this.isPartial()) {
		// Note: following a growth, a shrink and a new growth, this last one will have the last LR as partial, and thus this check does not apply
//		try {
//		    throw new RuntimeException();
//		} catch (Exception e) {
//		    e.printStackTrace();
//		}
//		System.out.println("Should be fully replicated!");
//		System.exit(-1);
	    } else {
		toRet = this.switchToPartial();
	    }
	    
	    BPlusTree.addLocalRoot(localRootsUUID, toRet);
	    return toRet;
	    
	} else {
	    DoubleArray<AbstractNode> myChildren = getSubNodes(false);
	    
	    if (distanceToTop + 1 == cutoff) {
		AbstractNode[] newValues = new AbstractNode[myChildren.values.length];
		for (int i = 0; i < myChildren.values.length; i++) {
		    InnerNode myChild = ((InnerNode) myChildren.values[i]);
		    InnerNode modifiedNode = myChild.fixLocalRootsMoveCutoffUp(localRootsUUID, cutoff, distanceToTop + 1, treeDepth);
		    newValues[i] = modifiedNode;
		}
		if (newValues[0] == null) {
		    try {
			throw new RuntimeException();
		    } catch (Exception e) {
			e.printStackTrace();
		    }
		    System.out.println("Next level should be fully replicated!");
		    System.exit(-1);
		}
		setSubNodes(myChildren.changeReplication(newValues));
	    } else {
		for (int i = 0; i < myChildren.values.length; i++) {
		   ((InnerNode) myChildren.values[i]).fixLocalRootsMoveCutoffUp(localRootsUUID, cutoff, distanceToTop + 1, treeDepth);
		}
	    }
	    
	    return null;
	}
    }
    
    private InnerNode fixLocalRootsMoveCutoffDown(String localRootsUUID, int cutoff, int distanceToTop, int treeDepth) {
	if ((distanceToTop + 1) == cutoff) {
	    // fix every child
	    DoubleArray<AbstractNode> myChildren = getSubNodes(false);
	    
	    for (int i = 0; i < myChildren.values.length; i++) {
		InnerNode toAdd = ((InnerNode) myChildren.values[i]);
		BPlusTree.addLocalRoot(localRootsUUID, toAdd);
	    }

	    BPlusTree.removeLocalRoot(localRootsUUID, this);
	    
	    if (!this.isPartial()) {
		try {
		    throw new RuntimeException();
		} catch (Exception e) {
		    e.printStackTrace();
		}
		System.out.println("Should be partially replicated!");
		System.exit(-1);
	    }
	    
	    InnerNode toRet = this.switchToFull();
	    return toRet;
	} else {
	    DoubleArray<AbstractNode> myChildren = getSubNodes(false);
	    if ((distanceToTop + 2) == cutoff) {
		AbstractNode[] newValues = new AbstractNode[myChildren.values.length];
		for (int i = 0; i < myChildren.values.length; i++) {
		    InnerNode myChild = ((InnerNode) myChildren.values[i]);
		    InnerNode modifiedNode = myChild.fixLocalRootsMoveCutoffDown(localRootsUUID, cutoff, distanceToTop + 1, treeDepth);
		    newValues[i] = modifiedNode;
		}
		if (newValues[0] == null) {
		    try {
			throw new RuntimeException();
		    } catch (Exception e) {
			e.printStackTrace();
		    }
		    System.out.println("Next level should be partial replicated!");
		    System.exit(-1);
		}
		setSubNodes(myChildren.changeReplication(newValues));
	    } else {
		AbstractNode[] newValues = new AbstractNode[myChildren.values.length];
		for (int i = 0; i < myChildren.values.length; i++) {
		    ((InnerNode) myChildren.values[i]).fixLocalRootsMoveCutoffDown(localRootsUUID, cutoff, distanceToTop + 1, treeDepth);
		}
	    }
	    return null;
	}
    }
    
    private void removeAllLocalRoots(String localRootsUUID, int cutoff, int distanceToTop) {
	if ((distanceToTop + 1) == cutoff) {
	    BPlusTree.removeLocalRoot(localRootsUUID, this);
	    return;
	} else {
	    DoubleArray<AbstractNode> myChildren = getSubNodes(false);
	    for (int i = 0; i < myChildren.values.length; i++) {
		((InnerNode) myChildren.values[i]).removeAllLocalRoots(localRootsUUID, cutoff, distanceToTop + 1);
	    }
	    return;
	}
    }
    
    private boolean isPartial() {
	return this.group != 0;
    }
    
    private InnerNode switchToPartial() {
	return new InnerNode(this, false);
    }
    
    private InnerNode switchToFull() {
	return new InnerNode(this, true);
    }

    private DoubleArray<AbstractNode> justInsert(Comparable middleKey, AbstractNode subLeftNode,
            AbstractNode subRightNode) {
        DoubleArray<AbstractNode> newArr = getSubNodes(false).duplicateAndAddKey(middleKey, subLeftNode, subRightNode);
        setSubNodes(newArr);
        return newArr;
    }

    public DoubleArray<AbstractNode> justInsertUpdatingParentRelation(Comparable middleKey, AbstractNode subLeftNode,
            AbstractNode subRightNode) {
        DoubleArray<AbstractNode> newArr = justInsert(middleKey, subLeftNode, subRightNode);
        subLeftNode.setParent(this);
        subRightNode.setParent(this);
        return newArr;
    }

    @Override
    public AbstractNode remove(boolean remote, Comparable key, int height, String localRootsUUID, LocatedKey cutoffKey) {
	if (!this.isFullyReplicated() && !remote && this.getGroup() != BPlusTree.myGroup()) {
	    try {
		return (AbstractNode) AbstractNode.executeDEF(BPlusTree.wrapper.createCacheCallable(new RemoveRemoteTask(this, key, height, localRootsUUID, cutoffKey)), super.parentKey);
	    } catch (Exception e) {
		if (e instanceof RuntimeException) {
		    throw (RuntimeException) e;
		}
		e.printStackTrace();
		System.exit(-1);
		return null;
	    }
	} else {
	    DoubleArray<AbstractNode> subNodes = this.getSubNodes(true);

	    for (int i = 0; i < subNodes.length(); i++) {
		Comparable splitKey = subNodes.keys[i];
		if (BPlusTree.COMPARATOR_SUPPORTING_LAST_KEY.compare(splitKey, key) > 0) { // this will eventually be true because the LAST_KEY is greater than all
		    return subNodes.values[i].remove(remote, key, height + 1, localRootsUUID, cutoffKey);
		}
	    }
	    throw new RuntimeException("findSubNode() didn't find a suitable sub-node!?");
	}
    }

    AbstractNode replaceDeletedKey(Comparable deletedKey, Comparable replacementKey) {
        AbstractNode subNode = this.getSubNodes(false).get(deletedKey);
        if (subNode != null) { // found the key a this level
            return replaceDeletedKey(deletedKey, replacementKey, subNode);
        } else if (this.getParent(false) != null) {
            return this.getParent(false).replaceDeletedKey(deletedKey, replacementKey);
        } else {
            return BPlusTree.TRUE_NODE;
        }
    }

    // replaces the key for the given sub-node.  The deletedKey is expected to exist in this node
    private AbstractNode replaceDeletedKey(Comparable deletedKey, Comparable replacementKey, AbstractNode subNode) {
        setSubNodes(getSubNodes(false).replaceKey(deletedKey, replacementKey, subNode));
        return BPlusTree.TRUE_NODE;
    }

    /*
     * Deal with underflow from LeafNodeArray
     */

    // null in replacement key means that deletedKey does not have to be
    // replaced. Corollary: the deleted key was not the first key in its leaf
    // node
    AbstractNode underflowFromLeaf(Comparable deletedKey, Comparable replacementKey, int treeDepth, int height, String localRootsUUID, int cutoff, boolean leafEmpty) {
        DoubleArray<AbstractNode> subNodes = this.getSubNodes(false);
        int iter = 0;
        // first, identify the deletion point
        while (BPlusTree.COMPARATOR_SUPPORTING_LAST_KEY.compare(subNodes.keys[iter], deletedKey) <= 0) {
            iter++;
        }

        // Now, 'entryValue' holds the child where the deletion occurred.
        Comparable entryKey = subNodes.keys[iter];
        AbstractNode entryValue = subNodes.values[iter];

        Comparable previousEntryKey = iter > 0 ? subNodes.keys[iter - 1] : null;
        AbstractNode previousEntryValue = iter > 0 ? subNodes.values[iter - 1] : null;

        Comparable nextEntryKey = null;
        AbstractNode nextEntryValue = null;

        /*
         * Decide whether to shift or merge, and whether to use the left
         * or the right sibling.  We prefer merging to shifting.
         *
         * Also, we may need to replace the deleted key in some scenarios
         * (namely when the key was deleted from the left side of a node
         * AND that side was not changed by a merge/move with/from the left.
         */
        if (iter == 0) { // the deletedKey was removed from the first sub-node
            nextEntryKey = subNodes.keys[iter + 1]; // always exists because of LAST_KEY
            nextEntryValue = subNodes.values[iter + 1];

            // Flexible: if first, always merge right, possibly exceeding upper bound
            // if (nextEntryValue.shallowSize() == BPlusTree.LOWER_BOUND) { // can we merge with the right?
            if (!leafEmpty) {
                rightLeafMerge(entryKey, entryValue, nextEntryValue, treeDepth, height, localRootsUUID, cutoff);
            } else {
        	replacementKey = nextEntryValue.getSmallestKey();
        	setSubNodes(getSubNodes(false).removeKey(entryKey));
            }
            //} 
            // else { // cannot merge with the right. We have to move an element from the right to here
            //    moveChildFromRightToLeft(entryKey, entryValue, nextEntryValue);
            //}
            if (replacementKey != null && this.getParent(false) != null) { // the deletedKey occurs somewhere atop only
                this.getParent(false).replaceDeletedKey(deletedKey, replacementKey);
            }
        } else if (iter >= (subNodes.length() - 1)) {
            // Flexible: if deleted the last one then cannot merge with right
            leftLeafMerge(previousEntryKey, previousEntryValue, entryValue, treeDepth, height, localRootsUUID, cutoff);
        } else {
            // Flexible: in the other cases, toss coin
            if (ThreadLocalRandom.current().nextBoolean()) {
        	leftLeafMerge(previousEntryKey, previousEntryValue, entryValue, treeDepth, height, localRootsUUID, cutoff);
            } else {
        	nextEntryValue = subNodes.values[iter + 1];
        	if (!leafEmpty) {
        	    rightLeafMerge(entryKey, entryValue, nextEntryValue, treeDepth, height, localRootsUUID, cutoff);
        	} else {
        	    replacementKey = nextEntryValue.getSmallestKey();
        	    setSubNodes(getSubNodes(false).removeKey(entryKey));
        	}
        	if (replacementKey != null) {
        	    this.replaceDeletedKey(deletedKey, replacementKey, previousEntryValue);
        	}
            }
        } 
//        else if (previousEntryValue.shallowSize() == BPlusTree.LOWER_BOUND) { // can we merge with the left?
//            leftLeafMerge(previousEntryKey, previousEntryValue, entryValue, treeDepth, height, localRootsUUID, cutoff);
//        } else {  // cannot merge with the left
//            if (iter >= (subNodes.length() - 1)
//                    || (nextEntryValue = subNodes.values[iter + 1]).shallowSize() > BPlusTree.LOWER_BOUND) { // caution: tricky test!!
//                // either there is no next or the next is above the lower bound
//                moveChildFromLeftToRight(previousEntryKey, previousEntryValue, entryValue);
//            } else {
//                rightLeafMerge(entryKey, entryValue, nextEntryValue, treeDepth, height, localRootsUUID, cutoff);
//                if (replacementKey != null) { // the deletedKey occurs anywhere (or at this level ONLY?)
//                    this.replaceDeletedKey(deletedKey, replacementKey, previousEntryValue);
//                }
//            }
//        }
        return checkForUnderflow(treeDepth, height + 1, localRootsUUID, cutoff);
    }

    private AbstractNode checkForUnderflow(int treeDepth, int height, String localRootsUUID, int cutoff) {
        DoubleArray<AbstractNode> localSubNodes = this.getSubNodes(false);

        // Now, just check for underflow in this node.   The LAST_KEY is fake, so it does not count for the total.
        if (localSubNodes.length() < BPlusTree.LOWER_BOUND_WITH_LAST_KEY) {
            // the LAST_KEY is merely an indirection.  This only occurs in the root node.  We can reduce one depth.
            if (localSubNodes.length() == 1) { // This only occurs in the root node
                // (size == 1) => (parent == null), but NOT the inverse
                AbstractNode child = localSubNodes.firstValue();
                child.setParent(null);
                
                this.clean();

                if ((treeDepth - 1) > cutoff) {
                    InnerNode newRoot = (InnerNode) child;
                    InnerNode possiblyNew = newRoot.fixLocalRootsMoveCutoffDown(localRootsUUID, cutoff, 1, treeDepth);
                    InnerNode toReturn = possiblyNew != null ? possiblyNew : newRoot;
                    return toReturn;
                } else if ((treeDepth - 1) == cutoff) {
                    System.out.println("Shrinking tree, should not happen!: " + treeDepth + " " + height + " " + cutoff);
                    try {
                	throw new RuntimeException();
                    } catch (Exception e) {
                	e.printStackTrace();
                	System.out.println("Shrinking tree, should not happen!: " + treeDepth + " " + height + " " + cutoff);
                	System.exit(-1);
                    }
                    InnerNode newRoot = (InnerNode) child;
                    newRoot.removeAllLocalRoots(localRootsUUID, cutoff, 1);
                    return newRoot;
                }
                
                return child;
            } else if (this.getParent(false) != null) {
                return this.getParent(false).underflowFromInner(this, treeDepth, height, localRootsUUID, cutoff);
            }
        }
        return BPlusTree.TRUE_NODE;
    }

    private void rightLeafMerge(Comparable entryKey, AbstractNode entryValue, AbstractNode nextEntryValue, int treeDepth, int height, String localRootsUUID, int cutoff) {
        leftLeafMerge(entryKey, entryValue, nextEntryValue, treeDepth, height, localRootsUUID, cutoff);
    }

    private void leftLeafMerge(Comparable previousEntryKey, AbstractNode previousEntryValue, AbstractNode entryValue, int treeDepth, int height, String localRootsUUID, int cutoff) {
        entryValue.mergeWithLeftNode(previousEntryValue, null, treeDepth, height, localRootsUUID, cutoff);
        // remove the superfluous node
        setSubNodes(getSubNodes(false).removeKey(previousEntryKey));
    }

    @Override
    void mergeWithLeftNode(AbstractNode leftNode, Comparable splitKey, int treeDepth, int height, String localRootsUUID, int cutoff) {
        InnerNode left = (InnerNode) leftNode;  // this node does not know how to merge with another kind

        // change the parent of all the left sub-nodes
        DoubleArray<AbstractNode> subNodes = this.getSubNodes(false);
        DoubleArray<AbstractNode> leftSubNodes = left.getSubNodes(false);
        InnerNode uncle = subNodes.values[subNodes.length() - 1].getParent(false);
        for (int i = 0; i < leftSubNodes.length(); i++) {
            leftSubNodes.values[i].setParent(uncle);
        }

        if ((treeDepth - height) == cutoff) {
            BPlusTree.removeLocalRoot(localRootsUUID, left);
        }
        
        DoubleArray<AbstractNode> newArr = subNodes.mergeWith(splitKey, leftSubNodes);
        setSubNodes(newArr);
        
        
        left.clean();
    }

    /*
     * Deal with underflow from InnerNodeArray
     */

    AbstractNode underflowFromInner(InnerNode deletedNode, int treeDepth, int height, String localRootsUUID, int cutoff) {
        DoubleArray<AbstractNode> subNodes = this.getSubNodes(false);
        int iter = 0;

        Comparable entryKey = null;
        AbstractNode entryValue = null;

        // first, identify the deletion point
        do {
            entryValue = subNodes.values[iter];
            iter++;
        } while (!entryValue.equals(deletedNode));
        // Now, the value() of 'entry' holds the child where the deletion occurred.
        entryKey = subNodes.keys[iter - 1];
        iter--;

        Comparable previousEntryKey = iter > 0 ? subNodes.keys[iter - 1] : null;
        AbstractNode previousEntryValue = iter > 0 ? subNodes.values[iter - 1] : null;
        Comparable nextEntryKey = null;
        AbstractNode nextEntryValue = null;

        /*
         * Decide whether to shift or merge, and whether to use the left
         * or the right sibling.  We prefer merging to shifting.
         */
        if (iter == 0) { // the deletion occurred in the first sub-node
            nextEntryKey = subNodes.keys[iter + 1]; // always exists because of LAST_KEY
            nextEntryValue = subNodes.values[iter + 1];

            if (nextEntryValue.shallowSize() == BPlusTree.LOWER_BOUND_WITH_LAST_KEY) { // can we merge with the right?
                rightInnerMerge(entryKey, entryValue, nextEntryValue, treeDepth, height, localRootsUUID, cutoff);
            } else { // cannot merge with the right. We have to move an element from the right to here
                rotateRightToLeft(entryKey, (InnerNode) entryValue, (InnerNode) nextEntryValue);
            }
        } else if (previousEntryValue.shallowSize() == BPlusTree.LOWER_BOUND_WITH_LAST_KEY) { // can we merge with the left?
            leftInnerMerge(previousEntryKey, previousEntryValue, entryValue, treeDepth, height, localRootsUUID, cutoff);
        } else {  // cannot merge with the left
            if (iter >= (subNodes.length() - 1)
                    || (nextEntryValue = subNodes.values[iter + 1]).shallowSize() > BPlusTree.LOWER_BOUND_WITH_LAST_KEY) { // caution: tricky test!!
                // either there is no next or the next is above the lower bound
                rotateLeftToRight(previousEntryKey, (InnerNode) previousEntryValue, (InnerNode) entryValue);
            } else {
                rightInnerMerge(entryKey, entryValue, nextEntryValue, treeDepth, height, localRootsUUID, cutoff);
            }
        }

        return checkForUnderflow(treeDepth, height + 1, localRootsUUID, cutoff);
    }

    private void rightInnerMerge(Comparable entryKey, AbstractNode entryValue, AbstractNode nextEntryValue, int treeDepth, int height, String localRootsUUID, int cutoff) {
        leftInnerMerge(entryKey, entryValue, nextEntryValue, treeDepth, height, localRootsUUID, cutoff);
    }

    private void leftInnerMerge(Comparable previousEntryKey, AbstractNode previousEntryValue, AbstractNode entryValue, int treeDepth, int height, String localRootsUUID, int cutoff) {
        entryValue.mergeWithLeftNode(previousEntryValue, previousEntryKey, treeDepth, height, localRootsUUID, cutoff);
        // remove the superfluous node
        setSubNodes(getSubNodes(false).removeKey(previousEntryKey));
    }

    private void rotateLeftToRight(Comparable leftEntryKey, InnerNode leftSubNode, InnerNode rightSubNode) {
        DoubleArray<AbstractNode> leftSubNodes = leftSubNode.getSubNodes(false);
        DoubleArray<AbstractNode> rightSubNodes = rightSubNode.getSubNodes(false);

        Comparable leftHighestKey = leftSubNodes.lowerKeyThanHighest();
        AbstractNode leftHighestValue = leftSubNodes.lastValue();

        // move the highest value from the left to the right.  Use the split-key as the index.
        DoubleArray<AbstractNode> newRightSubNodes = rightSubNodes.addKeyValue(leftEntryKey, leftHighestValue);
        leftHighestValue.setParent(rightSubNode);

        // shift a new child to the last entry on the left
        leftHighestValue = leftSubNodes.get(leftHighestKey);
        DoubleArray<AbstractNode> newLeftSubNodes = leftSubNodes.removeKey(leftHighestKey);
        // this is already a duplicated array, no need to go through that process again
        newLeftSubNodes.values[newLeftSubNodes.length() - 1] = leftHighestValue;

        leftSubNode.setSubNodes(newLeftSubNodes);
        rightSubNode.setSubNodes(newRightSubNodes);

        // update the split-key to be the key we just removed from the left
        setSubNodes(getSubNodes(false).replaceKey(leftEntryKey, leftHighestKey, leftSubNode));
    }

    private void rotateRightToLeft(Comparable leftEntryKey, InnerNode leftSubNode, InnerNode rightSubNode) {
        DoubleArray<AbstractNode> leftSubNodes = leftSubNode.getSubNodes(false);
        DoubleArray<AbstractNode> rightSubNodes = rightSubNode.getSubNodes(false);

        // remove right's lowest entry
        DoubleArray<AbstractNode>.KeyVal rightLowestEntry = rightSubNodes.getSmallestKeyValue();
        DoubleArray<AbstractNode> newRightSubNodes = rightSubNodes.removeSmallestKeyValue();

        // re-index the left highest value under the split-key, which is moved down
        AbstractNode leftHighestValue = leftSubNodes.lastValue();
        DoubleArray<AbstractNode> newLeftSubNodes = leftSubNodes.addKeyValue(leftEntryKey, leftHighestValue);
        // and add the right's lowest entry on the left
        AbstractNode rightLowestValue = rightLowestEntry.val;
        // this is already a duplicated array, no need to go through that process again
        newLeftSubNodes.values[newLeftSubNodes.length() - 1] = rightLowestValue;

        rightLowestValue.setParent(leftSubNode);

        leftSubNode.setSubNodes(newLeftSubNodes);
        rightSubNode.setSubNodes(newRightSubNodes);

        // update the split-key to be the key we just removed from the right
        setSubNodes(getSubNodes(false).replaceKey(leftEntryKey, rightLowestEntry.key, leftSubNode));
    }

    @Override
    DoubleArray.KeyVal removeBiggestKeyValue() {
        throw new UnsupportedOperationException("not yet implemented: removeBiggestKeyValue from inner node");
    }

    @Override
    DoubleArray.KeyVal removeSmallestKeyValue() {
        throw new UnsupportedOperationException("not yet implemented: removeSmallestKeyValue from inner node");
    }

    @Override
    Comparable getSmallestKey() {
        throw new UnsupportedOperationException("not yet implemented: getSmallestKey from inner node");
    }

    @Override
    void addKeyValue(DoubleArray.KeyVal keyValue) {
        throw new UnsupportedOperationException("not yet implemented: addKeyValue to inner node should account for LAST_KEY ?!?");
    }

    @Override
    public T get(Comparable key) {
	DoubleArray<AbstractNode> subNodes = this.getSubNodes(true);
	
	for (int i = 0; i < subNodes.length(); i++) {
            Comparable splitKey = subNodes.keys[i];
            if (BPlusTree.COMPARATOR_SUPPORTING_LAST_KEY.compare(splitKey, key) > 0) { // this will eventually be true because the LAST_KEY is greater than all
                return (T) subNodes.values[i].get(key);
            }
        }
        throw new RuntimeException("findSubNode() didn't find a suitable sub-node!?");
    }

    public void scan(boolean remote, Comparable key, int length) {
	if (!this.isFullyReplicated() && !remote && this.getGroup() != BPlusTree.myGroup()) {
	    try {
		AbstractNode.executeDEF(BPlusTree.wrapper.createCacheCallable(new ScanRemoteTask(this, key, length)), super.parentKey);
		return;
	    } catch (Exception e) {
		if (e instanceof RuntimeException) {
		    throw (RuntimeException) e;
		}
		e.printStackTrace();
		System.exit(-1);
		return;
	    }
	} else {
	    DoubleArray<AbstractNode> subNodes = this.getSubNodes(true);

	    if (subNodes.values[0] instanceof LeafNode) {
		for (int i = 0; i < subNodes.length(); i++) {
		    subNodes.values[i].scan(remote, key, length);
		}
		return;
	    } else {
		for (int i = 0; i < subNodes.length(); i++) {
		    Comparable splitKey = subNodes.keys[i];
		    if (BPlusTree.COMPARATOR_SUPPORTING_LAST_KEY.compare(splitKey, key) > 0) { // this will eventually be true because the LAST_KEY is greater than all
			subNodes.values[i].scan(remote, key, length);
			return;
		    }
		}
	    }
	    throw new RuntimeException("findSubNode() didn't find a suitable sub-node!?");
	}
    }
    
    @Override
    public boolean containsKey(boolean remote, Comparable key) {
	if (!this.isFullyReplicated() && !remote && this.getGroup() != BPlusTree.myGroup()) {
	    try {
		return (Boolean) AbstractNode.executeDEF(BPlusTree.wrapper.createCacheCallable(new ContainsRemoteTask(this, key)), super.parentKey);
	    } catch (Exception e) {
		if (e instanceof RuntimeException) {
		    throw (RuntimeException) e;
		}
		e.printStackTrace();
		System.exit(-1);
		return false;
	    }
	} else {
	    DoubleArray<AbstractNode> subNodes = this.getSubNodes(true);

	    for (int i = 0; i < subNodes.length(); i++) {
		Comparable splitKey = subNodes.keys[i];
		if (BPlusTree.COMPARATOR_SUPPORTING_LAST_KEY.compare(splitKey, key) > 0) { // this will eventually be true because the LAST_KEY is greater than all
		    return subNodes.values[i].containsKey(remote, key);
		}
	    }
	    throw new RuntimeException("findSubNode() didn't find a suitable sub-node!?");
	}
    }

    @Override
    int shallowSize() {
        return this.getSubNodes(false).length();
    }

    @Override
    public int size() {
        int total = 0;
        DoubleArray<AbstractNode> subNodes = this.getSubNodes(false);
        for (int i = 0; i < subNodes.length(); i++) {
            total += subNodes.values[i].size();
        }
        return total;
    }

    @Override
    public Iterator<T> iterator() {
        return this.getSubNodes(false).firstValue().iterator();
    }

    public InnerNode startChangeGroup(int newGroup) {
	InnerNode newMe = (InnerNode) this.changeGroup(newGroup);
	InnerNode parent = this.getParent(false);
	newMe.setParent(parent);
	DoubleArray<AbstractNode> parentsChildren = parent.getSubNodes(false);
	parent.setSubNodes(parentsChildren.replaceChild(this, newMe));
	return newMe;
    }
    
    public AbstractNode changeGroup(int newGroup) {
//	BPlusTree.wrapper.endTransaction(true);
//	BPlusTree.wrapper.startTransaction(false);
	AbstractNode result = new InnerNode(this, newGroup);
//	BPlusTree.wrapper.endTransaction(true);
//	BPlusTree.wrapper.startTransaction(false);
	return result;
    }
    
}
