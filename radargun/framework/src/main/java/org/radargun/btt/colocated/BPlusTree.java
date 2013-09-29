package org.radargun.btt.colocated;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;

public class BPlusTree<T extends Serializable> implements Serializable, Iterable<T> {
    private static final class ComparableLastKey implements Comparable, Serializable {
        private static final Serializable LAST_KEY_SERIALIZED_FORM = new Serializable() {
            protected Object readResolve() throws ObjectStreamException {
                return LAST_KEY;
            }
        };

        @Override
        public int compareTo(Object c) {
            if (c == null) {
                // because comparing the other way around would cause a NullPointerException
                throw new NullPointerException();
            } else if (c == this) {
                return 0;
            }
            return 1; // this key is always greater than any other, except itself.
        }

        @Override
        public String toString() {
            return "LAST_KEY";
        }

        // This object's serialization is special.  We need to ensure that two deserializations of
        // the same object will provide the same instance, so that we can compare using == in the
        // ComparatorSupportingLastKey
        protected Object writeReplace() throws ObjectStreamException {
            return LAST_KEY_SERIALIZED_FORM;
        }
    }

    static final Comparable LAST_KEY = new ComparableLastKey();

    /* Special comparator that takes into account LAST_KEY */
    private static class ComparatorSupportingLastKey implements Comparator<Comparable>, Serializable {
        // only LAST_KEY knows how to compare itself with others, so we must check for it before
        // delegating the comparison to the Comparables.
        @Override
        public int compare(Comparable o1, Comparable o2) {
            if (o1 == LAST_KEY) {
                return o1.compareTo(o2);
            } else if (o2 == LAST_KEY) {
                return -o2.compareTo(o1);
            }
            // neither is LAST_KEY.  Compare them normally.
            return o1.compareTo(o2);
        }
    }

    static final Comparator COMPARATOR_SUPPORTING_LAST_KEY = new ComparatorSupportingLastKey();

    // The minimum lower bound is 2 (two).  Only edit this.  The other values are derived.
    public transient static int LOWER_BOUND;
    // The LAST_KEY takes an entry but will still maintain the lower bound
    public transient static int LOWER_BOUND_WITH_LAST_KEY;
    // The maximum number of keys in a node NOT COUNTING with the special LAST_KEY. This number should be a multiple of 2.
    public transient static int MAX_NUMBER_OF_KEYS;
    public transient static int MAX_NUMBER_OF_ELEMENTS;

    static StringBuilder spaces(int level) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < level; i++) {
            str.append(' ');
        }
        return str;
    }

    public static CacheWrapper wrapper;
    
    public static <V> V getCache(Object key) {
	V value = (V) wrapper.get(key);
	if (value == null || value instanceof NullObject) {
	    return null;
	} else {
	    return value;
	}
    }
    
    public static void putCache(Object key, Object value) {
	if (value == null) {
	    wrapper.registerKey(key);
	    wrapper.put(key, NULL);
	} else {
	    wrapper.registerKey(key);
	    wrapper.put(key, value);
	}
    }
    
    public static final NullObject NULL = new NullObject();

    // non-static part start here

    private int group;
    private LocatedKey rootKey;
    public String localRootsUUID;
    private LocatedKey cutoffKey;

    public static int MEMBERS;
    public static boolean COLOCATE;
    public static boolean GHOST;
    public static boolean REPL_DEGREES;
    public static boolean THREAD_MIGRATION;
    public static boolean INTRA_NODE_CONC;
    public static boolean POPULATING;
    
    transient ColocationThread colocationThread;
    
    public boolean colocate() {
	if (colocationThread != null) {
	return this.colocationThread.colocate();
	} else { return false; }
    }
    
    public BPlusTree(int clusterSize, boolean threadMigration, boolean doColocation, boolean ghostReads, boolean replicationDegrees) {
	this.group = 0;
	    
	MEMBERS = clusterSize;
	THREAD_MIGRATION = threadMigration;
	COLOCATE = doColocation;
	GHOST = ghostReads;
	REPL_DEGREES = replicationDegrees;
	
	this.localRootsUUID = UUID.randomUUID().toString();
	this.rootKey = REPL_DEGREES ? wrapper.createGroupingKeyWithRepl("root-" + UUID.randomUUID(), this.group, clusterSize) : wrapper.createGroupingKey("root-" + UUID.randomUUID(), 0);
	this.cutoffKey = REPL_DEGREES ? wrapper.createGroupingKeyWithRepl("cutoff-" + UUID.randomUUID(), this.group, clusterSize) : wrapper.createGroupingKey("cutoff-" + UUID.randomUUID(), group);
	int cutoff = (int)Math.ceil(Math.log(clusterSize) / Math.log(MAX_NUMBER_OF_ELEMENTS)) + 2;
	System.out.println("Setting cutoff value: " + cutoff + " due to members: " + clusterSize + " and max number elements: " + MAX_NUMBER_OF_ELEMENTS);
	if (cutoff < 2) {
	    cutoff = 2;
	    System.out.println("Overriding cutoff to 2");
	}
	setCutoff(this.cutoffKey, cutoff);
	setRoot(new LeafNode<T>(BPlusTree.myGroup()));
	
	if (threadMigration) {
	    this.colocationThread = new ColocationThread(this); //.start();
	}
    }
    
    public static Integer getCutoff(boolean ghost, LocatedKey key) {
	if (ghost) {
	    return (Integer) wrapper.getGhost(key);
	} else {
	    return (Integer) wrapper.get(key);
	}
    }
    
    public static void setCutoff(LocatedKey key, int value) {
	wrapper.put(key, value);
    }
    
    public static List<InnerNode> getLocalRoots(LocatedKey key) {
	return (List<InnerNode>) wrapper.get(key);
    }
    
    public static void setLocalRoots(LocatedKey key, List<InnerNode> list) {
	wrapper.put(key, list);
    }
    
    protected AbstractNode getRoot(boolean ghostRead) {
	if (ghostRead && GHOST) {
	    return (AbstractNode) wrapper.getGhost(this.rootKey);
	} else {
	    return (AbstractNode) wrapper.get(this.rootKey);
	}
    }
    
    protected void setRoot(AbstractNode root) {
	wrapper.put(this.rootKey, root);
    }

    public static final transient AbstractNode TRUE_NODE = new LeafNode(true, LeafNode.TRUTH);
    
    /** Inserts the given key-value pair, overwriting any previous entry for the same key */
    public boolean insert(Comparable key, T value) {
        if (value == null) {
            throw new UnsupportedOperationException("This B+Tree does not support nulls");
        }
        AbstractNode rootNode = this.getRoot(true);
        AbstractNode resultNode = rootNode.insert(false, key, value, 1, this.localRootsUUID, this.cutoffKey);
        
        if (resultNode == null) {
            return false;
        } else if (resultNode.group == TRUE_NODE.group) {
            return true; // and no rebalance ocurred!
        }
        
        if (!rootNode.equals(resultNode)) {
            this.setRoot(resultNode);
        }
        return true;
    }
    
    // hack to try to partition better the transactions when expensive rebalances occur
    public boolean insert(Comparable key) {
        AbstractNode rootNode = this.getRoot(true);
        RebalanceBoolean result = rootNode.insert(key, (T) key, 1, this.localRootsUUID, this.cutoffKey);
        AbstractNode resultNode = result.node;
        
        if (resultNode == null) {
            return result.rebalance;
        } else if (resultNode.group == TRUE_NODE.group) {
            return result.rebalance; // and no rebalance ocurred!
        }
        
        if (!rootNode.equals(resultNode)) {
            this.setRoot(resultNode);
System.out.println("Should be true: " + result.rebalance);
            return result.rebalance;
        }
        return result.rebalance;
    }
    
    /** Removes the element with the given key */
    public boolean removeKey(Comparable key) {
        AbstractNode rootNode = this.getRoot(true);
        AbstractNode resultNode = rootNode.remove(false, key, 1, this.localRootsUUID, this.cutoffKey);

        if (resultNode == null) {
            return false;
        } else if (resultNode.group == TRUE_NODE.group) {
            return true; // and no rebalance ocurred!
        }
        
        if (!rootNode.equals(resultNode)) {
            this.setRoot(resultNode);
        }
        return true;
    }

    /**
     * Returns the value to which the specified key is mapped, or <code>null</code> if this map
     * contains no mapping for the key.
     */
    public T get(Comparable key) {
        return ((AbstractNode<T>) this.getRoot(true)).get(key);
    }

    /** Returns <code>true</code> if this map contains a mapping for the specified key. */
    public boolean containsKey(Comparable key) {
	AbstractNode root = this.getRoot(true);
	// System.out.println(Thread.currentThread().getId() + "] " + "root from btree: " + root);
        return root.containsKey(false, key);
    }
    
    public void scan(Comparable key, int length) {
	this.getRoot(true).scan(false, key, length);
    }

    /** Returns the number of key-value mappings in this map */
    public int size() {
        return this.getRoot(false).size();
    }

    @Override
    public Iterator<T> iterator() {
        return this.getRoot(false).iterator();
    }

    public boolean myEquals(BPlusTree other) {
        Iterator<T> it1 = this.iterator();
        Iterator<T> it2 = other.iterator();

        while (it1.hasNext() && it2.hasNext()) {
            T o1 = it1.next();
            T o2 = it2.next();

            if (!((o1 == null && o2 == null) || (o1.equals(o2)))) {
                return false;
            }
        }
        return true;
    }

    public static final void registerGet(LocatedKey key) {
	wrapper.registerKey(key);
    }
    
    public static final <E> E cacheGetShadow(LocatedKey key) {
	E value = GHOST ? (E) wrapper.getGhost(key) : (E) wrapper.get(key);
	if (value == null || value instanceof NullObject) {
	    return null;
	} else {
	    return value;
	}
    }
    
    public static Integer myGroup() {
	return wrapper.getLocalGrouping();
    }
    
    public static void addLocalRoot(String localRootsUUID, InnerNode localRoot) {
	if (!BPlusTree.THREAD_MIGRATION) {
	    return;
	}
	final LocatedKey localRootsKey = BPlusTree.wrapper.createGroupingKey(localRootsUUID + "-localRoots-" + localRoot.group, localRoot.group);
	List<InnerNode> localRoots = getLocalRoots(localRootsKey);
	if (localRoots == null) {
	    localRoots = new ArrayList<InnerNode>();
	}
//	System.out.println(Thread.currentThread().getId() + "] " + "Add: " + Arrays.toString(localRoots.toArray()) + " | add " + localRoot + "\t" + localRootsKey);
	localRoots = new ArrayList<InnerNode>(localRoots);
	localRoots.add(localRoot);
	setLocalRoots(localRootsKey, localRoots);
    }
    
    public static void removeLocalRoot(String localRootsUUID, InnerNode localRoot) {
	if (!BPlusTree.THREAD_MIGRATION) {
	    return;
	}
	final LocatedKey localRootsKey = BPlusTree.wrapper.createGroupingKey(localRootsUUID + "-localRoots-" + localRoot.group, localRoot.group);
	List<InnerNode> localRoots = getLocalRoots(localRootsKey);
	
	if (localRoots == null) {
	    return;
	}
	
	localRoots = new ArrayList<InnerNode>(localRoots);
	if (!localRoots.remove(localRoot)) {
	    System.out.println(Thread.currentThread().getId() + "] " + "Remove: " + Arrays.toString(localRoots.toArray()) + " | remove " + localRoot + "\t" + localRootsKey);
	    try {
		System.out.println(Thread.currentThread().getId() + "] " + "Could not find node to remove in list: " + localRootsKey + " " + localRoot);
		for (InnerNode lr : localRoots) {
		    System.out.println(Thread.currentThread().getId() + "] " + lr + " ---> " + Arrays.toString(lr.getSubNodes(true).keys));
		    System.out.println(Thread.currentThread().getId() + "] " + lr + " ---> " + Arrays.toString(lr.getSubNodes(true).values));
		}
		System.out.println(Thread.currentThread().getId() + "] " + "To Remove: " + localRoot + " ---> " + Arrays.toString(localRoot.getSubNodes(true).keys));
		System.out.println(Thread.currentThread().getId() + "] " + "To Remove: " + localRoot + " ---> " + Arrays.toString(localRoot.getSubNodes(true).values));
	    } finally {
		System.exit(-1);
	    }
	}
	setLocalRoots(localRootsKey, localRoots);
    }
    
    public static void updateLocalRoots(String localRootsUUID, InnerNode leftLocalRoot, InnerNode rightLocalRoot, InnerNode removeLocalRoot) {
	if (!BPlusTree.THREAD_MIGRATION) {
	    return;
	}
	int removeGroup = removeLocalRoot.group;
	int addGroup = leftLocalRoot.group;
	
	LocatedKey key = BPlusTree.wrapper.createGroupingKey(localRootsUUID + "-localRoots-" + removeGroup, removeGroup);
	List<InnerNode> localRoots = getLocalRoots(key);
	
	if (localRoots == null) {
	    System.out.println(Thread.currentThread().getId() + "] " + "Update: " + Arrays.toString(localRoots.toArray()) + " | remove " + removeLocalRoot + " | left " + leftLocalRoot + " | right " + rightLocalRoot + "\t" + key);
	    System.out.println(Thread.currentThread().getId() + "] " + "Should never try to remove a local root without a local root list: " + key);
	    System.exit(-1);
	}
	
	localRoots = new ArrayList<InnerNode>(localRoots);
	if (!localRoots.remove(removeLocalRoot)) {
	    try {
		System.out.println(Thread.currentThread().getId() + "] " + "Could not find node to remove in list: " + key + " " + removeLocalRoot);
		for (InnerNode lr : localRoots) {
		    System.out.println(Thread.currentThread().getId() + "] " + lr + " ---> " + Arrays.toString(lr.getSubNodes(true).keys));
		    System.out.println(Thread.currentThread().getId() + "] " + lr + " ---> " + Arrays.toString(lr.getSubNodes(true).values));
		}
		System.out.println(Thread.currentThread().getId() + "] " + "To Remove: " + removeLocalRoot + " ---> " + Arrays.toString(removeLocalRoot.getSubNodes(true).keys));
		System.out.println(Thread.currentThread().getId() + "] " + "To Remove: " + removeLocalRoot + " ---> " + Arrays.toString(removeLocalRoot.getSubNodes(true).values));
	    } finally {
		System.exit(-1);
	    }
	}
	
	if (removeGroup != addGroup) {
	    setLocalRoots(key, localRoots);
	    key = BPlusTree.wrapper.createGroupingKey(localRootsUUID + "-localRoots-" + addGroup, addGroup);
	    localRoots = getLocalRoots(key);
	    localRoots = new ArrayList<InnerNode>(localRoots);
	}
	
	localRoots.add(leftLocalRoot);
	localRoots.add(rightLocalRoot);
	setLocalRoots(key, localRoots);
    }
}
