package org.radargun.btt.colocated;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.radargun.LocatedKey;

public class LinkedList<U extends Serializable> implements Serializable {

    public static class Node implements Serializable {
	
	protected Comparable key;
	protected Serializable value;
	protected LocatedKey nextKey;

	public Node(int group, Comparable key, Serializable value, Node next) {
	    this.key = key;
	    this.value = value;
	    this.nextKey = BPlusTree.wrapper.createGroupingKey("nextList-" + UUID.randomUUID().toString(), group);
	    
	    if (next != null) {
		setNext(next);
	    }
	}

	public void setNext(Node next) {
	    BPlusTree.putCache(this.nextKey, next);
	}

	public Node getNext(boolean ghost) {
	    if (ghost) {
		return (Node) BPlusTree.cacheGetShadow(this.nextKey);
	    } else {
		return (Node) BPlusTree.getCache(this.nextKey);
	    }
	}
    }

    private Node headNode; // never changes, it's MIN_VALUE
    private LocatedKey keySize;

    public LinkedList(int group) {
	this.keySize = BPlusTree.wrapper.createGroupingKey("size-" + UUID.randomUUID().toString(), group);
	
	Node min = new Node(group, Long.MIN_VALUE, null, null);
	Node max = new Node(group, Long.MAX_VALUE, null, null);
	min.setNext(max);
	this.headNode = min;
	
	setSize(0);
	// changeSize(0);
    }
    
    public LinkedList(int newGroup, LinkedList<U> old) {
	this.keySize = BPlusTree.wrapper.createGroupingKey("size-" + UUID.randomUUID().toString(), newGroup);
	
	Node iter = old.headNode;
	this.headNode = new Node(newGroup, iter.key, iter.value, null);
	Node myLast = this.headNode;
	iter = iter.getNext(false);
	
	int size = 0;
	while (iter != null) {
	    size++;
	    Node newNode = new Node(newGroup, iter.key, iter.value, null);
	    myLast.setNext(newNode);
	    myLast = newNode;
	    iter = iter.getNext(false);
	}
	
	setSize(size - 1);
	// changeSize(size - 1);
    }
    
    public LinkedList(int group, List<Comparable> keys, List<Serializable> values, int start, int end) {
	this.keySize = BPlusTree.wrapper.createGroupingKey("size-" + UUID.randomUUID().toString(), group);
	
	this.headNode = new Node(group, Long.MIN_VALUE, null, null);
	Node iter = this.headNode;
	
	int k = 0;
	for (int i = start; i <= end; i++) {
	    k++;
	    Node newNode = new Node(group, keys.get(i), (U) values.get(i), null);
	    iter.setNext(newNode);
	    iter = newNode;
	}
	
	iter.setNext(new Node(group, Long.MAX_VALUE, null, null));
	
	setSize(k);
	// changeSize(k);
    }
    
    protected Integer getSize(boolean ghost) {
	if (ghost) {
	    return (Integer) BPlusTree.cacheGetShadow(this.keySize);
	} else {
	    return (Integer) BPlusTree.getCache(this.keySize);
	}
    }
    
    protected Integer getSizeWithRule() {
	// return (Integer) BPlusTree.CACHE.getWithRule(this.keySize, 0);
	return getSize(true);
    }

    protected void setSize(int value) {
	BPlusTree.putCache(this.keySize, value);
    }
    
    protected void changeSize(int count) {
	if (BPlusTree.INTRA_NODE_CONC && !BPlusTree.POPULATING) {
	    BPlusTree.wrapper.addDelayed(this.keySize, count);
	} else {
	    setSize(getSize(false) + count);
	}
    }
    
    public boolean insert(Comparable key, U value) {
	boolean result;

	Node previous = this.headNode;
	Node next = previous.getNext(true);
	Comparable currentKey;
	while ((currentKey = next.key).compareTo(key) < 0) {
	    previous = next;
	    next = next.getNext(true);
	}
	
	result = currentKey.compareTo(key) != 0;
	BPlusTree.registerGet(previous.nextKey);
	if (result) {
	    previous.setNext(new Node(this.keySize.getGroup(), key, value, next));
	    changeSize(1);
	}
	
	return result;
    }
    
    public boolean remove(Comparable key) {
	boolean result;

	Node previous = this.headNode;
	Node next = previous.getNext(true);
	Comparable currentKey;
	while ((currentKey = next.key).compareTo(key) < 0) {
	    previous = next;
	    next = next.getNext(true);
	}
	
	result = currentKey.compareTo(key) == 0;
	BPlusTree.registerGet(previous.nextKey);
	if (result) {
	    previous.setNext(next.getNext(false));
	    changeSize(-1);
	}

	return result;
    }
    
    public boolean contains(Comparable key) {
	boolean result;

	Node previous = this.headNode;
	Node next = previous.getNext(true);
	Comparable currentKey;
	while ((currentKey = next.key).compareTo(key) < 0) {
	    previous = next;
	    next = previous.getNext(true);
	}
	
	result = currentKey.compareTo(key) == 0;
	BPlusTree.registerGet(previous.nextKey);
	
	return result;
    }
    
    public U get(Comparable key) {
	U result = null;

	Node previous = this.headNode;
	Node next = previous.getNext(true);
	Comparable currentKey;
	while ((currentKey = next.key).compareTo(key) < 0) {
	    previous = next;
	    next = previous.getNext(true);
	}
	
	if (currentKey.compareTo(key) == 0) {
	    result = (U) next.value;
	}
	BPlusTree.registerGet(previous.nextKey);
	
	return result;
    }

    public void getAllElements(List<Comparable> keys, List<Serializable> values) {
	Node iter = this.headNode.getNext(false);
	while (iter.key.compareTo(Long.MAX_VALUE) < 0) {
	    keys.add(iter.key);
	    values.add(iter.value);
	    iter = iter.getNext(false);
	}
    }
    
    public boolean isEmpty() {
	return this.headNode.getNext(true).key.compareTo(Long.MAX_VALUE) == 0;
    }
    
    public Comparable getFirstKey() {
	Comparable c = this.headNode.getNext(false).key;
	if (c.compareTo(Long.MAX_VALUE) == 0) {
	    return null;
	}
	
	return c;
    }

    public LinkedList<Serializable> changeGroup(int newGroup) {
	LinkedList<Serializable> result = new LinkedList(newGroup, this);
	return result;
    }

    public void mergeWith(LinkedList entriesList) {
	List<Comparable> keys = new ArrayList<Comparable>();
	List<Serializable> values = new ArrayList<Serializable>();
	entriesList.getAllElements(keys, values);
	
	Node nextNode = this.headNode.getNext(false);
	
	for (int i = keys.size() - 1; i >= 0; i--) {
	    nextNode = new Node(this.keySize.getGroup(), keys.get(i), (U) values.get(i), nextNode);
	}
	
	this.headNode.setNext(nextNode);
	changeSize(keys.size());
    }

    public void clean() {
	Node iter = this.headNode;
	while (iter != null) {
	    Node next = iter.getNext(false);
	    iter.setNext(null);
	    iter = next;
	}
    }
    
}
