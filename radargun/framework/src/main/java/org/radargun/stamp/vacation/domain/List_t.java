package org.radargun.stamp.vacation.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.stamp.vacation.Cons;
import org.radargun.stamp.vacation.Vacation;

public class List_t<E> implements Serializable{

    protected /* final */ String cacheKey;
    protected int group;
    
    public transient final Random r = new Random();

    public List_t() { }
    
    public List_t(String strKey) {
	this.group = Math.abs(r.nextInt());
	
	this.cacheKey = UUID.randomUUID().toString() + ":" + strKey;
	LocatedKey key = Vacation.CACHE.createGroupingKey(this.cacheKey, group);
	Vacation.put(key, (Cons<E>) Cons.empty());
    }
    
    private void putElements(Cons<E> elems) {
	LocatedKey key = Vacation.CACHE.createGroupingKey(this.cacheKey, group);
	Vacation.put(key, elems);
    }
    
    private Cons<E> getElements() {
	LocatedKey key = Vacation.CACHE.createGroupingKey(this.cacheKey, group);
	return ((Cons<E>) Vacation.get(key));
    }

    public void add(E element) {
	putElements(getElements().cons(element));
    }

    public E find(int type, int id) {
	for (E iter : getElements()) {
	    if (iter instanceof Reservation_Info) {
		Reservation_Info resIter = (Reservation_Info) iter;
		if (resIter.type == type && resIter.id == id) {
		    return iter;
		}
	    } else {
		assert (false);
	    }
	}
	return null;
    }

    public boolean remove(E element) {
	Cons<E> oldElems = getElements();
	Cons<E> newElems = oldElems.removeFirst(element);

	if (oldElems == newElems) {
	    return false;
	} else {
	    putElements(newElems);
	    return true;
	}
    }

    public Iterator<E> iterator() {
	List<E> snapshot = new ArrayList<E>();
	for (E element : getElements())
	    snapshot.add(element);
	Collections.reverse(snapshot);
	return snapshot.iterator();
    }
}
