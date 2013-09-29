package org.radargun.cachewrappers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;

public class CustomHashing implements ConsistentHash, Serializable {

    protected final Hash hashFunction;
    protected final int numOwners;
    protected final int numSegments;
    protected final List<Address> members;
    protected final List<Address>[] segmentOwners;
    protected final int segmentSize;

    public CustomHashing(Hash hashFunction, int numOwners, int numSegments, List<Address> members, List<Address>[] segmentOwners) {
	if (numSegments < 1)
	    throw new IllegalArgumentException("The number of segments must be strictly positive");
	if (numOwners < 1)
	    throw new IllegalArgumentException("The number of owners must be strictly positive");

	this.numOwners = numOwners;
	this.numSegments = numSegments;
	this.hashFunction = hashFunction;
	this.members = new ArrayList<Address>(members);
	this.segmentOwners = new List[numSegments];
	for (int i = 0; i < numSegments; i++) {
	    if (segmentOwners[i] == null || segmentOwners[i].isEmpty()) {
		throw new IllegalArgumentException("Segment owner list cannot be null or empty");
	    }
	    this.segmentOwners[i] = Immutables.immutableListCopy(segmentOwners[i]);
	}
	// this
	this.segmentSize = (int)Math.ceil((double)Integer.MAX_VALUE / numSegments);
    }

    @Override
    public Hash getHashFunction() {
	return hashFunction;
    }

    @Override
    public int getNumSegments() {
	return numSegments;
    }

    @Override
    public Set<Integer> getSegmentsForOwner(Address owner) {
	if (owner == null) {
	    throw new IllegalArgumentException("owner cannot be null");
	}
	if (!members.contains(owner)) {
	    throw new IllegalArgumentException("Node " + owner + " is not a member");
	}

	Set<Integer> segments = new HashSet<Integer>();
	for (int segment = 0; segment < segmentOwners.length; segment++) {
	    if (segmentOwners[segment].contains(owner)) {
		segments.add(segment);
	    }
	}
	return segments;
    }

    @Override
    public int getSegment(Object key) {
	// The result must always be positive, so we make sure the dividend is positive first
	return getNormalizedHash(key) / segmentSize;
    }

    public int getNormalizedHash(Object key) {
	return hashFunction.hash(key) & Integer.MAX_VALUE;
    }

    public List<Integer> getSegmentEndHashes() {
	List<Integer> hashes = new ArrayList<Integer>(numSegments);
	for (int i = 0; i < numSegments; i++) {
	    hashes.add(((i + 1) % numSegments) * segmentSize);
	}
	return hashes;
    }

    @Override
    public List<Address> locateOwnersForSegment(int segmentId) {
	return segmentOwners[segmentId];
    }

    @Override
    public Address locatePrimaryOwnerForSegment(int segmentId) {
	return segmentOwners[segmentId].get(0);
    }

    @Override
    public List<Address> getMembers() {
	return members;
    }

    @Override
    public int getNumOwners() {
	return numOwners;
    }

    @Override
    public Address locatePrimaryOwner(Object key) {
	String keyStr = (String) key;
	if (keyStr.contains(":::")) {
	    String trimmed = keyStr.split(":::")[0];
	    return locatePrimaryOwnerForSegment(getSegment(trimmed));    
	} else {
	    return locatePrimaryOwnerForSegment(getSegment(key));
	}
    }

    @Override
    public List<Address> locateOwners(Object key) {
	String keyStr = (String) key;
	if (keyStr.contains(":::")) {
	    String trimmed = keyStr.split(":::")[0];
	    int numOwners = Integer.parseInt(keyStr.split(":::")[1]);
	    
	    if (numOwners == members.size()) {
		return new ArrayList<Address>(members);
	    }
	    
	    int segment = getSegment(trimmed);

	    List<Address> result = new ArrayList<Address>(numOwners);
	    Set<Address> tmp = new HashSet<Address>(numOwners);
	    tmp.addAll(segmentOwners[segment]);

	    int difference = numOwners - tmp.size();

	    if (difference < 0) {
		Iterator<Address> it = tmp.iterator();
		for (int i = 0; i < numOwners; i++) {
		    result.add(it.next());
		}
	    } else {
		// get more
		result.addAll(segmentOwners[segment]);
		int i = segment % members.size();
		while (difference > 0) {
		    Address newOne = members.get(i);
		    i = (i + 1) % members.size();
		    if (tmp.contains(newOne)) {
			continue;
		    } else {
			tmp.add(newOne);
			result.add(newOne);
			difference--;
		    }
		}
	    }

	    return result;
	} else {
	    return locateOwnersForSegment(getSegment(key));
	}
    }

    @Override
    public Set<Address> locateAllOwners(Collection<Object> keys) {
	HashSet<Address> result = new HashSet<Address>();
	for (Object key : keys) {
	    result.addAll(this.locateOwners(key));
	}
	return result;
    }

    @Override
    public boolean isKeyLocalToNode(Address nodeAddress, Object key) {
	for (Address a : this.locateOwners(key)) {
	    if (a.equals(nodeAddress))
		return true;
	}
	return false;
    }

    @Override
    public int hashCode() {
	int result = hashFunction.hashCode();
	result = 31 * result + numOwners;
	result = 31 * result + numSegments;
	result = 31 * result + members.hashCode();
	return result;
    }

    @Override
    public boolean equals(Object o) {
	if (this == o) return true;
	if (o == null || getClass() != o.getClass()) return false;

	CustomHashing that = (CustomHashing) o;

	if (numOwners != that.numOwners) return false;
	if (numSegments != that.numSegments) return false;
	if (!hashFunction.equals(that.hashFunction)) return false;
	if (!members.equals(that.members)) return false;
	for (int i = 0; i < numSegments; i++) {
	    if (!segmentOwners[i].equals(that.segmentOwners[i]))
		return false;
	}

	return true;
    }

    @Override
    public String toString() {
	StringBuilder sb = new StringBuilder("CustomHashing{");
	sb.append("numSegments=").append(numSegments);
	sb.append(", numOwners=").append(numOwners);
	sb.append(", members=").append(members);
	sb.append('}');
	return sb.toString();
    }

    public String getRoutingTableAsString() {
	StringBuilder sb = new StringBuilder();
	for (int i = 0; i < numSegments; i++) {
	    if (i > 0) {
		sb.append(", ");
	    }
	    sb.append(i).append(":");
	    for (int j = 0; j < segmentOwners[i].size(); j++) {
		sb.append(' ').append(members.indexOf(segmentOwners[i].get(j)));
	    }
	}
	return sb.toString();
    }

    public CustomHashing union(CustomHashing dch2) {
	if (!hashFunction.equals(dch2.getHashFunction())) {
	    throw new IllegalArgumentException("The consistent hash objects must have the same hash function");
	}
	if (numSegments != dch2.getNumSegments()) {
	    throw new IllegalArgumentException("The consistent hash objects must have the same number of segments");
	}
	List<Address> unionMembers = new ArrayList<Address>(this.members);
	mergeLists(unionMembers, dch2.getMembers());

	List<Address>[] unionSegmentOwners = new List[numSegments];
	for (int i = 0; i < numSegments; i++) {
	    unionSegmentOwners[i] = new ArrayList<Address>(locateOwnersForSegment(i));
	    mergeLists(unionSegmentOwners[i], dch2.locateOwnersForSegment(i));
	}

	return new CustomHashing(hashFunction, dch2.numOwners, numSegments, unionMembers, unionSegmentOwners);
    }

    /**
     * Adds all elements from <code>src</code> list that do not already exist in <code>dest</code> list to the latter.
     *
     * @param dest List where elements are added
     * @param src List of elements to add - this is never modified
     */
    private void mergeLists(List<Address> dest, List<Address> src) {
	for (Address a2 : src) {
	    if (!dest.contains(a2)) {
		dest.add(a2);
	    }
	}
    }

    public static class Externalizer extends AbstractExternalizer<CustomHashing> {

	@Override
	public void writeObject(ObjectOutput output, CustomHashing ch) throws IOException {
	    output.writeInt(ch.numSegments);
	    output.writeInt(ch.numOwners);
	    output.writeObject(ch.members);
	    output.writeObject(ch.hashFunction);
	    output.writeObject(ch.segmentOwners);
	}

	@Override
	@SuppressWarnings("unchecked")
	public CustomHashing readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
	    int numSegments = unmarshaller.readInt();
	    int numOwners = unmarshaller.readInt();
	    List<Address> members = (List<Address>) unmarshaller.readObject();
	    Hash hash = (Hash) unmarshaller.readObject();
	    List<Address>[] owners = (List<Address>[]) unmarshaller.readObject();

	    return new CustomHashing(hash, numOwners, numSegments, members, owners);
	}

	@Override
	public Integer getId() {
	    return Ids.DEFAULT_CONSISTENT_HASH;
	}

	@Override
	public Set<Class<? extends CustomHashing>> getTypeClasses() {
	    return Collections.<Class<? extends CustomHashing>>singleton(CustomHashing.class);
	}
    }

}