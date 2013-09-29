package org.radargun.btt.colocated;

import java.util.ArrayList;
import java.util.List;

import org.radargun.LocatedKey;

public class ColocationThread extends Thread {

    private final BPlusTree tree;
    private long sleepTime;
    private final int[] groups;
    private final LocatedKey[] keys;
    
    public ColocationThread(BPlusTree tree) {
	this.setDaemon(true);
	this.tree = tree;
	this.sleepTime = 2000;
	int clusterSize = BPlusTree.wrapper.getNumMembers();
	this.groups = new int[clusterSize];
	this.keys = new LocatedKey[clusterSize];
	
	int k = 0;
	for (Integer group : BPlusTree.wrapper.getGroups().values()) {
	    this.groups[k] = group;
	    this.keys[k] = BPlusTree.wrapper.createGroupingKey(tree.localRootsUUID + "-localRoots-" + this.groups[k], this.groups[k]);
	    k++;
	}
	
    }
    
    @Override
    public void run() {
	while (true) {
	    colocate();
	}
    }
    
    public boolean colocate() {
	List<InnerNode>[] allRoots = new List[this.keys.length];
	sleep();

	boolean successful = false;
	while (!successful) {
	    try {
		BPlusTree.wrapper.startTransaction(false);
		int i = 0;
		int s = 0;
		List<InnerNode> source = null;
		for (LocatedKey lrKey : this.keys) {
		    allRoots[i] = BPlusTree.getLocalRoots(lrKey);
		    if (allRoots[i] != null && allRoots[i].size() > 0) {
			source = allRoots[i];
			s = i;
		    }
		    i++;
		}
		
		int members = allRoots.length;
		int localRoots = source.size();
		int division = (int) Math.floor((localRoots + 0.0) / (members + 0.0));
		if (division == 0) {
		    BPlusTree.wrapper.endTransaction(true);
		    return false;
		}
		
		int groupFrom = s;
		int groupTo = -1;
		InnerNode toMove = null;
		source = new ArrayList<InnerNode>(source);
		
		for (int k = 0; k < allRoots.length; k++) {
		    if (k == s) {
			continue;
		    } 
		    
		    List<InnerNode> otherRoots = allRoots[k];
		    if (otherRoots == null) {
			otherRoots = new ArrayList<InnerNode>();
		    } else {
			otherRoots = new ArrayList<InnerNode>(otherRoots);
		    }
		    
		    groupTo = k;
		    for (int l = 0; l < division; l++) {
			toMove = source.remove(source.size() - 1);
			toMove = toMove.startChangeGroup(this.keys[k].getGroup());
			otherRoots.add(toMove);
			
			BPlusTree.wrapper.endTransaction(true);
			System.out.println("Async ColocationThread moved Root " + toMove + " from " + this.keys[groupFrom].getGroup() + " to " + this.keys[groupTo].getGroup());
			BPlusTree.wrapper.startTransaction(false);
		    }
		    BPlusTree.setLocalRoots(this.keys[k], otherRoots);
		}
		BPlusTree.setLocalRoots(this.keys[s], source);

		BPlusTree.wrapper.endTransaction(true);
		successful = true;
	    } catch (Exception e) {
		e.printStackTrace();
		System.out.println("Colocation should not have exceptions");
		System.exit(-1);
//		try { BPlusTree.wrapper.endTransaction(false); } 
//		catch (Exception e2) {
//
//		}
//		sleep();
	    }
	}
	return false;
    }
    
//    public boolean colocate() {
//	List<InnerNode>[] allRoots = new List[this.keys.length];
////	while (true) {
//	    sleep();
//	    
//	    boolean successful = false;
//	    while (!successful) {
//		try {
//		    BPlusTree.wrapper.startTransaction(false);
//		    int i = 0;
//		    for (LocatedKey lrKey : this.keys) {
//			allRoots[i] = BPlusTree.getLocalRoots(lrKey);
//			i++;
//		    }
//
//		    int groupFrom = -1;
//		    int groupTo = -1;
//		    InnerNode toMove = null;
//
//		    boolean migrated = false;
//		    for (i = 0; i < allRoots.length; i++) {
//			List<InnerNode> roots = allRoots[i];
//			if (roots == null) continue;
//			if (roots.size() > 1) {
//			    groupFrom = i;
//			    for (int k = 0; k < allRoots.length; k++) {
//				List<InnerNode> otherRoots = allRoots[k];
//				if (otherRoots == null) {
//				    otherRoots = new ArrayList<InnerNode>();
//				}
//				if (k != i && otherRoots.size() == 0) {
//				    groupTo = k;
//				    roots = new ArrayList<InnerNode>(roots);
//				    toMove = roots.remove(roots.size() - 1);
//				    
//				    toMove = toMove.startChangeGroup(this.keys[k].getGroup());
//				    otherRoots = new ArrayList<InnerNode>(otherRoots);
//				    otherRoots.add(toMove);
//				    BPlusTree.setLocalRoots(this.keys[i], roots);
//				    BPlusTree.setLocalRoots(this.keys[k], otherRoots);
//				    migrated = true;
//				    break;
//				}
//			    }
//			    if (migrated) {
//				break;
//			    }
//			}
//		    }
//
//		    BPlusTree.wrapper.endTransaction(true);
//		    successful = true;
//		    if (groupFrom != - 1 && groupTo != -1) {
//			System.out.println("Async ColocationThread moved Root " + toMove + " from " + this.keys[groupFrom].getGroup() + " to " + this.keys[groupTo].getGroup());
//			return true;
//		    }
//		} catch (Exception e) {
//		    e.printStackTrace();
//		    try { BPlusTree.wrapper.endTransaction(false); } 
//		    catch (Exception e2) {
//
//		    }
//		    sleep();
//		}
//	    }
////	}
//	    return false;
//    }
    
    private void sleep() {
	try {
	    Thread.sleep(this.sleepTime);
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}	
    }
    
}
