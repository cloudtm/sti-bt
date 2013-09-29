package org.radargun.stamp.vacation.transaction;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.stamp.vacation.Definitions;
import org.radargun.stamp.vacation.Random;
import org.radargun.stamp.vacation.Vacation;
import org.radargun.stamp.vacation.domain.Manager;

public class UpdateTablesOperation extends VacationTransaction {

    final private int[] types;
    final private int[] ids;
    final private int[] ops;
    final private int[] prices;
    final private int numUpdate;

    public UpdateTablesOperation(Random randomPtr, int numQueryPerTransaction, int queryRange, int relations) {
	this.types = new int[numQueryPerTransaction];
	this.ids = new int[numQueryPerTransaction];
	this.ops = new int[numQueryPerTransaction];
	this.prices = new int[numQueryPerTransaction];

	numUpdate = numQueryPerTransaction;
	int n;

	for (n = 0; n < numUpdate; n++) {
	    types[n] = randomPtr.posrandom_generate() % Definitions.NUM_RESERVATION_TYPE;
	    ids[n] = (randomPtr.posrandom_generate() % relations);
	    ops[n] = randomPtr.posrandom_generate() % 2;
	    if (ops[n]==1) {
	        prices[n] = ((randomPtr.posrandom_generate() % 5) * 10) + 50;
	    }
	}
    }

    @Override
    public void executeTransaction() throws Throwable {
//	Vacation.NODE_TARGET.set(super.node);
//	LocatedKey key = cacheWrapper.createKey("MANAGER" + super.node, super.node);
//	Manager managerPtr = (Manager) cacheWrapper.get(null, key);
//	int n;
//	for (n = 0; n < numUpdate; n++) {
//	    int t = types[n];
//	    int id = ids[n];
//	    int doAdd = ops[n];
//	    if (doAdd == 1) {
//		int newPrice = prices[n];
//		if (t == Definitions.RESERVATION_CAR) {
//		    managerPtr.manager_addCar(cacheWrapper, id, 100, newPrice);
//		} else if (t == Definitions.RESERVATION_FLIGHT) {
//		    managerPtr.manager_addFlight(cacheWrapper, id, 100, newPrice);
//		} else if (t == Definitions.RESERVATION_ROOM) {
//		    managerPtr.manager_addRoom(cacheWrapper, id, 100, newPrice);
//		} else {
//		    assert (false);
//		}
//	    } else { /* do delete */
//		if (t == Definitions.RESERVATION_CAR) {
//		    managerPtr.manager_deleteCar(cacheWrapper, id, 100);
//		} else if (t == Definitions.RESERVATION_FLIGHT) {
//		    managerPtr.manager_deleteFlight(cacheWrapper, id);
//		} else if (t == Definitions.RESERVATION_ROOM) {
//		    managerPtr.manager_deleteRoom(cacheWrapper, id, 100);
//		} else {
//		    assert (false);
//		}
//	    }
//	}
    }

    @Override
    public boolean isReadOnly() {
	return false;
    }

}
