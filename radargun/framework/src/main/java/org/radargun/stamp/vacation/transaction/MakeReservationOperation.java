package org.radargun.stamp.vacation.transaction;

import org.radargun.LocatedKey;
import org.radargun.stamp.vacation.Definitions;
import org.radargun.stamp.vacation.Random;
import org.radargun.stamp.vacation.Vacation;
import org.radargun.stamp.vacation.domain.Manager;

public class MakeReservationOperation extends VacationTransaction {

    final private int[] types;
    final private int[] ids;
    final private int[] maxPrices;
    final private int[] maxIds;
    final private int customerId;
    final private int numQuery;
    final private boolean readOnly;

    public MakeReservationOperation(Random random, int numQueryPerTx, int queryRange, int relations, int readOnly) {
	this.types = new int[numQueryPerTx];
	this.ids = new int[numQueryPerTx];

	this.maxPrices = new int[Definitions.NUM_RESERVATION_TYPE];
	this.maxIds = new int[Definitions.NUM_RESERVATION_TYPE];
	this.maxPrices[0] = -1;
	this.maxPrices[1] = -1;
	this.maxPrices[2] = -1;
	this.maxIds[0] = -1;
	this.maxIds[1] = -1;
	this.maxIds[2] = -1;
	int n;
	this.numQuery = numQueryPerTx;

	customerId = random.posrandom_generate() % relations;
	for (n = 0; n < numQuery; n++) {
            types[n] = random.random_generate() % Definitions.NUM_RESERVATION_TYPE;
            ids[n] = random.random_generate() % relations;
        }

	this.readOnly = (random.random_generate() % 100) <= readOnly;
        
    }

    @Override
    public void executeTransaction() throws Throwable {
	LocatedKey key = Vacation.CACHE.createGroupingKeyWithRepl("MANAGER", 0, Vacation.CACHE.getNumMembers());
	Manager manager = (Manager) Vacation.CACHE.get(key);
	boolean isFound = false;
	int n;
	for (n = 0; n < numQuery; n++) {
	    int t = types[n];
	    int id = ids[n];
	    int price = -1;
	    if (t == Definitions.RESERVATION_CAR) {
		if (manager.manager_queryCar(id) >= 0) {
		    price = manager.manager_queryCarPrice(id);
		}
	    } else if (t == Definitions.RESERVATION_FLIGHT) {
		if (manager.manager_queryFlight(id) >= 0) {
		    price = manager.manager_queryFlightPrice(id);
		}
	    } else if (t == Definitions.RESERVATION_ROOM) {
		if (manager.manager_queryRoom(id) >= 0) {
		    price = manager.manager_queryRoomPrice(id);
		}
	    } else {
		assert (false);
	    }
	    if (price > maxPrices[t]) {
		maxPrices[t] = price;
		maxIds[t] = id;
		isFound = true;
	    }
	}

	if (!readOnly) {
	    if (isFound) {
		manager.manager_addCustomer(customerId);
	    }
	    if (maxIds[Definitions.RESERVATION_CAR] > 0) {
		manager.manager_reserveCar(customerId, maxIds[Definitions.RESERVATION_CAR]);
	    }
	    if (maxIds[Definitions.RESERVATION_FLIGHT] > 0) {
		manager.manager_reserveFlight(customerId, maxIds[Definitions.RESERVATION_FLIGHT]);
	    }
	    if (maxIds[Definitions.RESERVATION_ROOM] > 0) {
		manager.manager_reserveRoom(customerId, maxIds[Definitions.RESERVATION_ROOM]);
	    }
	}
    }

    @Override
    public boolean isReadOnly() {
	return this.readOnly;
    }

}
