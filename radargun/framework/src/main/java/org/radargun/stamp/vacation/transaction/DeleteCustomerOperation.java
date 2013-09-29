package org.radargun.stamp.vacation.transaction;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.stamp.vacation.Random;
import org.radargun.stamp.vacation.Vacation;
import org.radargun.stamp.vacation.domain.Manager;

public class DeleteCustomerOperation extends VacationTransaction {

    final private int customerId;
    final private boolean readOnly;

    public DeleteCustomerOperation(Random randomPtr, int queryRange, int relations, int readOnlyPerc) {
	this.customerId = randomPtr.posrandom_generate() % relations;
	this.readOnly = (randomPtr.random_generate() % 100) <= readOnlyPerc;
    }

    @Override
    public void executeTransaction() throws Throwable {
//	Vacation.NODE_TARGET.set(super.node);
//	LocatedKey key = cache.createKey("MANAGER" + super.node, super.node);
//	Manager managerPtr = (Manager) cache.get(null, key);
//	int bill = managerPtr.manager_queryCustomerBill(cache, customerId);
//	if (bill >= 0) {
//	    managerPtr.manager_deleteCustomer(cache, customerId);
//	}
    }

    @Override
    public boolean isReadOnly() {
	return false;
    }
    
}
