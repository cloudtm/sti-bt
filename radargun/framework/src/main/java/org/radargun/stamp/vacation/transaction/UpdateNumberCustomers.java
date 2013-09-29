package org.radargun.stamp.vacation.transaction;

import java.util.Collection;
import java.util.Collections;

import org.radargun.CacheWrapper;
import org.radargun.IDelayedComputation;
import org.radargun.LocatedKey;
import org.radargun.stamp.vacation.Random;
import org.radargun.stamp.vacation.Vacation;
import org.radargun.stamp.vacation.domain.Manager;

public class UpdateNumberCustomers extends VacationTransaction {

    public UpdateNumberCustomers(Random random, int queryRange) {
    }

    @Override
    public void executeTransaction() throws Throwable {
//        super.WRAPPER = cacheWrapper;
//	Vacation.NODE_TARGET.set(super.node);
//	LocatedKey key = cacheWrapper.createKey("MANAGER" + super.node, super.node);
//	Manager manager = (Manager) cacheWrapper.get(null, key);
	
//    final LocatedKey key1 = manager.getNumberResourcesKey(WRAPPER);
//    // delay computation
//    cacheWrapper.delayComputation(new IDelayedComputation<Void>() {
//      public Collection getIAffectedKeys() {
//          return Collections.singleton(key1);
//      }
//
//      public Void computeI() {
//          Integer newValue = ((Integer) WRAPPER.getDelayed(key1)) + 1;
//          WRAPPER.putDelayed(key1, newValue);
//          return null;
//      }
//    });
//	
    }

    @Override
    public boolean isReadOnly() {
	return false;
    }

}
