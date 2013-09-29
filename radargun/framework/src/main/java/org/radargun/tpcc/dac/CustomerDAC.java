package org.radargun.tpcc.dac;

import org.radargun.CacheWrapper;
import org.radargun.tpcc.TpccTools;
import org.radargun.tpcc.domain.Customer;
import org.radargun.tpcc.domain.CustomerLookup;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 */
public final class CustomerDAC {

	private CustomerDAC() {
	}

	public static List<Customer> loadByCLast(CacheWrapper cacheWrapper, long c_w_id, long c_d_id, String c_last) throws Throwable {

		List<Customer> result=new ArrayList<Customer>();

		CustomerLookup customerLookup = new CustomerLookup(c_last, c_w_id, c_d_id);

		customerLookup.load(cacheWrapper, ((int) c_w_id - 1));

		Customer current=null;
		boolean found = false;
		if(customerLookup.getIds() != null){

			Iterator<Long> itr = customerLookup.getIds().iterator();

			while(itr.hasNext()){

				long c_id = itr.next();

				current=new Customer();

				current.setC_id(c_id);
				current.setC_d_id(c_d_id);
				current.setC_w_id(c_w_id);

				found = current.load(cacheWrapper, ((int) c_w_id - 1));

				if(found){
					result.add(current);
				}

			}
		}

		return result;



	}

}
