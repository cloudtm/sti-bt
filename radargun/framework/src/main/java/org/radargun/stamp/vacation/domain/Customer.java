package org.radargun.stamp.vacation.domain;

import java.io.Serializable;
import java.util.Iterator;

import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.OpacityException;


public class Customer implements Serializable {

    /* final */ int id;
    /* final */ List_t<Reservation_Info> reservationInfoList;

    public Customer() { }
    
    public Customer(int id) {
	this.id = id;
	reservationInfoList = new List_t<Reservation_Info>("List:" + this.id + ":elements");
    }

    int customer_compare(Customer aPtr, Customer bPtr) {
	return (aPtr.id - bPtr.id);
    }

    boolean customer_addReservationInfo(int type, int id, int price) {
	Reservation_Info reservationInfo = new Reservation_Info(type, id, price);

	reservationInfoList.add(reservationInfo);
	return true;
    }

    boolean customer_removeReservationInfo(int type, int id) {
	Reservation_Info reservationInfo = reservationInfoList.find(type, id);

	if (reservationInfo == null) {
	    return false;
	}

	boolean status = reservationInfoList.remove(reservationInfo);
	if (!status) {
	    throw new OpacityException();
	}
	return true;
    }

    int customer_getBill() {
	int bill = 0;

	Iterator<Reservation_Info> iter = reservationInfoList.iterator();
	while (iter.hasNext()) {
	    Reservation_Info it = iter.next();
	    bill += it.price;
	}

	return bill;
    }
}
