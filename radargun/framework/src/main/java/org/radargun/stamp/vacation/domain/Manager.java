package org.radargun.stamp.vacation.domain;

/* =============================================================================
 *
 * manager.c
 * -- Travel reservation resource manager
 *
 * =============================================================================
 *
 * Copyright (C) Stanford University, 2006.  All Rights Reserved.
 * Author: Chi Cao Minh
 *
 * =============================================================================
 *
 * For the license of bayes/sort.h and bayes/sort.c, please see the header
 * of the files.
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of kmeans, please see kmeans/LICENSE.kmeans
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of ssca2, please see ssca2/COPYRIGHT
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of lib/mt19937ar.c and lib/mt19937ar.h, please see the
 * header of the files.
 * 
 * ------------------------------------------------------------------------
 * 
 * For the license of lib/rbtree.h and lib/rbtree.c, please see
 * lib/LEGALNOTICE.rbtree and lib/LICENSE.rbtree
 * 
 * ------------------------------------------------------------------------
 * 
 * Unless otherwise noted, the following license applies to STAMP files:
 * 
 * Copyright (c) 2007, Stanford University
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 * 
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 * 
 *     * Neither the name of Stanford University nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 *
 * =============================================================================
 */

/* =============================================================================
 * DECLARATION OF TM_CALLABLE FUNCTIONS
 * =============================================================================
 */

import java.io.Serializable;
import java.io.ObjectInputStream.GetField;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.btt.colocated.BPlusTree;
import org.radargun.stamp.vacation.Cons;
import org.radargun.stamp.vacation.Definitions;
import org.radargun.stamp.vacation.OpacityException;
import org.radargun.stamp.vacation.Vacation;
import org.radargun.stamp.vacation.VacationStressor;

public class Manager implements Serializable {
    public BPlusTree<Customer> customers;
    public BPlusTree<Reservation> reservations;
    
    public Manager(boolean threadMigration, boolean colocation, boolean ghostReads, boolean replicationDegrees) {
	customers = new BPlusTree<Customer>(Vacation.CACHE.getNumMembers(), threadMigration, colocation, ghostReads, replicationDegrees);
	reservations = new BPlusTree<Reservation>(Vacation.CACHE.getNumMembers(), threadMigration, colocation, ghostReads, replicationDegrees);
    }
    
    public void colocate() {
	System.out.println("Starting colocation customers!");
	while (customers.colocate()) {System.out.println("Successful colocation customers!");}
	System.out.println("Finished colocation customers!");
	try { Thread.sleep(2000); } catch (Exception e) {}
	
	System.out.println("Starting colocation reservations!");
	while (reservations.colocate()) {System.out.println("Successful colocation reservations!");}
	System.out.println("Finished colocation reservations!");
	try { Thread.sleep(2000); } catch (Exception e) {}
    }
    
    void putCustomer(int id, Customer val) {
	String key = "CUSTOMERS:" + id;
	customers.insert(key, val);
    }
    
    Customer getCustomer(int id) {
	String key = "CUSTOMERS:" + id;
	return customers.get(key);
    }
    
    void putReservation(String table, int id, Reservation val) {
	String key = table + ":" + id;
	reservations.insert(key, val);
    }
    
    Reservation getReservation(String table, int id) {
	String key = table + ":" + id;
	return reservations.get(key);
    }
    
    boolean addReservation(String type, int id, int num, int price) {
	Reservation reservation = getReservation(type, id);

	if (reservation == null) {
	    /* Create new reservation */
	    if (num < 1 || price < 0) {
		return false;
	    }
	    reservation = new Reservation(type, id, num, price);
	    putReservation(type, id, reservation);
	} else {
	    /* Update existing reservation */
	    if (!reservation.reservation_addToTotal(num)) {
		return false;
	    }
	    if (reservation.getNumTotal() == 0) {
	    } else {
		reservation.reservation_updatePrice(price);
	    }
	}

	return true;
    }

    public boolean manager_addCar(int carId, int numCars, int price) {
	return addReservation("car", carId, numCars, price);
    }

    public boolean manager_deleteCar(int carId, int numCar) {
	/* -1 keeps old price */
	return addReservation("car", carId, -numCar, -1);
    }

    public boolean manager_addRoom(int roomId, int numRoom, int price) {
	return addReservation("room", roomId, numRoom, price);
    }

    public boolean manager_deleteRoom(int roomId, int numRoom) {
	/* -1 keeps old price */
	return addReservation("room", roomId, -numRoom, -1);
    }

    public boolean manager_addFlight(int flightId, int numSeat, int price) {
	return addReservation("flight", flightId, numSeat, price);
    }

    public boolean manager_deleteFlight(int flightId) {
	Reservation reservation = getReservation("flight", flightId);
	if (reservation == null) {
	    return false;
	}

	if (reservation.getNumUsed() > 0) {
	    return false; /* somebody has a reservation */
	}

	return addReservation("flight", flightId, -reservation.getNumTotal(), -1);
    }

    public boolean manager_addCustomer(int customerId) {
	Customer customer = getCustomer(customerId);

	if (customer != null) {
	    return false;
	}

	customer = new Customer(customerId);
	putCustomer(customerId, customer);

	return true;
    }

    int queryNumFree(String table, int id) {
	int numFree = -1;
	Reservation reservation = getReservation(table, id);
	if (reservation != null) {
	    numFree = reservation.getNumFree();
	}

	return numFree;
    }

    int queryPrice(String table, int id) {
	int price = -1;
	Reservation reservation = getReservation(table, id);
	if (reservation != null) {
	    price = reservation.getPrice();
	}

	return price;
    }

    public int manager_queryCar(int carId) {
	return queryNumFree("car", carId);
    }

    public int manager_queryCarPrice(int carId) {
	return queryPrice("car", carId);
    }

    public int manager_queryRoom(int roomId) {
	return queryNumFree("room", roomId);
    }

    public int manager_queryRoomPrice(int roomId) {
	return queryPrice("room", roomId);
    }

    public int manager_queryFlight(int flightId) {
	return queryNumFree("flight", flightId);
    }

    public int manager_queryFlightPrice(int flightId) {
	return queryPrice("flight", flightId);
    }

    public int manager_queryCustomerBill(int customerId) {
	int bill = -1;
	Customer customer = getCustomer(customerId);

	if (customer != null) {
	    bill = customer.customer_getBill();
	}

	return bill;
    }

    boolean reserve(String table, int customerId, int id, int type) {
	Customer customer = getCustomer(customerId);
	Reservation reservation = getReservation(table, id);

	if (customer == null) {
	    return false;
	}

	if (reservation == null) {
	    return false;
	}

	if (!reservation.reservation_make()) {
	    return false;
	}

	if (!customer.customer_addReservationInfo(type, id, reservation.getPrice())) {
	    /* Undo previous successful reservation */
	    boolean status = reservation.reservation_cancel();
	    if (!status) {
		throw new OpacityException();
	    }
	    return false;
	}

	return true;
    }

    public boolean manager_reserveCar(int customerId, int carId) {
	return reserve("car", customerId, carId, Definitions.RESERVATION_CAR);
    }

    public boolean manager_reserveRoom(int customerId, int roomId) {
	return reserve("room", customerId, roomId, Definitions.RESERVATION_ROOM);
    }

    public boolean manager_reserveFlight(int customerId, int flightId) {
	return reserve("flight", customerId, flightId, Definitions.RESERVATION_FLIGHT);
    }

    boolean cancel(String table, int customerId, int id, int type) {
	Customer customer = getCustomer(customerId);
	Reservation reservation = getReservation(table, id);

	if (customer == null) {
	    return false;
	}

	if (reservation == null) {
	    return false;
	}

	if (!reservation.reservation_cancel()) {
	    return false;
	}

	if (!customer.customer_removeReservationInfo(type, id)) {
	    /* Undo previous successful cancellation */
	    boolean status = reservation.reservation_make();
	    if (!status) {
		throw new OpacityException();
	    }
	    return false;
	}

	return true;
    }

    boolean manager_cancelCar(int customerId, int carId) {
	return cancel("car", customerId, carId, Definitions.RESERVATION_CAR);
    }

    boolean manager_cancelRoom(int customerId, int roomId) {
	return cancel("room", customerId, roomId, Definitions.RESERVATION_ROOM);
    }

    boolean manager_cancelFlight(int customerId, int flightId) {
	return cancel("flight", customerId, flightId, Definitions.RESERVATION_FLIGHT);
    }
    
}
