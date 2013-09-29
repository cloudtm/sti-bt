package org.radargun.stamp.vacation.domain;

import java.io.Serializable;

/* =============================================================================
 * reservation_info_alloc
 * -- Returns NULL on failure
 * =============================================================================
 */
public class Reservation_Info implements Serializable {
    /* final */ int id;
    /* final */ int type;
    /* final */ int price;

    public Reservation_Info() { }
    
    public Reservation_Info(int type, int id, int price) {
	this.type = type;
	this.id = id;
	this.price = price;
    }

    /*
     * ==========================================================================
     * === reservation_info_compare -- Returns -1 if A < B, 0 if A = B, 1 if A >
     * B
     * ========================================================================
     * =====
     */
    public static int reservation_info_compare(Reservation_Info aPtr, Reservation_Info bPtr) {
	int typeDiff;

	typeDiff = aPtr.type - bPtr.type;

	return ((typeDiff != 0) ? (typeDiff) : (aPtr.id - bPtr.id));
    }

}
/*
 * =============================================================================
 * reservation_alloc -- Returns NULL on failure
 * =============================================================================
 */
