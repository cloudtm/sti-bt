package org.radargun.stamp.vacation.transaction;

import java.util.Random;

import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.VacationStressor;

public abstract class VacationTransaction {

    protected static CacheWrapper WRAPPER;
    
    static Random r = new Random();
    
    public VacationTransaction() {
    }
    
    public abstract void executeTransaction() throws Throwable;
    
    public abstract boolean isReadOnly();
}
