package org.radargun.utils;

import java.util.NoSuchElementException;

/**
 * Created by IntelliJ IDEA.
 * Date: 3/4/12
 * Time: 1:49 PM
 *
 * @author Pedro Ruivo
 */
public class CacheSizeValues {
    private String statName;
    private int[] cacheSizesPerSlave;

    public CacheSizeValues(String statName, int numberOfSlaves) {
        this.statName = statName;
        this.cacheSizesPerSlave = new int[numberOfSlaves];
    }

    public String getStatName() {
        return statName;
    }

    public int getCacheSize(int slaveIndex) {
        checkIndex(slaveIndex);
        return cacheSizesPerSlave[slaveIndex];
    }

    public void setCacheSize(int slaveIndex, int cacheSize) {
        checkIndex(slaveIndex);
        cacheSizesPerSlave[slaveIndex] = cacheSize;
    }

    private void checkIndex(int idx) {
        if (idx < 0 || idx >= cacheSizesPerSlave.length) {
            throw new NoSuchElementException("Slave index not found [" + idx + "]. Possible indexes are [" + 0 + "," +
                    cacheSizesPerSlave.length + "]");
        }
    }
}