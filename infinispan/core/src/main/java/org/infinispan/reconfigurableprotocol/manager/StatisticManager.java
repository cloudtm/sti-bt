/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.reconfigurableprotocol.manager;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistics collection about the switch, namely duration between phases
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StatisticManager {

   private static final long NO_STAT = -1;

   private final Map<String, Map<String, StatEntry>> switchStats;

   public StatisticManager() {
      switchStats = new HashMap<String, Map<String, StatEntry>>();
   }

   public final void add(Stats stats) {
      synchronized (switchStats) {
         if (!switchStats.containsKey(stats.from)) {
            createNonExistingNewEntry(stats);
         } else if (!switchStats.get(stats.from).containsKey(stats.to)) {
            createNewEntry(stats);
         } else {
            updateEntry(stats);
         }
      }
   }

   public final double getSafeToSafe(String from, String to) {
      synchronized (switchStats) {
         StatEntry entry = getEntry(from, to);
         return entry == null ? NO_STAT : convertNanosToMilli(entry.getSafeToSafe());
      }
   }

   public final double getSafeToUnsafe(String from, String to) {
      synchronized (switchStats) {
         StatEntry entry = getEntry(from, to);
         return entry == null ? NO_STAT : convertNanosToMilli(entry.getSafeToUnsafe());
      }
   }

   public final double getUnsafeToSafe(String from, String to) {
      synchronized (switchStats) {
         StatEntry entry = getEntry(from, to);
         return entry == null ? NO_STAT : convertNanosToMilli(entry.getUnsafeToSafe());
      }
   }

   public final int getSwitchCounter(String from, String to) {
      synchronized (switchStats) {
         StatEntry entry = getEntry(from, to);
         return entry == null ? (int)NO_STAT : entry.switchCount;
      }
   }

   public final int getNumberOfAbortedTransactions(String from, String to) {
      synchronized (switchStats) {
         StatEntry entry = getEntry(from, to);
         return entry == null ? (int)NO_STAT : entry.numberOfTransactionAborted;
      }
   }

   public final String printAllStats() {
      StringBuilder sb = new StringBuilder();
      synchronized (switchStats) {
         for (Map.Entry<String, Map<String, StatEntry>> outside : switchStats.entrySet()) {
            String from = outside.getKey();
            for (Map.Entry<String, StatEntry> inside : outside.getValue().entrySet()) {
               String to = inside.getKey();
               StatEntry entry = inside.getValue();
               sb.append(from).append("=>").append(to).append(": ");
               sb.append("SafeToSafe=").append(convertNanosToMilli(entry.getSafeToSafe()));
               sb.append(",SafeToUnsafe=").append(convertNanosToMilli(entry.getSafeToUnsafe()));
               sb.append(",UnsafeToSafe=").append(convertNanosToMilli(entry.getUnsafeToSafe()));
               sb.append(",Counter=").append(entry.switchCount);
               sb.append(",NumberOfTransactionAborted=").append(entry.numberOfTransactionAborted);
               sb.append("\n");
            }
         }
      }
      return sb.toString();
   }

   public final void reset() {
      synchronized (switchStats) {
         switchStats.clear();
      }
   }

   public final Stats createNewStats(String from) {
      return new Stats(from, this);
   }

   private void createNonExistingNewEntry(Stats stats) {
      Map<String, StatEntry> newEntry = new HashMap<String, StatEntry>();
      StatEntry statEntry = new StatEntry();
      statEntry.add(stats.start, stats.unsafe, stats.safe, stats.numberOfTransactionAborted);
      newEntry.put(stats.to, statEntry);
      switchStats.put(stats.from, newEntry);
   }

   private void createNewEntry(Stats stats) {
      StatEntry statEntry = new StatEntry();
      statEntry.add(stats.start, stats.unsafe, stats.safe, stats.numberOfTransactionAborted);
      switchStats.get(stats.from).put(stats.to, statEntry);
   }

   private void updateEntry(Stats stats) {
      StatEntry entry = switchStats.get(stats.from).get(stats.to);
      entry.add(stats.start, stats.unsafe, stats.safe, stats.numberOfTransactionAborted);
   }

   private StatEntry getEntry(String from, String to) {
      Map<String, StatEntry> line = switchStats.get(from);
      return line == null ? null : line.get(to);
   }

   private double convertNanosToMilli(double nanos) {
      if (nanos == NO_STAT) {
         return nanos;
      }
      return nanos / 1000000.0;
   }

   private class StatEntry {
      private long safeToSafe;
      private long safeToUnsafe;
      private long unsafeToSafe;

      private int safeToSafeCounter;
      private int safeToUnsafeCounter;
      private int unsafeToSafeCounter;

      private int switchCount;
      private int numberOfTransactionAborted;

      public void add(long start, long unsafe, long safe, int numberOfTransactionAborted) {
         if (unsafe != NO_STAT) {
            safeToSafe += (safe - start);
            safeToUnsafe += (unsafe - start);
            unsafeToSafe += (safe - unsafe);
            safeToSafeCounter++;
            safeToUnsafeCounter++;
            unsafeToSafeCounter++;
         } else {
            safeToSafe += (safe - start);
            safeToSafeCounter++;
         }
         switchCount++;
         this.numberOfTransactionAborted += numberOfTransactionAborted;
      }

      public double getSafeToSafe() {
         if (safeToSafeCounter == 0) {
            return NO_STAT;
         }
         return safeToSafe * 1.0 / safeToSafeCounter;
      }

      public double getSafeToUnsafe() {
         if (safeToUnsafeCounter == 0) {
            return NO_STAT;
         }
         return safeToUnsafe * 1.0 / safeToUnsafeCounter;
      }

      public double getUnsafeToSafe() {
         if (unsafeToSafeCounter == 0) {
            return NO_STAT;
         }
         return unsafeToSafe * 1.0 / unsafeToSafeCounter;
      }

      public int getNumberOfTransactionAborted() {
         return numberOfTransactionAborted;
      }
   }

   public static class Stats {
      private final long start;
      private final String from;
      private final StatisticManager manager;

      private long unsafe = NO_STAT;
      private long safe = NO_STAT;
      private String to;
      private int numberOfTransactionAborted;

      private Stats(String from, StatisticManager manager) {
         this.start = System.nanoTime();
         this.from = from;
         this.manager = manager;
      }

      public final void unsafe(String to) {
         if (to != null) {
            this.to = to;
         }
         unsafe = System.nanoTime();
      }

      public final void safe(String to) {
         if (to != null) {
            this.to = to;
         }
         safe = System.nanoTime();
         manager.add(this);
      }
      
      public final void addNumberOfTransactionAborted(int val) {
         numberOfTransactionAborted += val;
      }
   }

}
