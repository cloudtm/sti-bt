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
package org.infinispan.dataplacement.stats;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.EnumMap;

import static java.lang.System.currentTimeMillis;

/**
 * Keeps all the round stats values.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class Stats {

   private static final Log log = LogFactory.getLog(Stats.class);
   private final long roundId;
   private final IncrementableLong[] durations;
   private final EnumMap<Counter, IncrementalInteger> counters;
   private final EnumMap<TimeStamp, Long> timestamps;
   private final EnumMap<Size, Integer> messageSizes;
   private final EnumMap<Duration, Long> duration;

   public Stats(long roundId, int size) {
      this.roundId = roundId;
      counters = new EnumMap<Counter, IncrementalInteger>(Counter.class);

      for (Counter counter : Counter.values()) {
         counters.put(counter, new IncrementalInteger());
      }

      timestamps = new EnumMap<TimeStamp, Long>(TimeStamp.class);
      timestamps.put(TimeStamp.START, currentTimeMillis());

      messageSizes = new EnumMap<Size, Integer>(Size.class);
      durations = new IncrementableLong[size];

      for (int i = 0; i < size; ++i) {
         durations[i] = new IncrementableLong();
      }

      duration = new EnumMap<Duration, Long>(Duration.class);
   }

   public final IncrementableLong[] createQueryPhaseDurationsArray() {
      IncrementableLong[] array = new IncrementableLong[durations.length];
      for (int i = 0; i < array.length; ++i) {
         array[i] = new IncrementableLong();
      }
      return array;
   }

   public final void collectedAccesses() {
      timestamps.put(TimeStamp.ACCESSES, currentTimeMillis());
   }

   public final void receivedAccesses() {
      timestamps.put(TimeStamp.RECEIVED_ACCESSES, currentTimeMillis());
   }

   public final void calculatedNewOwners() {
      timestamps.put(TimeStamp.NEW_OWNERS, currentTimeMillis());
   }

   public final void receivedObjectLookup() {
      timestamps.put(TimeStamp.RECEIVED_OBJECT_LOOKUP, currentTimeMillis());
   }

   public final void receivedAcks() {
      timestamps.put(TimeStamp.ACKS, currentTimeMillis());
   }

   public final void startStateTransfer() {
      timestamps.put(TimeStamp.STATE_TRANSFER_START, currentTimeMillis());
   }

   public final void endStateTransfer() {
      timestamps.put(TimeStamp.STATE_TRANSFER_END, currentTimeMillis());
   }

   public final void wrongOwnersErrors(int value) {
      counters.get(Counter.WRONG_OWNER).increment(value);
   }

   public final void wrongKeyMovedErrors(int value) {
      counters.get(Counter.WRONG_MOVE).increment(value);
   }

   public final void totalKeysMoved(int value) {
      counters.get(Counter.TOTAL_KEYS_TO_MOVE).increment(value);
   }

   public final void accessesSize(int value) {
      messageSizes.put(Size.ACCESSES, value);
   }

   public final void objectLookupSize(int value) {
      messageSizes.put(Size.OBJECT_LOOKUP, value);
   }

   public final void setBloomFilterSize(int value) {
      messageSizes.put(Size.BLOOM_FILTER, value);
   }

   public final void setMachineLearner1(int value) {
      messageSizes.put(Size.MACHINE_LEARNER_1, value);
   }

   public final void setMachineLearner2(int value) {
      messageSizes.put(Size.MACHINE_LEARNER_2, value);
   }

   public final void queryDuration(IncrementableLong[] values) {
      int size = Math.min(values.length, durations.length);

      for (int i = 0; i < size; ++i) {
         durations[i].add(values[i]);
      }
   }

   public final void setObjectLookupCreationDuration(long duration) {
      this.duration.put(Duration.OBJECT_LOOKUP_CREATION, duration);
   }

   public final void saveTo(BufferedWriter writer, boolean printHeader) {
      if (log.isTraceEnabled()) {
         log.tracef("Saving statistics to %s. print headers? %s", writer, printHeader);
      }
      try {
         if (printHeader) {
            writer.write("RoundId");
            for (TimeStamp timeStamp : TimeStamp.values()) {
               writer.write(",");
               writer.write(timeStamp.toString());
            }
            for (Counter counter : Counter.values()) {
               writer.write(",");
               writer.write(counter.toString());
            }
            for (Size size : Size.values()) {
               writer.write(",");
               writer.write(size.toString());
            }
            for (Duration duration : Duration.values()) {
               writer.write(",");
               writer.write(duration.toString());
            }
            for (int i = 0; i < durations.length; ++i) {
               writer.write(",");
               writer.write("QUERY_DURATION_PHASE_" + i);
            }
            writer.newLine();
         }

         writer.write(Long.toString(roundId));
         write(writer, timestamps, TimeStamp.values());
         write(writer, counters, Counter.values());
         write(writer, messageSizes, Size.values());
         write(writer, duration, Duration.values());
         for (IncrementableLong duration : durations) {
            writer.write(",");
            writer.write(duration == null ? "N/A" : duration.toString());
         }
         writer.newLine();
         writer.flush();
      } catch (Exception e) {
         log.errorf(e, "Error saving stats %s.", this);
      }
   }

   @Override
   public String toString() {
      return "Stats{" +
            "roundId=" + roundId +
            ", counters=" + counters +
            ", timestamps=" + timestamps +
            '}';
   }

   private void write(BufferedWriter writer, EnumMap map, Enum[] values) throws IOException {
      for (Enum e : values) {
         writer.write(",");
         Object value = map.get(e);
         writer.write(value == null ? "N/A" : value.toString());
      }
   }

   private static enum TimeStamp {
      /**
       * the start timestamp
       */
      START,
      /**
       * the timestamp when it finishes to calculate the local and remote accesses
       */
      ACCESSES,
      /**
       * the timestamp when it received all the object requests and start to calculate the new owners
       */
      RECEIVED_ACCESSES,
      /**
       * the timestamp when it finishes to calculate the new owners
       */
      NEW_OWNERS,
      /**
       * the timestamp when all objects lookup are received
       */
      RECEIVED_OBJECT_LOOKUP,
      /**
       * the timestamp when the acks are received
       */
      ACKS,
      /**
       * the timestamp when the state transfer starts
       */
      STATE_TRANSFER_START,
      /**
       * the timestamp when the state transfer ends
       */
      STATE_TRANSFER_END
   }

   private static enum Counter {
      /**
       * the number of keys that are moved to a wrong node
       */
      WRONG_OWNER,
      /**
       * the number of keys that are moved but they aren't supposed to be moved
       */
      WRONG_MOVE,
      /**
       * the total number of keys to move
       */
      TOTAL_KEYS_TO_MOVE
   }

   private static enum Size {
      /**
       * the sum of size of the object requests
       */
      ACCESSES,
      /**
       * the size of the object lookup
       */
      OBJECT_LOOKUP,
      /**
       * bloom filter size
       */
      BLOOM_FILTER,
      MACHINE_LEARNER_1,
      MACHINE_LEARNER_2
   }

   private static enum Duration {
      /**
       * time (in nanoseconds) to create the object lookup
       */
      OBJECT_LOOKUP_CREATION
   }

   private class IncrementalInteger {
      private int value;

      public final void increment() {
         increment(1);
      }

      public final void increment(int value) {
         this.value += value;
      }

      @Override
      public String toString() {
         return Integer.toString(value);
      }
   }
}
