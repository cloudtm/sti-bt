package org.infinispan.stats.container;

import org.infinispan.stats.ExposedStatistic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.infinispan.stats.ExposedStatistic.*;

/**
 * Thread safe cache statistics that allows multiple writers and reader at the same time.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public final class ConcurrentGlobalContainer {

   private static final int LOCAL_STATS_OFFSET = 1;
   private static final int REMOTE_STATS_OFFSET = LOCAL_STATS_OFFSET + getLocalStatsSize();
   private static final int LOCAL_SIZE = getLocalStatsSize();
   private static final int REMOTE_SIZE = getRemoteStatsSize();
   private static final int TOTAL_SIZE = 1 + LOCAL_SIZE + REMOTE_SIZE;
   private final AtomicBoolean flushing;
   private final BlockingQueue<Mergeable> queue;
   private volatile long[] values;
   private volatile boolean reset;

   public ConcurrentGlobalContainer() {
      flushing = new AtomicBoolean(false);
      queue = new LinkedBlockingQueue<Mergeable>();
      values = create();
      values[0] = System.nanoTime();
   }

   public final void add(ExposedStatistic stat, long value, boolean local) {
      queue.add(new SingleOperation(local ? getLocalIndex(stat) : getRemoteIndex(stat), value));
      tryFlush();
   }

   public final void merge(long[] toMerge, boolean local) {
      final int expectedSize = local ? LOCAL_SIZE : REMOTE_SIZE;
      final int offset = local ? LOCAL_STATS_OFFSET : REMOTE_STATS_OFFSET;

      if (toMerge.length != expectedSize) {
         throw new IllegalArgumentException("Size mismatch to merge transaction statistic");
      }

      queue.add(new Transaction(toMerge, offset));
      tryFlush();
   }

   public final StatisticsSnapshot getSnapshot() {
      tryFlush();
      return new StatisticsSnapshot(values);
   }

   public final void reset() {
      reset = true;
      tryFlush();
   }

   public static int getLocalIndex(ExposedStatistic stat) {
      final int index = stat.getLocalIndex();
      if (index == NO_INDEX) {
         throw new IllegalArgumentException("This should never happen. Statistic " + stat + " is not local");
      }
      return LOCAL_STATS_OFFSET + index;
   }

   public static int getRemoteIndex(ExposedStatistic stat) {
      final int index = stat.getRemoteIndex();
      if (index == NO_INDEX) {
         throw new IllegalArgumentException("This should never happen. Statistic " + stat + " is not remote");
      }
      return REMOTE_STATS_OFFSET + index;
   }

   /**
    * @return TEST ONLY!!
    */
   public final BlockingQueue<?> queue() {
      return queue;
   }

   /**
    * @return TEST ONLY!!
    */
   public final AtomicBoolean flushing() {
      return flushing;
   }

   /**
    * @return TEST ONLY!!
    */
   public final boolean isReset() {
      return reset;
   }

   private void tryFlush() {
      if (flushing.compareAndSet(false, true)) {
         flush();
      }
   }

   private void flush() {
      if (reset) {
         values = create();
         queue.clear();
         reset = false;
         values[0] = System.nanoTime();
         flushing.set(false);
         return;
      }
      final long[] copy = create();
      System.arraycopy(values, 0, copy, 0, copy.length);
      List<Mergeable> drain = new ArrayList<Mergeable>();
      queue.drainTo(drain);
      for (Mergeable mergeable : drain) {
         try {
            mergeable.mergeTo(copy);
         } catch (Throwable throwable) {
            //ignore
         }
      }
      values = copy;
      flushing.set(false);
   }

   private long[] create() {
      return new long[TOTAL_SIZE];
   }

   private interface Mergeable {
      void mergeTo(long[] values);
   }

   private class Transaction implements Mergeable {

      private final long[] toMerge;
      private final int offset;

      private Transaction(long[] toMerge, int offset) {
         this.toMerge = toMerge;
         this.offset = offset;
      }

      @Override
      public void mergeTo(long[] values) {
         for (int i = 0; i < values.length; ++i) {
            values[offset + i] += toMerge[i];
         }
      }
   }

   private class SingleOperation implements Mergeable {

      private final int index;
      private final long value;

      private SingleOperation(int index, long value) {
         this.value = value;
         this.index = index;
      }

      @Override
      public void mergeTo(long[] values) {
         values[index] += value;
      }
   }
}
