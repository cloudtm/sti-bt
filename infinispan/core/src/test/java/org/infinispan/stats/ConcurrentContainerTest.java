package org.infinispan.stats;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.stats.container.ConcurrentGlobalContainer;
import org.infinispan.stats.container.LocalTransactionStatistics;
import org.infinispan.stats.container.RemoteTransactionStatistics;
import org.infinispan.stats.container.StatisticsSnapshot;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "stats.ConcurrentContainerTest")
public class ConcurrentContainerTest {

   private static final Configuration DEFAULT = new ConfigurationBuilder().build();

   public void testIsolationWithTransactionMerge() {
      final ConcurrentGlobalContainer globalContainer = new ConcurrentGlobalContainer();
      final List<StatisticsSnapshot> snapshots = new ArrayList<StatisticsSnapshot>(4);

      snapshots.add(globalContainer.getSnapshot());
      int localIndex;
      int remoteIndex;

      LocalTransactionStatistics localTransactionStatistics = new LocalTransactionStatistics(DEFAULT);

      localIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            localTransactionStatistics.addValue(stats, localIndex++);
         }
      }

      localTransactionStatistics.flush(globalContainer);

      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, (long) localIndex), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, 0L), stats, false);
         }
      }

      RemoteTransactionStatistics remoteTransactionStatistics = new RemoteTransactionStatistics(DEFAULT);

      remoteIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isRemote()) {
            remoteTransactionStatistics.addValue(stats, remoteIndex++);
         }
      }

      remoteTransactionStatistics.flush(globalContainer);

      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, (long) localIndex, (long) localIndex), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, 0L, (long) remoteIndex), stats, false);
            remoteIndex++;
         }
      }

      assertFinalState(globalContainer);
   }

   public void testIsolationWithSingleActionMerge() {
      final ConcurrentGlobalContainer globalContainer = new ConcurrentGlobalContainer();
      final List<StatisticsSnapshot> snapshots = new ArrayList<StatisticsSnapshot>(4);
      snapshots.add(globalContainer.getSnapshot());

      //two random stats, one local and one remote
      final ExposedStatistic localStat = ExposedStatistic.PREPARE_COMMAND_SIZE;
      final ExposedStatistic remoteStat = ExposedStatistic.TX_COMPLETE_NOTIFY_EXECUTION_TIME;

      Assert.assertTrue(localStat.isLocal());
      Assert.assertTrue(remoteStat.isRemote());

      globalContainer.add(localStat, 10, true);

      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0L, 10L), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0L, 0L), remoteStat, false);


      globalContainer.add(remoteStat, 20, false);

      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0L, 10L, 10L), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0L, 0L, 20L), remoteStat, false);

      try {
         globalContainer.add(localStat, 30, false);
         Assert.fail("Expected exception");
      } catch (Exception e) {
         //expected
      }

      assertSnapshotValues(snapshots, Arrays.asList(0L, 10L, 10L), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0L, 0L, 20L), remoteStat, false);

      try {
         globalContainer.add(remoteStat, 30, true);
         Assert.fail("Expected exception");
      } catch (Exception e) {
         //expected
      }

      assertSnapshotValues(snapshots, Arrays.asList(0L, 10L, 10L), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0L, 0L, 20L), remoteStat, false);

      assertFinalState(globalContainer);
   }

   public void testIsolationWithReset() {
      final ConcurrentGlobalContainer globalContainer = new ConcurrentGlobalContainer();
      final List<StatisticsSnapshot> snapshots = new ArrayList<StatisticsSnapshot>(4);

      snapshots.add(globalContainer.getSnapshot());
      int localIndex;
      int remoteIndex;

      LocalTransactionStatistics localTransactionStatistics = new LocalTransactionStatistics(DEFAULT);
      RemoteTransactionStatistics remoteTransactionStatistics = new RemoteTransactionStatistics(DEFAULT);

      localIndex = 0;
      remoteIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            localTransactionStatistics.addValue(stats, localIndex++);
         }
         if (stats.isRemote()) {
            remoteTransactionStatistics.addValue(stats, remoteIndex++);
         }
      }

      localTransactionStatistics.flush(globalContainer);
      remoteTransactionStatistics.flush(globalContainer);

      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, (long) localIndex), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, (long) remoteIndex), stats, false);
            remoteIndex++;
         }
      }

      globalContainer.reset();

      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, (long) localIndex, 0L), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, (long) remoteIndex, 0L), stats, false);
            remoteIndex++;
         }
      }

      localTransactionStatistics.flush(globalContainer);
      remoteTransactionStatistics.flush(globalContainer);

      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, (long) localIndex, 0L, (long) localIndex), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, (long) remoteIndex, 0L, (long) remoteIndex), stats, false);
            remoteIndex++;
         }
      }

      assertFinalState(globalContainer);
   }

   public void testIsolationWithResetMerge() {
      final ConcurrentGlobalContainer globalContainer = new ConcurrentGlobalContainer();
      final List<StatisticsSnapshot> snapshots = new ArrayList<StatisticsSnapshot>(4);
      snapshots.add(globalContainer.getSnapshot());

      //two random stats, one local and one remote
      final ExposedStatistic localStat = ExposedStatistic.PREPARE_COMMAND_SIZE;
      final ExposedStatistic remoteStat = ExposedStatistic.TX_COMPLETE_NOTIFY_EXECUTION_TIME;

      Assert.assertTrue(localStat.isLocal());
      Assert.assertTrue(remoteStat.isRemote());

      globalContainer.add(localStat, 10, true);
      globalContainer.add(remoteStat, 20, false);

      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0L, 10L), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0L, 20L), remoteStat, false);

      globalContainer.reset();
      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0L, 10L, 0L), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0L, 20L, 0L), remoteStat, false);

      globalContainer.add(localStat, 10, true);
      globalContainer.add(remoteStat, 20, false);

      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0L, 10L, 0L, 10L), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0L, 20L, 0L, 20L), remoteStat, false);

      assertFinalState(globalContainer);
   }

   public void testIsolationWithEnqueueAndResetTransaction() {
      final ConcurrentGlobalContainer globalContainer = new ConcurrentGlobalContainer();
      final List<StatisticsSnapshot> snapshots = new ArrayList<StatisticsSnapshot>(4);

      snapshots.add(globalContainer.getSnapshot());
      int localIndex;
      int remoteIndex;

      LocalTransactionStatistics localTransactionStatistics = new LocalTransactionStatistics(DEFAULT);
      RemoteTransactionStatistics remoteTransactionStatistics = new RemoteTransactionStatistics(DEFAULT);

      localIndex = 0;
      remoteIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            localTransactionStatistics.addValue(stats, localIndex++);
         }
         if (stats.isRemote()) {
            remoteTransactionStatistics.addValue(stats, remoteIndex++);
         }
      }

      //all the stuff is enqueued
      globalContainer.flushing().set(true);

      localTransactionStatistics.flush(globalContainer);
      remoteTransactionStatistics.flush(globalContainer);
      localTransactionStatistics.flush(globalContainer);
      remoteTransactionStatistics.flush(globalContainer);
      localTransactionStatistics.flush(globalContainer);
      remoteTransactionStatistics.flush(globalContainer);

      Assert.assertEquals(globalContainer.queue().size(), 6);

      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, 0L), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, 0L), stats, false);
            remoteIndex++;
         }
      }

      globalContainer.flushing().set(false);

      //this should flush pending statistics
      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, 0L, 3L * localIndex), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, 0L, 3L * remoteIndex), stats, false);
            remoteIndex++;
         }
      }

      globalContainer.reset();
      snapshots.clear();
      snapshots.add(globalContainer.getSnapshot());

      localIndex = 0;
      remoteIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L), stats, false);
            remoteIndex++;
         }
      }

      globalContainer.flushing().set(true);

      localTransactionStatistics.flush(globalContainer);
      remoteTransactionStatistics.flush(globalContainer);
      localTransactionStatistics.flush(globalContainer);
      remoteTransactionStatistics.flush(globalContainer);
      localTransactionStatistics.flush(globalContainer);

      globalContainer.reset();

      remoteTransactionStatistics.flush(globalContainer);

      snapshots.add(globalContainer.getSnapshot());

      Assert.assertTrue(globalContainer.isReset());
      Assert.assertEquals(globalContainer.queue().size(), 6);

      localIndex = 0;
      remoteIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, 0L), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, 0L), stats, false);
            remoteIndex++;
         }
      }

      globalContainer.flushing().set(false);
      snapshots.add(globalContainer.getSnapshot());
      Assert.assertFalse(globalContainer.isReset());

      localIndex = 0;
      remoteIndex = 0;
      for (ExposedStatistic stats : ExposedStatistic.values()) {
         if (stats.isLocal()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, 0L, 0L), stats, true);
            localIndex++;
         }
         if (stats.isRemote()) {
            assertSnapshotValues(snapshots, Arrays.<Long>asList(0L, 0L, 0L), stats, false);
            remoteIndex++;
         }
      }

      assertFinalState(globalContainer);
   }

   public void testIsolationWithEnqueueAndResetSingleAction() {
      final ConcurrentGlobalContainer globalContainer = new ConcurrentGlobalContainer();
      final List<StatisticsSnapshot> snapshots = new ArrayList<StatisticsSnapshot>(4);
      snapshots.add(globalContainer.getSnapshot());

      //two random stats, one local and one remote
      final ExposedStatistic localStat = ExposedStatistic.PREPARE_COMMAND_SIZE;
      final ExposedStatistic remoteStat = ExposedStatistic.TX_COMPLETE_NOTIFY_EXECUTION_TIME;

      Assert.assertTrue(localStat.isLocal());
      Assert.assertTrue(remoteStat.isRemote());

      globalContainer.flushing().set(true);

      globalContainer.add(localStat, 10, true);
      globalContainer.add(remoteStat, 20, false);

      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0L, 0L), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0L, 0L), remoteStat, false);

      Assert.assertEquals(globalContainer.queue().size(), 2);
      globalContainer.flushing().set(false);

      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0L, 0L, 10L), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0L, 0L, 20L), remoteStat, false);

      globalContainer.reset();
      snapshots.clear();
      snapshots.add(globalContainer.getSnapshot());

      assertSnapshotValues(snapshots, Arrays.asList(0L), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0L), remoteStat, false);

      globalContainer.flushing().set(true);

      globalContainer.add(localStat, 10, true);
      globalContainer.add(remoteStat, 20, false);
      globalContainer.reset();

      snapshots.add(globalContainer.getSnapshot());
      Assert.assertTrue(globalContainer.isReset());
      Assert.assertEquals(globalContainer.queue().size(), 2);

      assertSnapshotValues(snapshots, Arrays.asList(0L, 0L), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0L, 0L), remoteStat, false);

      globalContainer.flushing().set(false);

      snapshots.add(globalContainer.getSnapshot());
      Assert.assertFalse(globalContainer.isReset());
      Assert.assertTrue(globalContainer.queue().isEmpty());

      assertSnapshotValues(snapshots, Arrays.asList(0L, 0L, 0L), localStat, true);
      assertSnapshotValues(snapshots, Arrays.asList(0L, 0L, 0L), remoteStat, false);

      assertFinalState(globalContainer);
   }

   private void assertSnapshotValues(List<StatisticsSnapshot> snapshots, List<Long> expected, ExposedStatistic stat, boolean local) {
      Assert.assertEquals(snapshots.size(), expected.size());
      for (int i = 0; i < snapshots.size(); ++i) {
         if (local) {
            Assert.assertEquals((long) expected.get(i), snapshots.get(i).getLocal(stat));
         } else {
            Assert.assertEquals((long) expected.get(i), snapshots.get(i).getRemote(stat));
         }
      }
   }

   private void assertFinalState(ConcurrentGlobalContainer globalContainer) {
      Assert.assertTrue(globalContainer.queue().isEmpty());
      Assert.assertFalse(globalContainer.isReset());
      Assert.assertFalse(globalContainer.flushing().get());
   }
}
