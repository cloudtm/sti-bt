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
package org.infinispan.stats;

import org.infinispan.stats.container.ConcurrentGlobalContainer;
import org.infinispan.stats.container.StatisticsSnapshot;
import org.infinispan.stats.container.TransactionStatistics;
import org.infinispan.stats.percentiles.PercentileStats;
import org.infinispan.stats.percentiles.PercentileStatsFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.stats.ExposedStatistic.*;


/**
 * Websiste: www.cloudtm.eu Date: 01/05/12
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NodeScopeStatisticCollector {
   private final static Log log = LogFactory.getLog(NodeScopeStatisticCollector.class);
   private final ConcurrentGlobalContainer globalContainer;
   private volatile PercentileStats localTransactionWrExecutionTime;
   private volatile PercentileStats remoteTransactionWrExecutionTime;
   private volatile PercentileStats localTransactionRoExecutionTime;
   private volatile PercentileStats remoteTransactionRoExecutionTime;

   public NodeScopeStatisticCollector() {
      globalContainer = new ConcurrentGlobalContainer();
      reset();
   }

   public final synchronized void reset() {
      if (log.isTraceEnabled()) {
         log.tracef("Resetting Node Scope Statistics");
      }
      globalContainer.reset();

      this.localTransactionRoExecutionTime = PercentileStatsFactory.createNewPercentileStats();
      this.localTransactionWrExecutionTime = PercentileStatsFactory.createNewPercentileStats();
      this.remoteTransactionRoExecutionTime = PercentileStatsFactory.createNewPercentileStats();
      this.remoteTransactionWrExecutionTime = PercentileStatsFactory.createNewPercentileStats();
   }

   public final void merge(TransactionStatistics ts) {
      if (log.isTraceEnabled()) {
         log.tracef("Merge transaction statistics %s to the node statistics", ts);
      }
      ts.flush(globalContainer);
      if (ts.isLocal()) {
         if (ts.isCommit()) {
            if (ts.isReadOnly()) {
               this.localTransactionRoExecutionTime.insertSample(ts.getValue(RO_TX_SUCCESSFUL_EXECUTION_TIME));
            } else {
               this.localTransactionWrExecutionTime.insertSample(ts.getValue(WR_TX_SUCCESSFUL_EXECUTION_TIME));
            }
         } else {
            if (ts.isReadOnly()) {
               this.localTransactionRoExecutionTime.insertSample(ts.getValue(RO_TX_ABORTED_EXECUTION_TIME));
            } else {
               this.localTransactionWrExecutionTime.insertSample(ts.getValue(WR_TX_ABORTED_EXECUTION_TIME));
            }
         }
      } else {
         if (ts.isCommit()) {
            if (ts.isReadOnly()) {
               this.remoteTransactionRoExecutionTime.insertSample(ts.getValue(RO_TX_SUCCESSFUL_EXECUTION_TIME));
            } else {
               this.remoteTransactionWrExecutionTime.insertSample(ts.getValue(WR_TX_SUCCESSFUL_EXECUTION_TIME));
            }
         } else {
            if (ts.isReadOnly()) {
               this.remoteTransactionRoExecutionTime.insertSample(ts.getValue(RO_TX_ABORTED_EXECUTION_TIME));
            } else {
               this.remoteTransactionWrExecutionTime.insertSample(ts.getValue(WR_TX_ABORTED_EXECUTION_TIME));
            }
         }
      }
   }

   public final void addLocalValue(ExposedStatistic stat, double value) {
      globalContainer.add(stat, (long) value, true);
   }

   public final void addRemoteValue(ExposedStatistic stat, double value) {
      globalContainer.add(stat, (long) value, false);
   }

   public final double getPercentile(ExposedStatistic param, int percentile) throws NoIspnStatException {
      if (log.isTraceEnabled()) {
         log.tracef("Get percentile %s from %s", percentile, param);
      }
      switch (param) {
         case RO_LOCAL_PERCENTILE:
            return localTransactionRoExecutionTime.getKPercentile(percentile);
         case WR_LOCAL_PERCENTILE:
            return localTransactionWrExecutionTime.getKPercentile(percentile);
         case RO_REMOTE_PERCENTILE:
            return remoteTransactionRoExecutionTime.getKPercentile(percentile);
         case WR_REMOTE_PERCENTILE:
            return remoteTransactionWrExecutionTime.getKPercentile(percentile);
         default:
            throw new NoIspnStatException("Invalid percentile " + param);
      }
   }

   /*
   Can I invoke this synchronized method from inside itself??
    */

   @SuppressWarnings("UnnecessaryBoxing")
   public final Object getAttribute(ExposedStatistic param) throws NoIspnStatException {
      if (log.isTraceEnabled()) {
         log.tracef("Get attribute %s", param);
      }
      StatisticsSnapshot snapshot = globalContainer.getSnapshot();

      switch (param) {
         case NUM_EARLY_ABORTS: {
            return snapshot.getLocal(param);
         }
         case NUM_LOCALPREPARE_ABORTS: {
            return snapshot.getLocal(param);
         }
         case NUM_REMOTELY_ABORTED: {
            return snapshot.getLocal(param);
         }
         case NUM_UPDATE_TX_GOT_TO_PREPARE: {
            return snapshot.getLocal(param);
         }
         case NUM_WAITS_IN_COMMIT_QUEUE: {
            return snapshot.getLocal(param);
         }
         case NUM_WAITS_IN_REMOTE_COMMIT_QUEUE: {
            return snapshot.getRemote(NUM_WAITS_IN_COMMIT_QUEUE);
         }
         case NUM_WAITS_REMOTE_REMOTE_GETS: {
            return snapshot.getRemote(param);
         }
         case LOCK_HOLD_TIME: {
            long localLocks = snapshot.getLocal(NUM_HELD_LOCKS);
            long remoteLocks = snapshot.getRemote(NUM_HELD_LOCKS);
            if ((localLocks + remoteLocks) != 0) {
               long localHoldTime = snapshot.getLocal(LOCK_HOLD_TIME);
               long remoteHoldTime = snapshot.getRemote(LOCK_HOLD_TIME);
               return new Long(convertNanosToMicro(localHoldTime + remoteHoldTime) / (localLocks + remoteLocks));
            }
            return new Long(0);
         }
         case SUX_LOCK_HOLD_TIME: {
            return microAvgLocal(snapshot, NUM_SUX_LOCKS, SUX_LOCK_HOLD_TIME);
         }
         case LOCAL_ABORT_LOCK_HOLD_TIME: {
            return microAvgLocal(snapshot, NUM_LOCAL_ABORTED_LOCKS, LOCAL_ABORT_LOCK_HOLD_TIME);
         }
         case REMOTE_ABORT_LOCK_HOLD_TIME: {
            return microAvgLocal(snapshot, NUM_REMOTE_ABORTED_LOCKS, REMOTE_ABORT_LOCK_HOLD_TIME);
         }
         case LOCAL_REMOTE_GET_S:
            return microAvgLocal(snapshot, NUM_LOCAL_REMOTE_GET, LOCAL_REMOTE_GET_S);
         case LOCAL_REMOTE_GET_R:
            return microAvgLocal(snapshot, NUM_LOCAL_REMOTE_GET, LOCAL_REMOTE_GET_R);
         case RTT_PREPARE:
            return microAvgLocal(snapshot, NUM_RTTS_PREPARE, RTT_PREPARE);
         case RTT_COMMIT:
            return microAvgLocal(snapshot, NUM_RTTS_COMMIT, RTT_COMMIT);
         case RTT_ROLLBACK:
            return microAvgLocal(snapshot, NUM_RTTS_ROLLBACK, RTT_ROLLBACK);
         case RTT_GET:
            return microAvgLocal(snapshot, NUM_RTTS_GET, RTT_GET);
         case ASYNC_COMMIT:
            return microAvgLocal(snapshot, NUM_ASYNC_COMMIT, ASYNC_COMMIT);
         case ASYNC_COMPLETE_NOTIFY:
            return microAvgLocal(snapshot, NUM_ASYNC_COMPLETE_NOTIFY, ASYNC_COMPLETE_NOTIFY);
         case ASYNC_PREPARE:
            return microAvgLocal(snapshot, NUM_ASYNC_PREPARE, ASYNC_PREPARE);
         case ASYNC_ROLLBACK:
            return microAvgLocal(snapshot, NUM_ASYNC_ROLLBACK, ASYNC_ROLLBACK);
         case NUM_NODES_COMMIT:
            return avgMultipleLocalCounters(snapshot, NUM_NODES_COMMIT, NUM_RTTS_COMMIT, NUM_ASYNC_COMMIT);
         case NUM_NODES_GET:
            return avgMultipleLocalCounters(snapshot, NUM_NODES_GET, NUM_RTTS_GET);
         case NUM_NODES_PREPARE:
            return avgMultipleLocalCounters(snapshot, NUM_NODES_PREPARE, NUM_RTTS_PREPARE, NUM_ASYNC_PREPARE);
         case NUM_NODES_ROLLBACK:
            return avgMultipleLocalCounters(snapshot, NUM_NODES_ROLLBACK, NUM_RTTS_ROLLBACK, NUM_ASYNC_ROLLBACK);
         case NUM_NODES_COMPLETE_NOTIFY:
            return avgMultipleLocalCounters(snapshot, NUM_NODES_COMPLETE_NOTIFY, NUM_ASYNC_COMPLETE_NOTIFY);
         case PUTS_PER_LOCAL_TX: {
            return avgDoubleLocal(snapshot, NUM_COMMITTED_WR_TX, NUM_SUCCESSFUL_PUTS);
         }
         case LOCAL_CONTENTION_PROBABILITY: {
            long numLocalPuts = snapshot.getLocal(NUM_PUT);
            if (numLocalPuts != 0) {
               long numLocalLocalContention = snapshot.getLocal(LOCK_CONTENTION_TO_LOCAL);
               long numLocalRemoteContention = snapshot.getLocal(LOCK_CONTENTION_TO_REMOTE);
               return new Double((numLocalLocalContention + numLocalRemoteContention) * 1.0 / numLocalPuts);
            }
            return new Double(0);
         }
         case REMOTE_CONTENTION_PROBABILITY: {
            long numRemotePuts = snapshot.getRemote(NUM_PUT);
            if (numRemotePuts != 0) {
               long numRemoteLocalContention = snapshot.getRemote(LOCK_CONTENTION_TO_LOCAL);
               long numRemoteRemoteContention = snapshot.getRemote(LOCK_CONTENTION_TO_REMOTE);
               return new Double((numRemoteLocalContention + numRemoteRemoteContention) * 1.0 / numRemotePuts);
            }
            return new Double(0);
         }
         case LOCK_CONTENTION_PROBABILITY: {
            long numLocalPuts = snapshot.getLocal(NUM_PUT);
            long numRemotePuts = snapshot.getRemote(NUM_PUT);
            long totalPuts = numLocalPuts + numRemotePuts;
            if (totalPuts != 0) {
               long localLocal = snapshot.getLocal(LOCK_CONTENTION_TO_LOCAL);
               long localRemote = snapshot.getLocal(LOCK_CONTENTION_TO_REMOTE);
               long remoteLocal = snapshot.getRemote(LOCK_CONTENTION_TO_LOCAL);
               long remoteRemote = snapshot.getRemote(LOCK_CONTENTION_TO_REMOTE);
               long totalCont = localLocal + localRemote + remoteLocal + remoteRemote;
               return new Double(totalCont / totalPuts);
            }
            return new Double(0);
         }

         case LOCK_CONTENTION_TO_LOCAL: {
            return new Long(snapshot.getLocal(param));
         }
         case LOCK_CONTENTION_TO_REMOTE: {
            return new Long(snapshot.getLocal(param));
         }
         case REMOTE_LOCK_CONTENTION_TO_LOCAL: {
            return new Long(snapshot.getLocal(LOCK_CONTENTION_TO_LOCAL));
         }

         case REMOTE_LOCK_CONTENTION_TO_REMOTE: {
            return new Long(snapshot.getLocal(LOCK_CONTENTION_TO_REMOTE));
         }

         case NUM_OWNED_WR_ITEMS_IN_LOCAL_PREPARE: {
            return avgDoubleLocal(snapshot, NUM_UPDATE_TX_PREPARED, NUM_OWNED_WR_ITEMS_IN_OK_PREPARE);
         }
         case NUM_OWNED_WR_ITEMS_IN_REMOTE_PREPARE: {
            return avgDoubleRemote(snapshot, NUM_UPDATE_TX_PREPARED, NUM_OWNED_WR_ITEMS_IN_OK_PREPARE);
         }

         case NUM_OWNED_RD_ITEMS_IN_LOCAL_PREPARE: {
            return avgDoubleLocal(snapshot, NUM_UPDATE_TX_PREPARED, NUM_OWNED_RD_ITEMS_IN_OK_PREPARE);
         }
         case NUM_OWNED_RD_ITEMS_IN_REMOTE_PREPARE: {
            return avgDoubleRemote(snapshot, NUM_UPDATE_TX_PREPARED, NUM_OWNED_RD_ITEMS_IN_OK_PREPARE);
         }


        /*
         Local execution times
          */
         case UPDATE_TX_LOCAL_S: {
            return microAvgLocal(snapshot, NUM_UPDATE_TX_LOCAL_COMMIT, UPDATE_TX_LOCAL_S);
         }
         case UPDATE_TX_LOCAL_R: {
            return microAvgLocal(snapshot, NUM_UPDATE_TX_LOCAL_COMMIT, UPDATE_TX_LOCAL_R);
         }
         case READ_ONLY_TX_LOCAL_S: {
            return microAvgLocal(snapshot, NUM_READ_ONLY_TX_COMMIT, READ_ONLY_TX_LOCAL_S);
         }
         case READ_ONLY_TX_LOCAL_R: {
            return microAvgLocal(snapshot, NUM_READ_ONLY_TX_COMMIT, READ_ONLY_TX_LOCAL_R);
         }
         /*
         Prepare Times (only relevant to successfully executed prepareCommand...even for xact that eventually abort...)
         //TODO: is it the case to take also this stat only for xacts that commit?
          */

         case UPDATE_TX_LOCAL_PREPARE_R: {
            return microAvgLocal(snapshot, NUM_UPDATE_TX_PREPARED, UPDATE_TX_LOCAL_PREPARE_R);
         }

         case UPDATE_TX_LOCAL_PREPARE_S: {
            return microAvgLocal(snapshot, NUM_UPDATE_TX_PREPARED, UPDATE_TX_LOCAL_PREPARE_S);
         }
         case UPDATE_TX_REMOTE_PREPARE_R: {
            return microAvgRemote(snapshot, NUM_UPDATE_TX_PREPARED, UPDATE_TX_REMOTE_PREPARE_R);
         }
         case UPDATE_TX_REMOTE_PREPARE_S: {
            return microAvgRemote(snapshot, NUM_UPDATE_TX_PREPARED, UPDATE_TX_REMOTE_PREPARE_S);
         }
         case READ_ONLY_TX_PREPARE_R: {
            return microAvgRemote(snapshot, NUM_READ_ONLY_TX_COMMIT, READ_ONLY_TX_PREPARE_R);
         }
         case READ_ONLY_TX_PREPARE_S: {
            return microAvgRemote(snapshot, NUM_READ_ONLY_TX_COMMIT, READ_ONLY_TX_PREPARE_S);
         }

          /*
             Commit Times
           */

         case UPDATE_TX_LOCAL_COMMIT_R: {
            return microAvgLocal(snapshot, NUM_UPDATE_TX_LOCAL_COMMIT, UPDATE_TX_LOCAL_COMMIT_R);
         }
         case UPDATE_TX_LOCAL_COMMIT_S: {
            return microAvgLocal(snapshot, NUM_UPDATE_TX_LOCAL_COMMIT, UPDATE_TX_LOCAL_COMMIT_S);
         }
         case UPDATE_TX_REMOTE_COMMIT_R: {
            return microAvgRemote(snapshot, NUM_UPDATE_TX_REMOTE_COMMIT, UPDATE_TX_REMOTE_COMMIT_R);
         }
         case UPDATE_TX_REMOTE_COMMIT_S: {
            return microAvgRemote(snapshot, NUM_UPDATE_TX_REMOTE_COMMIT, UPDATE_TX_REMOTE_COMMIT_S);
         }
         case READ_ONLY_TX_COMMIT_R: {
            return microAvgRemote(snapshot, NUM_READ_ONLY_TX_COMMIT, READ_ONLY_TX_COMMIT_R);
         }
         case READ_ONLY_TX_COMMIT_S: {
            return microAvgRemote(snapshot, NUM_READ_ONLY_TX_COMMIT, READ_ONLY_TX_COMMIT_S);
         }

          /*
          Rollback times
           */

         case UPDATE_TX_REMOTE_ROLLBACK_R: {
            return microAvgRemote(snapshot, NUM_UPDATE_TX_REMOTE_ROLLBACK, UPDATE_TX_REMOTE_ROLLBACK_R);
         }
         case UPDATE_TX_REMOTE_ROLLBACK_S: {
            return microAvgRemote(snapshot, NUM_UPDATE_TX_REMOTE_ROLLBACK, UPDATE_TX_REMOTE_ROLLBACK_S);
         }
         case UPDATE_TX_LOCAL_REMOTE_ROLLBACK_R: {
            return microAvgLocal(snapshot, NUM_UPDATE_TX_LOCAL_REMOTE_ROLLBACK, UPDATE_TX_LOCAL_REMOTE_ROLLBACK_R);
         }
         case UPDATE_TX_LOCAL_REMOTE_ROLLBACK_S: {
            return microAvgLocal(snapshot, NUM_UPDATE_TX_LOCAL_REMOTE_ROLLBACK, UPDATE_TX_LOCAL_REMOTE_ROLLBACK_S);
         }
         case UPDATE_TX_LOCAL_LOCAL_ROLLBACK_R: {
            return microAvgLocal(snapshot, NUM_UPDATE_TX_LOCAL_LOCAL_ROLLBACK, UPDATE_TX_LOCAL_LOCAL_ROLLBACK_R);
         }
         case UPDATE_TX_LOCAL_LOCAL_ROLLBACK_S: {
            return microAvgLocal(snapshot, NUM_UPDATE_TX_LOCAL_LOCAL_ROLLBACK, UPDATE_TX_LOCAL_LOCAL_ROLLBACK_S);
         }
         case REMOTE_REMOTE_GET_WAITING_TIME: {
            return microAvgRemote(snapshot, NUM_WAITS_REMOTE_REMOTE_GETS, REMOTE_REMOTE_GET_WAITING_TIME);
         }
         case REMOTE_REMOTE_GET_R: {
            return microAvgRemote(snapshot, NUM_REMOTE_REMOTE_GETS, REMOTE_REMOTE_GET_R);
         }
         case REMOTE_REMOTE_GET_S: {
            return microAvgRemote(snapshot, NUM_REMOTE_REMOTE_GETS, REMOTE_REMOTE_GET_S);
         }
         case FIRST_WRITE_INDEX: {
            return avgDoubleLocal(snapshot, NUM_COMMITTED_WR_TX, FIRST_WRITE_INDEX);
         }
         case LOCK_WAITING_TIME: {
            long localWaitedForLocks = snapshot.getLocal(NUM_WAITED_FOR_LOCKS);
            long remoteWaitedForLocks = snapshot.getRemote(NUM_WAITED_FOR_LOCKS);
            long totalWaitedForLocks = localWaitedForLocks + remoteWaitedForLocks;
            if (totalWaitedForLocks != 0) {
               long localWaitedTime = snapshot.getLocal(LOCK_WAITING_TIME);
               long remoteWaitedTime = snapshot.getRemote(LOCK_WAITING_TIME);
               return new Long(convertNanosToMicro(localWaitedTime + remoteWaitedTime) / totalWaitedForLocks);
            }
            return new Long(0);
         }
         case TX_WRITE_PERCENTAGE: {     //computed on the locally born txs
            long readTx = snapshot.getLocal(NUM_READ_ONLY_TX_COMMIT) +
                  snapshot.getLocal(NUM_ABORTED_RO_TX);
            long writeTx = snapshot.getLocal(NUM_COMMITTED_WR_TX) +
                  snapshot.getLocal(NUM_ABORTED_WR_TX);
            long total = readTx + writeTx;
            if (total != 0)
               return new Double(writeTx * 1.0 / total);
            return new Double(0);
         }
         case SUCCESSFUL_WRITE_PERCENTAGE: { //computed on the locally born txs
            long readSuxTx = snapshot.getLocal(NUM_READ_ONLY_TX_COMMIT);
            long writeSuxTx = snapshot.getLocal(NUM_COMMITTED_WR_TX);
            long total = readSuxTx + writeSuxTx;
            if (total != 0) {
               return new Double(writeSuxTx * 1.0 / total);
            }
            return new Double(0);
         }
         case APPLICATION_CONTENTION_FACTOR: {
            long localTakenLocks = snapshot.getLocal(NUM_HELD_LOCKS);
            long remoteTakenLocks = snapshot.getRemote(NUM_HELD_LOCKS);
            long elapsedTime = System.nanoTime() - snapshot.getLastResetTime();
            double totalLocksArrivalRate = (localTakenLocks + remoteTakenLocks) / convertNanosToMicro(elapsedTime);
            long holdTime = (Long) this.getAttribute(LOCK_HOLD_TIME);

            if ((totalLocksArrivalRate * holdTime) != 0) {
               double lockContProb = (Double) this.getAttribute(LOCK_CONTENTION_PROBABILITY);
               return new Double(lockContProb / (totalLocksArrivalRate * holdTime));
            }
            return new Double(0);
         }
         case NUM_SUCCESSFUL_GETS_RO_TX:
            return avgLocal(snapshot, NUM_READ_ONLY_TX_COMMIT, NUM_SUCCESSFUL_GETS_RO_TX);
         case NUM_SUCCESSFUL_GETS_WR_TX:
            return avgLocal(snapshot, NUM_COMMITTED_WR_TX, NUM_SUCCESSFUL_GETS_WR_TX);
         case NUM_SUCCESSFUL_REMOTE_GETS_RO_TX:
            return avgDoubleLocal(snapshot, NUM_READ_ONLY_TX_COMMIT, NUM_SUCCESSFUL_REMOTE_GETS_RO_TX);
         case NUM_SUCCESSFUL_REMOTE_GETS_WR_TX:
            return avgDoubleLocal(snapshot, NUM_COMMITTED_WR_TX, NUM_SUCCESSFUL_REMOTE_GETS_WR_TX);

         case NUM_SUCCESSFUL_PUTS_WR_TX:
            return avgDoubleLocal(snapshot, NUM_COMMITTED_WR_TX, NUM_SUCCESSFUL_PUTS_WR_TX);
         case NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX:
            return avgDoubleLocal(snapshot, NUM_COMMITTED_WR_TX, NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX);
         case REMOTE_PUT_EXECUTION:
            return microAvgLocal(snapshot, NUM_REMOTE_PUT, REMOTE_PUT_EXECUTION);
         case NUM_LOCK_FAILED_DEADLOCK:
         case NUM_LOCK_FAILED_TIMEOUT:
         case NUM_READLOCK_FAILED_TIMEOUT:
            return new Long(snapshot.getLocal(param));
         case WR_TX_SUCCESSFUL_EXECUTION_TIME:
            return microAvgLocal(snapshot, NUM_COMMITTED_WR_TX, WR_TX_SUCCESSFUL_EXECUTION_TIME);
         case WR_TX_ABORTED_EXECUTION_TIME:
            return microAvgLocal(snapshot, NUM_ABORTED_WR_TX, WR_TX_ABORTED_EXECUTION_TIME);
         case PREPARE_COMMAND_SIZE:
            return avgMultipleLocalCounters(snapshot, PREPARE_COMMAND_SIZE, NUM_RTTS_PREPARE, NUM_ASYNC_PREPARE);
         case ROLLBACK_COMMAND_SIZE:
            return avgMultipleLocalCounters(snapshot, ROLLBACK_COMMAND_SIZE, NUM_RTTS_ROLLBACK, NUM_ASYNC_ROLLBACK);
         case COMMIT_COMMAND_SIZE:
            return avgMultipleLocalCounters(snapshot, COMMIT_COMMAND_SIZE, NUM_RTTS_COMMIT, NUM_ASYNC_COMMIT);
         case CLUSTERED_GET_COMMAND_SIZE:
            return avgLocal(snapshot, NUM_RTTS_GET, CLUSTERED_GET_COMMAND_SIZE);
         case REMOTE_REMOTE_GET_REPLY_SIZE:
            return avgRemote(snapshot, NUM_REMOTE_REMOTE_GETS, REMOTE_REMOTE_GET_REPLY_SIZE);
         case NUM_LOCK_PER_LOCAL_TX:
            return avgMultipleLocalCounters(snapshot, NUM_HELD_LOCKS, NUM_COMMITTED_WR_TX, NUM_ABORTED_WR_TX);
         case NUM_LOCK_PER_REMOTE_TX:
            return avgMultipleRemoteCounters(snapshot, NUM_HELD_LOCKS, NUM_COMMITTED_WR_TX, NUM_ABORTED_WR_TX);
         case NUM_LOCK_PER_SUCCESS_LOCAL_TX:
            return avgLocal(snapshot, NUM_COMMITTED_WR_TX, NUM_HELD_LOCKS_SUCCESS_TX);
         case TX_COMPLETE_NOTIFY_EXECUTION_TIME:
            return microAvgRemote(snapshot, NUM_TX_COMPLETE_NOTIFY_COMMAND, TX_COMPLETE_NOTIFY_EXECUTION_TIME);
         case UPDATE_TX_TOTAL_R: {
            return microAvgLocal(snapshot, NUM_COMMITTED_WR_TX, UPDATE_TX_TOTAL_R);
         }
         case UPDATE_TX_TOTAL_S: {
            return microAvgLocal(snapshot, NUM_COMMITTED_WR_TX, UPDATE_TX_TOTAL_S);
         }
         case READ_ONLY_TX_TOTAL_R: {
            return microAvgLocal(snapshot, NUM_READ_ONLY_TX_COMMIT, READ_ONLY_TX_TOTAL_R);
         }
         case READ_ONLY_TX_TOTAL_S: {
            return microAvgLocal(snapshot, NUM_READ_ONLY_TX_COMMIT, READ_ONLY_TX_TOTAL_S);
         }
         case ABORT_RATE:
            long totalAbort = snapshot.getLocal(NUM_ABORTED_RO_TX) +
                  snapshot.getLocal(NUM_ABORTED_WR_TX);
            long totalCommitAndAbort = snapshot.getLocal(NUM_READ_ONLY_TX_COMMIT) +
                  snapshot.getLocal(NUM_COMMITTED_WR_TX) + totalAbort;
            if (totalCommitAndAbort != 0) {
               return new Double(totalAbort * 1.0 / totalCommitAndAbort);
            }
            return new Double(0);
         case NUM_ABORTED_WR_TX: {
            return new Long(snapshot.getLocal(param));
         }
         case ARRIVAL_RATE:
            long localCommittedTx = snapshot.getLocal(NUM_READ_ONLY_TX_COMMIT) +
                  snapshot.getLocal(NUM_COMMITTED_WR_TX);
            long localAbortedTx = snapshot.getLocal(NUM_ABORTED_RO_TX) +
                  snapshot.getLocal(NUM_ABORTED_WR_TX);
            long remoteCommittedTx = snapshot.getRemote(NUM_READ_ONLY_TX_COMMIT) +
                  snapshot.getRemote(NUM_COMMITTED_WR_TX);
            long remoteAbortedTx = snapshot.getRemote(NUM_ABORTED_RO_TX) +
                  snapshot.getRemote(NUM_ABORTED_WR_TX);
            long totalBornTx = localAbortedTx + localCommittedTx + remoteAbortedTx + remoteCommittedTx;
            return new Double(totalBornTx * 1.0 / convertNanosToSeconds(System.nanoTime() - snapshot.getLastResetTime()));
         case THROUGHPUT:
            long totalLocalBornTx = snapshot.getLocal(NUM_READ_ONLY_TX_COMMIT) +
                  snapshot.getLocal(NUM_COMMITTED_WR_TX);
            return new Double(totalLocalBornTx * 1.0 / convertNanosToSeconds(System.nanoTime() - snapshot.getLastResetTime()));
         case LOCK_HOLD_TIME_LOCAL:
            return microAvgLocal(snapshot, NUM_HELD_LOCKS, LOCK_HOLD_TIME);
         case LOCK_HOLD_TIME_REMOTE:
            return microAvgRemote(snapshot, NUM_HELD_LOCKS, LOCK_HOLD_TIME);
         case NUM_COMMITS:
            return new Long(snapshot.getLocal(NUM_READ_ONLY_TX_COMMIT) +
                                  snapshot.getLocal(NUM_COMMITTED_WR_TX) +
                                  snapshot.getRemote(NUM_READ_ONLY_TX_COMMIT) +
                                  snapshot.getRemote(NUM_COMMITTED_WR_TX));
         case NUM_LOCAL_COMMITS:
            return new Long(snapshot.getLocal(NUM_READ_ONLY_TX_COMMIT) +
                                  snapshot.getLocal(NUM_COMMITTED_WR_TX));
         case WRITE_SKEW_PROBABILITY:
            long totalTxs = snapshot.getLocal(NUM_READ_ONLY_TX_COMMIT) +
                  snapshot.getLocal(NUM_COMMITTED_WR_TX) +
                  snapshot.getLocal(NUM_ABORTED_RO_TX) +
                  snapshot.getLocal(NUM_ABORTED_WR_TX);
            if (totalTxs != 0) {
               long writeSkew = snapshot.getLocal(NUM_WRITE_SKEW);
               return new Double(writeSkew * 1.0 / totalTxs);
            }
            return new Double(0);
         case NUM_GET:
            return snapshot.getLocal(NUM_SUCCESSFUL_GETS_WR_TX) +
                  snapshot.getLocal(NUM_SUCCESSFUL_GETS_RO_TX);
         case NUM_LOCAL_REMOTE_GET:
            return snapshot.getLocal(NUM_SUCCESSFUL_REMOTE_GETS_WR_TX) +
                  snapshot.getLocal(NUM_SUCCESSFUL_REMOTE_GETS_RO_TX);
         case NUM_PUT:
            return snapshot.getLocal(NUM_SUCCESSFUL_PUTS_WR_TX);
         case NUM_REMOTE_PUT:
            return snapshot.getLocal(NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX);
         case LOCAL_GET_EXECUTION:
            long num = snapshot.getLocal(NUM_GET) - snapshot.getLocal(NUM_LOCAL_REMOTE_GET);
            if (num == 0) {
               return new Long(0L);
            } else {
               long local_get_time = snapshot.getLocal(ALL_GET_EXECUTION) -
                     snapshot.getLocal(LOCAL_REMOTE_GET_R);

               return new Long(convertNanosToMicro(local_get_time) / num);
            }
         case WAIT_TIME_IN_COMMIT_QUEUE: {
            return microAvgLocal(snapshot, NUM_WAITS_IN_COMMIT_QUEUE, WAIT_TIME_IN_COMMIT_QUEUE);
         }
         case WAIT_TIME_IN_REMOTE_COMMIT_QUEUE: {
            return microAvgRemote(snapshot, NUM_WAITS_IN_COMMIT_QUEUE, WAIT_TIME_IN_COMMIT_QUEUE);
         }
         case NUM_ABORTED_TX_DUE_TO_VALIDATION: {
            return new Long(snapshot.getLocal(NUM_ABORTED_TX_DUE_TO_VALIDATION));
         }
         case NUM_KILLED_TX_DUE_TO_VALIDATION: {
            return new Long(snapshot.getLocal(NUM_KILLED_TX_DUE_TO_VALIDATION) + snapshot.getRemote(NUM_KILLED_TX_DUE_TO_VALIDATION));
         }
         case RO_TX_SUCCESSFUL_EXECUTION_TIME: {
            return microAvgLocal(snapshot, NUM_COMMITTED_RO_TX, RO_TX_SUCCESSFUL_EXECUTION_TIME);
         }
         case SENT_SYNC_COMMIT: {
            return avgLocal(snapshot, NUM_RTTS_COMMIT, SENT_SYNC_COMMIT);
         }
         case SENT_ASYNC_COMMIT: {
            return avgLocal(snapshot, NUM_ASYNC_COMMIT, SENT_ASYNC_COMMIT);
         }
         case TERMINATION_COST: {
            return avgMultipleLocalCounters(snapshot, TERMINATION_COST, NUM_ABORTED_WR_TX, NUM_ABORTED_RO_TX, NUM_COMMITTED_RO_TX, NUM_COMMITTED_WR_TX);
         }

         case TBC:
            return convertNanosToMicro(avgMultipleLocalCounters(snapshot, TBC_EXECUTION_TIME, NUM_GET, NUM_PUT));
         case NTBC:
            return microAvgLocal(snapshot, NTBC_COUNT, NTBC_EXECUTION_TIME);
         case RESPONSE_TIME:
            long succWrTot = convertNanosToMicro(snapshot.getLocal(WR_TX_SUCCESSFUL_EXECUTION_TIME));
            long abortWrTot = convertNanosToMicro(snapshot.getLocal(WR_TX_ABORTED_EXECUTION_TIME));
            long succRdTot = convertNanosToMicro(snapshot.getLocal(RO_TX_SUCCESSFUL_EXECUTION_TIME));

            long numWr = snapshot.getLocal(NUM_COMMITTED_WR_TX);
            long numRd = snapshot.getLocal(NUM_READ_ONLY_TX_COMMIT);

            if ((numWr + numRd) > 0) {
               return new Long((succRdTot + succWrTot + abortWrTot) / (numWr + numRd));
            } else {
               return new Long(0);
            }
         case GMU_WAITING_IN_QUEUE_DUE_PENDING_LOCAL:
            return microAvgLocal(snapshot, NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING, GMU_WAITING_IN_QUEUE_DUE_PENDING);
         case GMU_WAITING_IN_QUEUE_DUE_PENDING_REMOTE:
            return microAvgRemote(snapshot, NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING, GMU_WAITING_IN_QUEUE_DUE_PENDING);
         case GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_LOCAL:
            return microAvgLocal(snapshot, NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS, GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS);
         case GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_REMOTE:
            return microAvgRemote(snapshot, NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS, GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS);
         case GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_LOCAL:
            return microAvgLocal(snapshot, NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION, GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION);
         case GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_REMOTE:
            return microAvgRemote(snapshot, NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION, GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION);
         case NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING_LOCAL:
            return snapshot.getLocal(NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING);
         case NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING_REMOTE:
            return snapshot.getRemote(NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING);
         case NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_LOCAL:
            return snapshot.getLocal(NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS);
         case NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_REMOTE:
            return snapshot.getRemote(NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS);
         case NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_LOCAL:
            return snapshot.getLocal(NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION);
         case NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_REMOTE:
            return snapshot.getRemote(NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION);
         case TO_GMU_PREPARE_COMMAND_NODES_WAITED:
            return avgLocal(snapshot, NUM_RTTS_PREPARE, TO_GMU_PREPARE_COMMAND_NODES_WAITED);
         case NUM_TO_GMU_PREPARE_COMMAND_AT_LEAST_ONE_WAIT:
            return avgLocal(snapshot, NUM_RTTS_PREPARE, NUM_TO_GMU_PREPARE_COMMAND_AT_LEAST_ONE_WAIT);
         case TO_GMU_PREPARE_COMMAND_RTT_MINUS_AVG:
            return microAvgLocal(snapshot, NUM_RTTS_PREPARE, TO_GMU_PREPARE_COMMAND_RTT_MINUS_AVG);
         case TO_GMU_PREPARE_COMMAND_RTT_MINUS_MAX:
            return microAvgLocal(snapshot, NUM_RTTS_PREPARE, TO_GMU_PREPARE_COMMAND_RTT_MINUS_MAX);
         case TO_GMU_PREPARE_COMMAND_MAX_WAIT_TIME:
            return microAvgLocal(snapshot, NUM_TO_GMU_PREPARE_COMMAND_AT_LEAST_ONE_WAIT, TO_GMU_PREPARE_COMMAND_MAX_WAIT_TIME);
         case TO_GMU_PREPARE_COMMAND_AVG_WAIT_TIME:
            return microAvgLocal(snapshot, NUM_TO_GMU_PREPARE_COMMAND_AT_LEAST_ONE_WAIT, TO_GMU_PREPARE_COMMAND_AVG_WAIT_TIME);
         case TO_GMU_PREPARE_COMMAND_RESPONSE_TIME:
            return microAvgRemote(snapshot, NUM_TO_GMU_PREPARE_COMMAND_SERVED, TO_GMU_PREPARE_COMMAND_RESPONSE_TIME);
         case TO_GMU_PREPARE_COMMAND_SERVICE_TIME:
            return microAvgRemote(snapshot, NUM_TO_GMU_PREPARE_COMMAND_SERVED, TO_GMU_PREPARE_COMMAND_SERVICE_TIME);
         case TO_GMU_PREPARE_COMMAND_REMOTE_WAIT:
            return microAvgRemote(snapshot, NUM_TO_GMU_PREPARE_COMMAND_REMOTE_WAITED, TO_GMU_PREPARE_COMMAND_REMOTE_WAIT);
         case NUM_TO_GMU_PREPARE_COMMAND_REMOTE_WAITED:
            return snapshot.getRemote(NUM_TO_GMU_PREPARE_COMMAND_REMOTE_WAITED);
         case TO_GMU_PREPARE_COMMAND_RTT_NO_WAIT:
            return microAvgLocal(snapshot, NUM_TO_GMU_PREPARE_COMMAND_RTT_NO_WAITED, TO_GMU_PREPARE_COMMAND_RTT_NO_WAIT);
         case NUM_TO_GMU_PREPARE_COMMAND_RTT_NO_WAITED:
            return snapshot.getLocal(NUM_TO_GMU_PREPARE_COMMAND_RTT_NO_WAITED);
         case RTT_GET_NO_WAIT:
            return microAvgLocal(snapshot, NUM_RTT_GET_NO_WAIT, RTT_GET_NO_WAIT);
         default:
            throw new NoIspnStatException("Invalid statistic " + param);
      }
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Long avgLocal(StatisticsSnapshot snapshot, ExposedStatistic counter, ExposedStatistic duration) {
      long num = snapshot.getLocal(counter);
      if (num != 0) {
         long dur = snapshot.getLocal(duration);
         return new Long(dur / num);
      }
      return new Long(0);
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Long avgRemote(StatisticsSnapshot snapshot, ExposedStatistic counter, ExposedStatistic duration) {
      long num = snapshot.getRemote(counter);
      if (num != 0) {
         long dur = snapshot.getRemote(duration);
         return new Long(dur / num);
      }
      return new Long(0);
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Double avgDoubleLocal(StatisticsSnapshot snapshot, ExposedStatistic counter, ExposedStatistic duration) {
      double num = snapshot.getLocal(counter);
      if (num != 0) {
         double dur = snapshot.getLocal(duration);
         return new Double(dur / num);
      }
      return new Double(0);
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Double avgDoubleRemote(StatisticsSnapshot snapshot, ExposedStatistic counter, ExposedStatistic duration) {
      double num = snapshot.getRemote(counter);
      if (num != 0) {
         double dur = snapshot.getRemote(duration);
         return new Double(dur / num);
      }
      return new Double(0);
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Long avgMultipleLocalCounters(StatisticsSnapshot snapshot, ExposedStatistic duration, ExposedStatistic... counters) {
      long num = 0;
      for (ExposedStatistic counter : counters) {
         num += snapshot.getLocal(counter);
      }
      if (num != 0) {
         long dur = snapshot.getLocal(duration);
         return new Long(dur / num);
      }
      return new Long(0);
   }

   @SuppressWarnings("UnnecessaryBoxing")
   private Long avgMultipleRemoteCounters(StatisticsSnapshot snapshot, ExposedStatistic duration, ExposedStatistic... counters) {
      long num = 0;
      for (ExposedStatistic counter : counters) {
         num += snapshot.getRemote(counter);
      }
      if (num != 0) {
         long dur = snapshot.getRemote(duration);
         return new Long(dur / num);
      }
      return new Long(0);
   }

   private static long convertNanosToMicro(long nanos) {
      return nanos / 1000;
   }

   private static long convertNanosToMillis(long nanos) {
      return nanos / 1000000;
   }

   private static long convertNanosToSeconds(long nanos) {
      return nanos / 1000000000;
   }

   private Long microAvgLocal(StatisticsSnapshot snapshot, ExposedStatistic counter, ExposedStatistic duration) {
      return convertNanosToMicro(avgLocal(snapshot, counter, duration));
   }

   private Long microAvgRemote(StatisticsSnapshot snapshot, ExposedStatistic counter, ExposedStatistic duration) {
      return convertNanosToMicro(avgRemote(snapshot, counter, duration));
   }

}
