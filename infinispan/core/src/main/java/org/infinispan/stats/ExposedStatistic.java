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

/**
 * Date: 28/12/11 Time: 15:38
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */

public enum ExposedStatistic {

   LOCK_WAITING_TIME(true, true),         // C
   LOCK_HOLD_TIME(true, true),            // C
   SUX_LOCK_HOLD_TIME(true, true),
   LOCAL_ABORT_LOCK_HOLD_TIME(true, false),
   REMOTE_ABORT_LOCK_HOLD_TIME(true, true),
   NUM_SUX_LOCKS(true, true),
   NUM_LOCAL_ABORTED_LOCKS(true, false),
   NUM_REMOTE_ABORTED_LOCKS(true, true),
   NUM_HELD_LOCKS(true, true),            // C
   NUM_HELD_LOCKS_SUCCESS_TX(true, false),   // L

   NUM_COMMITTED_RO_TX(true, true), // C
   NUM_COMMITTED_WR_TX(true, true), // C
   NUM_ABORTED_WR_TX(true, true),   // C
   NUM_ABORTED_RO_TX(true, true),   // C
   NUM_COMMITS(false, false),  //ONLY FOR QUERY
   NUM_LOCAL_COMMITS(false, false),  //ONLY FOR QUERY

   NUM_PREPARES(true, false), // L
   LOCAL_EXEC_NO_CONT(true, false),            // L
   LOCAL_CONTENTION_PROBABILITY(false, false),  // ONLY FOR QUERY, derived on the fly
   REMOTE_CONTENTION_PROBABILITY(false, false), //ONLY FOR QUERY, derived on the fly
   LOCK_CONTENTION_PROBABILITY(false, false), //ONLY FOR QUERY, derived on the fly
   LOCK_HOLD_TIME_LOCAL(false, false), //ONLY FOR QUERY
   LOCK_HOLD_TIME_REMOTE(false, false), //ONLY FOR QUERY
   LOCK_CONTENTION_TO_LOCAL(true, true),  // C
   LOCK_CONTENTION_TO_REMOTE(true, true), // C
   REMOTE_LOCK_CONTENTION_TO_LOCAL(false, false), //just to query
   REMOTE_LOCK_CONTENTION_TO_REMOTE(false, false), //just to query
   NUM_SUCCESSFUL_PUTS(true, false),   // L, this includes also repeated puts over the same item
   PUTS_PER_LOCAL_TX(false, false), // ONLY FOR QUERY, derived on the fly
   NUM_WAITED_FOR_LOCKS(true, true),   // C
   NUM_LOCAL_REMOTE_GET(true, true),                  // C
   NUM_GET(true, true),                          // C
   NUM_SUCCESSFUL_GETS_RO_TX(true, true),        // C
   NUM_SUCCESSFUL_GETS_WR_TX(true, true),        // C
   NUM_SUCCESSFUL_REMOTE_GETS_WR_TX(true, true), // C
   NUM_SUCCESSFUL_REMOTE_GETS_RO_TX(true, true), // C
   LOCAL_GET_EXECUTION(true, true),
   ALL_GET_EXECUTION(true, true),
   REMOTE_PUT_EXECUTION(true, true),            // C
   NUM_REMOTE_PUT(true, true),                  // C
   NUM_PUT(true, true),                         // C
   NUM_SUCCESSFUL_PUTS_WR_TX(true, true),        // C
   NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX(true, true), // C
   TX_WRITE_PERCENTAGE(false, false),           // ONLY FOR QUERY, derived on the fly
   SUCCESSFUL_WRITE_PERCENTAGE(false, false),   // ONLY FOR QUERY, derived on the fly
   WR_TX_ABORTED_EXECUTION_TIME(true, true),    //C
   WR_TX_SUCCESSFUL_EXECUTION_TIME(true, true), //C
   RO_TX_SUCCESSFUL_EXECUTION_TIME(true, true), //C
   RO_TX_ABORTED_EXECUTION_TIME(true, true),    //C
   APPLICATION_CONTENTION_FACTOR(false, false), // ONLY FOR QUERY

   NUM_WRITE_SKEW(true, false), // L
   WRITE_SKEW_PROBABILITY(false, false), // ONLY FOR QUERY

   //Abort rate, arrival rate and throughput
   ABORT_RATE(false, false),     // ONLY FOR QUERY, derived on the fly
   ARRIVAL_RATE(false, false),   // ONLY FOR QUERY, derived on the fly
   THROUGHPUT(false, false),     // ONLY FOR QUERY, derived on the fly

   //Percentile stuff
   RO_LOCAL_PERCENTILE(false, false),  // ONLY FOR QUERY, derived on the fly
   WR_LOCAL_PERCENTILE(false, false),  // ONLY FOR QUERY, derived on the fly
   RO_REMOTE_PERCENTILE(false, false), // ONLY FOR QUERY, derived on the fly
   WR_REMOTE_PERCENTILE(false, false), // ONLY FOR QUERY, derived on the fly

   //Prepare, rollback and commit execution times
   ROLLBACK_EXECUTION_TIME(true, true),   // C
   NUM_ROLLBACKS(true, true),             // C
   LOCAL_ROLLBACK_EXECUTION_TIME(false, false),    // ONLY FOR QUERY, derived on the fly
   REMOTE_ROLLBACK_EXECUTION_TIME(false, false),   // ONLY FOR QUERY, derived on the fly

   //COMMIT_EXECUTION_TIME(true, true),     // C
   NUM_COMMIT_COMMAND(true, true),        // C
   LOCAL_COMMIT_EXECUTION_TIME(false, false),      // ONLY FOR QUERY, derived on the fly
   REMOTE_COMMIT_EXECUTION_TIME(false, false),     // ONLY FOR QUERY, derived on the fly

   PREPARE_EXECUTION_TIME(true, true),    // C
   NUM_PREPARE_COMMAND(true, true),       // C
   LOCAL_PREPARE_EXECUTION_TIME(false, false),     // ONLY FOR QUERY, derived on the fly
   REMOTE_PREPARE_EXECUTION_TIME(false, false),    // ONLY FOR QUERY, derived on the fly

   TX_COMPLETE_NOTIFY_EXECUTION_TIME(false, true),    // R
   NUM_TX_COMPLETE_NOTIFY_COMMAND(false, true),       // R

   //Lock querying
   NUM_LOCK_PER_LOCAL_TX(false, false),         // ONLY FOR QUERY, derived on the fly
   NUM_LOCK_PER_REMOTE_TX(false, false),        // ONLY FOR QUERY, derived on the fly
   NUM_LOCK_PER_SUCCESS_LOCAL_TX(false, false), // ONLY FOR QUERY, derived on the fly

   //commands size
   PREPARE_COMMAND_SIZE(true, false),        // L
   COMMIT_COMMAND_SIZE(true, false),         // L
   CLUSTERED_GET_COMMAND_SIZE(true, false),  // L
   REMOTE_REMOTE_GET_REPLY_SIZE(false, true), //R
   ROLLBACK_COMMAND_SIZE(true, false),//L

   //Lock failed stuff
   NUM_LOCK_FAILED_TIMEOUT(true, false),  //L
   NUM_LOCK_FAILED_DEADLOCK(true, false), //L
   NUM_READLOCK_FAILED_TIMEOUT(true, false),

   //RTT STUFF: everything is local && synchronous communication
   NUM_RTTS_PREPARE(true, false),   // L
   RTT_PREPARE(true, false),        // L
   NUM_RTTS_COMMIT(true, false),    // L
   RTT_COMMIT(true, false),         // L
   NUM_RTTS_ROLLBACK(true, false),  // L
   RTT_ROLLBACK(true, false),       // L
   NUM_RTTS_GET(true, false),       // L
   RTT_GET(true, false),            // L
   RTT_GET_NO_WAIT(true,false),
   NUM_RTT_GET_NO_WAIT(true,false),

   //SEND STUFF: everything is local && asynchronous communication .
   //NUM refers to the number of nodes INVOLVED in the distributed synchronization phases
   //SENT refers to the number of messages effectively sent (i.e., without the current node, if present in the recipient list)
   ASYNC_PREPARE(true, false),               // L
   NUM_ASYNC_PREPARE(true, false),           // L
   ASYNC_COMMIT(true, false),                // L
   NUM_ASYNC_COMMIT(true, false),            // L
   ASYNC_ROLLBACK(true, false),              // L
   NUM_ASYNC_ROLLBACK(true, false),          // L
   ASYNC_COMPLETE_NOTIFY(true, false),       // L
   NUM_ASYNC_COMPLETE_NOTIFY(true, false),   // L
   SENT_SYNC_COMMIT(true, false), //Just for the commit, as this is the same also for prepares (which result eventually into a commit) and txCompletion, if present
   SENT_ASYNC_COMMIT(true, false),
   //Number of nodes involved stuff
   NUM_NODES_PREPARE(true, false),           //L
   NUM_NODES_COMMIT(true, false),            //L
   NUM_NODES_ROLLBACK(true, false),          //L
   NUM_NODES_COMPLETE_NOTIFY(true, false),   //L
   NUM_NODES_GET(true, false),               //L

   //Additional Stats
   TBC_EXECUTION_TIME(true, false),
   TBC_COUNT(true, false),
   TBC(false, false), //Time between operations in Transaction   // ONLY FOR QUERY, derived on the fly

   NTBC_EXECUTION_TIME(true, false),
   NTBC_COUNT(true, false),
   NTBC(false, false), //Time between Transactions in a thread   // ONLY FOR QUERY, derived on the fly

   RESPONSE_TIME(true, false),

   //Service and response Times
   //Cpu demand for the local execution (till the prepare) of a local transaction
   UPDATE_TX_LOCAL_S(true, false),
   UPDATE_TX_LOCAL_R(true, false),            // L
   //Cpu demand for prepareCommand handling   for a transaction. It can be local or remote (in this case it's the validation and replay of the modifications)
   UPDATE_TX_LOCAL_PREPARE_S(true, false),
   UPDATE_TX_LOCAL_PREPARE_R(true, false),
   UPDATE_TX_REMOTE_PREPARE_S(false, true),
   UPDATE_TX_REMOTE_PREPARE_R(false, true),
   //Number of prepares. This is used to know how many xacts reach the prepare phase.
   NUM_UPDATE_TX_GOT_TO_PREPARE(true, true),
   NUM_UPDATE_TX_PREPARED(true, true), //I need this to compute the time spent by remote xacts which abort due to abort on other nodes
   //Cpu demand for CommitCommand handling for a transaction. It can be local or remote.
   UPDATE_TX_LOCAL_COMMIT_S(true, false),
   UPDATE_TX_LOCAL_COMMIT_R(true, false),     // C
   UPDATE_TX_REMOTE_COMMIT_S(false, true),
   UPDATE_TX_REMOTE_COMMIT_R(false, true),
   NUM_UPDATE_TX_LOCAL_COMMIT(true, false),
   NUM_UPDATE_TX_REMOTE_COMMIT(false, true),
   //Cpu demand for a rollbackCommand which has not to be propagated . Coordinator Side
   UPDATE_TX_LOCAL_LOCAL_ROLLBACK_S(true, false),
   UPDATE_TX_LOCAL_LOCAL_ROLLBACK_R(true, false),
   NUM_UPDATE_TX_LOCAL_LOCAL_ROLLBACK(true, false),
   //Cpu demand for a rollbackCommand which has to be propagated  . Coordinator side
   UPDATE_TX_LOCAL_REMOTE_ROLLBACK_S(true, false),
   UPDATE_TX_LOCAL_REMOTE_ROLLBACK_R(true, false),
   NUM_UPDATE_TX_LOCAL_REMOTE_ROLLBACK(true, false),
   //Cpu demand for a rollbackCommand, cohort side
   UPDATE_TX_REMOTE_ROLLBACK_S(false, true),
   UPDATE_TX_REMOTE_ROLLBACK_R(false, true),
   NUM_UPDATE_TX_REMOTE_ROLLBACK(false, true),
   //Cpu demand for the local execution of a readOnlyTransaction
   READ_ONLY_TX_LOCAL_S(true, false),
   READ_ONLY_TX_LOCAL_R(true, false),
   //Cpu demand for the execution of the prepareCommand for a read only transaction
   READ_ONLY_TX_PREPARE_S(true, false),
   READ_ONLY_TX_PREPARE_R(true, false),
   //Cpu demand for the execution of the commitCommand for a read only transaction
   READ_ONLY_TX_COMMIT_S(true, false),
   READ_ONLY_TX_COMMIT_R(true, false),
   NUM_READ_ONLY_TX_COMMIT(true, true), // C
   //Cpu demand to perform the remote read of a datum
   LOCAL_REMOTE_GET_S(true, false),
   //WC time to perform the remote read of a datum
   LOCAL_REMOTE_GET_R(true, true),            // C
   //Cpu demand to serve an incoming remote read of a datum
   REMOTE_REMOTE_GET_S(false, true),
   //WC Time to serve an incoming remote read, without waiting time in the queue
   REMOTE_REMOTE_GET_R(false, true),
   WAIT_TIME_IN_COMMIT_QUEUE(true, true),
   WAIT_TIME_IN_REMOTE_COMMIT_QUEUE(false, false), //used just for query     from customInterceptor
   NUM_WAITS_IN_COMMIT_QUEUE(true, true),
   NUM_WAITS_IN_REMOTE_COMMIT_QUEUE(false, true), //just for query
   NUM_OWNED_RD_ITEMS_IN_OK_PREPARE(true, true),
   NUM_OWNED_WR_ITEMS_IN_OK_PREPARE(true, true),
   NUM_OWNED_RD_ITEMS_IN_LOCAL_PREPARE(true, false), //just for query
   NUM_OWNED_RD_ITEMS_IN_REMOTE_PREPARE(false, true), //just for query
   NUM_OWNED_WR_ITEMS_IN_LOCAL_PREPARE(true, false), //just for query
   NUM_OWNED_WR_ITEMS_IN_REMOTE_PREPARE(false, true), //just for query
   NUM_ABORTED_TX_DUE_TO_VALIDATION(true, false),
   NUM_KILLED_TX_DUE_TO_VALIDATION(true, true),
   NUM_REMOTELY_ABORTED(true, false),
   NUM_EARLY_ABORTS(true, false),
   NUM_LOCALPREPARE_ABORTS(true, false),
   GET_OPERATION_S(true, false),
   NUM_REMOTE_REMOTE_GETS(false, true),
   NUM_WAITS_REMOTE_REMOTE_GETS(false, true),
   REMOTE_REMOTE_GET_WAITING_TIME(false, true),
   UPDATE_TX_TOTAL_R(true, false),
   UPDATE_TX_TOTAL_S(true, false),
   READ_ONLY_TX_TOTAL_R(true, false),
   READ_ONLY_TX_TOTAL_S(true, false),
   TERMINATION_COST(true, true),
   FIRST_WRITE_INDEX(true, false),

   //gmu waiting times:
   GMU_WAITING_IN_QUEUE_DUE_PENDING(true, true),
   NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING(true, true),
   GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS(true, true),
   NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS(true, true),
   GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION(true, true),
   NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION(true, true),

   //gmu waiting times for query
   GMU_WAITING_IN_QUEUE_DUE_PENDING_LOCAL(false, false),
   GMU_WAITING_IN_QUEUE_DUE_PENDING_REMOTE(false, false),
   GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_LOCAL(false, false),
   GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_REMOTE(false, false),
   GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_LOCAL(false, false),
   GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_REMOTE(false, false),
   NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING_LOCAL(false, false),
   NUM_GMU_WAITING_IN_QUEUE_DUE_PENDING_REMOTE(false, false),
   NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_LOCAL(false, false),
   NUM_GMU_WAITING_IN_QUEUE_DUE_SLOW_COMMITS_REMOTE(false, false),
   NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_LOCAL(false, false),
   NUM_GMU_WAITING_IN_QUEUE_DUE_CONFLICT_VERSION_REMOTE(false, false),
   //TO waiting times
   TO_GMU_PREPARE_COMMAND_RTT_NO_WAIT(true,false),
   NUM_TO_GMU_PREPARE_COMMAND_RTT_NO_WAITED(true,false),
   TO_GMU_PREPARE_COMMAND_NODES_WAITED(true, false),
   NUM_TO_GMU_PREPARE_COMMAND_AT_LEAST_ONE_WAIT(true, false),
   TO_GMU_PREPARE_COMMAND_RTT_MINUS_AVG(true, false),
   TO_GMU_PREPARE_COMMAND_RTT_MINUS_MAX(true, false),
   TO_GMU_PREPARE_COMMAND_MAX_WAIT_TIME(true, false),
   TO_GMU_PREPARE_COMMAND_AVG_WAIT_TIME(true, false),
   TO_GMU_PREPARE_COMMAND_RESPONSE_TIME(false, true),
   TO_GMU_PREPARE_COMMAND_SERVICE_TIME(false, true),
   NUM_TO_GMU_PREPARE_COMMAND_SERVED(false, true),
   TO_GMU_PREPARE_COMMAND_REMOTE_WAIT(false, true),
   NUM_TO_GMU_PREPARE_COMMAND_REMOTE_WAITED(false, true);

   public static final int NO_INDEX = -1;
   private static short localStatsSize = 0;
   private static short remoteStatsSize = 0;
   private final boolean local;
   private final boolean remote;
   private short localIndex = NO_INDEX;
   private short remoteIndex = NO_INDEX;

   ExposedStatistic(boolean local, boolean remote) {
      this.local = local;
      this.remote = remote;
   }

   public final int getLocalIndex() {
      return localIndex;
   }

   public final int getRemoteIndex() {
      return remoteIndex;
   }

   public final boolean isLocal() {
      return local;
   }

   public final boolean isRemote() {
      return remote;
   }

   public static int getRemoteStatsSize() {
      return remoteStatsSize;
   }

   public static int getLocalStatsSize() {
      return localStatsSize;
   }

   static {
      for (ExposedStatistic stat : values()) {
         if (stat.local) {
            stat.localIndex = localStatsSize++;
         }
         if (stat.remote) {
            stat.remoteIndex = remoteStatsSize++;
         }
      }
   }
}
