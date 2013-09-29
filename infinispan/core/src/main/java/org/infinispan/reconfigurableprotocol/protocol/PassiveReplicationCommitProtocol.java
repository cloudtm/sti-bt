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
package org.infinispan.reconfigurableprotocol.protocol;

import org.infinispan.configuration.cache.Configurations;
import org.infinispan.interceptors.PassiveReplicationInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.EnumMap;

import static org.infinispan.interceptors.InterceptorChain.InterceptorType;

/**
 * Represents the switch protocol when Passive Replication is in use
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class PassiveReplicationCommitProtocol extends ReconfigurableProtocol {

   public static final String UID = "PB";
   private static final String MASTER_ACK = "_MASTER_ACK_";
   private static final String SWITCH_TO_MASTER_ACK = "_MASTER_ACK_2_";
   private static final String TWO_PC_UID = TwoPhaseCommitProtocol.UID;
   private boolean masterAckReceived = false;

   @Override
   public final String getUniqueProtocolName() {
      return UID;
   }

   @Override
   public final boolean canSwitchTo(ReconfigurableProtocol protocol) {
      return TWO_PC_UID.equals(protocol.getUniqueProtocolName());
   }

   @Override
   public final void switchTo(ReconfigurableProtocol protocol) {
      manager.unsafeSwitch(protocol);
      if (isCoordinator()) {
         new SendMasterAckThread().start();
      }
   }

   @Override
   public final void stopProtocol(boolean abortOnStop) throws InterruptedException {
      if (isCoordinator()) {
         if (log.isDebugEnabled()) {
            log.debugf("[%s] Stop protocol in master for Passive Replication protocol. Wait until all local transactions " +
                             "are finished", Thread.currentThread().getName());
         }
         awaitUntilLocalExecutingTransactionsFinished(abortOnStop);
         awaitUntilLocalCommittingTransactionsFinished();
         broadcastData(MASTER_ACK, false);
         if (log.isDebugEnabled()) {
            log.debugf("[%s] Ack sent to the slaves. Starting new epoch", Thread.currentThread().getName());
         }
      } else {
         if (log.isDebugEnabled()) {
            log.debugf("[%s] Stop protocol in slave for Passive Replication protocol. Wait for the master ack",
                       Thread.currentThread().getName());
         }
         synchronized (this) {
            while (!masterAckReceived) {
               this.wait();
            }
            masterAckReceived = false;
         }
         if (log.isDebugEnabled()) {
            log.debugf("[%s] Ack received from master. Starting new epoch", Thread.currentThread().getName());
         }
      }
      //this wait should return immediately, because we don't have any remote transactions pending...
      //it is just to be safe
      awaitUntilRemoteTransactionsFinished();
   }

   @Override
   public final void bootProtocol() {
      //no-op
   }

   @Override
   public final void processTransaction(GlobalTransaction globalTransaction, Object[] affectedKKeys) {
      logProcessTransaction(globalTransaction);
   }

   @Override
   public final void processOldTransaction(GlobalTransaction globalTransaction, Object[] affectedKeys,
                                           ReconfigurableProtocol currentProtocol) {
      logProcessOldTransaction(globalTransaction, currentProtocol);
      if (TWO_PC_UID.equals(currentProtocol.getUniqueProtocolName())) {
         return;
      }
      throwOldTxException(globalTransaction);
   }

   @Override
   public final void processSpeculativeTransaction(GlobalTransaction globalTransaction, Object[] affectedKeys,
                                                   ReconfigurableProtocol oldProtocol) {
      logProcessSpeculativeTransaction(globalTransaction, oldProtocol);
      if (TWO_PC_UID.equals(oldProtocol.getUniqueProtocolName())) {
         return;
      }
      throwSpeculativeTxException(globalTransaction);
   }

   @Override
   public final void bootstrapProtocol() {
      //no-op
   }

   @Override
   public final EnumMap<InterceptorType, CommandInterceptor> buildInterceptorChain() {
      EnumMap<InterceptorType, CommandInterceptor> interceptors = buildDefaultInterceptorChain();

      //Custom interceptor after TxInterceptor
      interceptors.put(InterceptorType.CUSTOM_INTERCEPTOR_AFTER_TX_INTERCEPTOR,
                       createInterceptor(new PassiveReplicationInterceptor(), PassiveReplicationInterceptor.class));

      if (log.isTraceEnabled()) {
         log.tracef("Building interceptor chain for Passive Replication protocol %s", interceptors);
      }

      return interceptors;
   }

   @Override
   public final boolean use1PC(LocalTransaction localTransaction) {
      //force 1 phase commit for passive replication
      //return true;
      //original condition
      return Configurations.isOnePhaseCommit(configuration) || is1PcForAutoCommitTransaction(localTransaction) ||
            Configurations.isOnePhasePassiveReplication(configuration);
   }

   @Override
   public final boolean useTotalOrder() {
      return false;
   }

   @Override
   protected final void internalHandleData(Object data, Address from) {
      if (MASTER_ACK.equals(data)) {
         if (log.isDebugEnabled()) {
            log.debugf("Handle Master Ack message");
         }
         synchronized (this) {
            masterAckReceived = true;
            this.notifyAll();
         }
      } else if (SWITCH_TO_MASTER_ACK.equals(data)) {
         if (log.isDebugEnabled()) {
            log.debugf("Handle Switch To Master Ack message");
         }
         try {
            //just to be safe... we will not have remote transactions
            awaitUntilRemoteTransactionsFinished();
         } catch (InterruptedException e) {
            //ignore
         }
         manager.safeSwitch(null);
      }
   }

   private boolean is1PcForAutoCommitTransaction(LocalTransaction localTransaction) {
      return configuration.transaction().use1PcForAutoCommitTransactions() && localTransaction.isImplicitTransaction();
   }

   /**
    * Asynchronously sends the Ack after all local transaction has finished
    */
   private class SendMasterAckThread extends Thread {

      private SendMasterAckThread() {
         super("PB-Send-Ack-Thread");
      }

      @Override
      public void run() {
         try {
            awaitUntilLocalCommittingTransactionsFinished();
         } catch (InterruptedException e) {
            //interrupted
            return;
         }
         broadcastData(SWITCH_TO_MASTER_ACK, false);
         manager.safeSwitch(null);
      }
   }
}
