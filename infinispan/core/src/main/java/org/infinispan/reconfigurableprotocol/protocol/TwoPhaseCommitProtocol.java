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
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.EnumMap;

import static org.infinispan.interceptors.InterceptorChain.InterceptorType;

/**
 * Represents the switch protocol when Two Phase Commit is in use
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TwoPhaseCommitProtocol extends ReconfigurableProtocol {

   public static final String UID = "2PC";
   private static final String TO_UID = TotalOrderCommitProtocol.UID;
   private static final String PB_UID = PassiveReplicationCommitProtocol.UID;
   private static final String ACK = "_ACK_";
   private final AckCollector ackCollector = new AckCollector();
   private TransactionTable transactionTable;

   @Override
   public final String getUniqueProtocolName() {
      return UID;
   }

   @Override
   public final boolean canSwitchTo(ReconfigurableProtocol protocol) {
      return PB_UID.equals(protocol.getUniqueProtocolName()) ||
            TO_UID.endsWith(protocol.getUniqueProtocolName());
   }

   @Override
   public final void switchTo(ReconfigurableProtocol protocol) {
      if (TO_UID.equals(protocol.getUniqueProtocolName())) {
         try {
            awaitUntilLocalCommittingTransactionsFinished();
         } catch (InterruptedException e) {
            //no-op
         }
      }
      manager.unsafeSwitch(protocol);
      new SendAckThread().start();
   }

   @Override
   public final void stopProtocol(boolean abortOnStop) throws InterruptedException {
      globalStopProtocol(false, abortOnStop);
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
      if (PB_UID.equals(currentProtocol.getUniqueProtocolName())) {
         return;
      } else if (TO_UID.equals(currentProtocol.getUniqueProtocolName())) {
         if (affectedKeys == null) {
            //commit or rollback
            return;
         }
      }

      /*RemoteTransaction remoteTransaction = transactionTable.getRemoteTransaction(globalTransaction);
      if (remoteTransaction.check2ndPhaseAndPrepare()) {
         transactionTable.remoteTransactionRollback(globalTransaction);
      }*/

      throwOldTxException(globalTransaction);
   }

   @Override
   public final void processSpeculativeTransaction(GlobalTransaction globalTransaction, Object[] affectedKeys,
                                                   ReconfigurableProtocol oldProtocol) {
      logProcessSpeculativeTransaction(globalTransaction, oldProtocol);
      if (PB_UID.equals(oldProtocol.getUniqueProtocolName())) {
         return;
      }

      /*RemoteTransaction remoteTransaction = transactionTable.getRemoteTransaction(globalTransaction);
      if (remoteTransaction.check2ndPhaseAndPrepare()) {
         transactionTable.remoteTransactionRollback(globalTransaction);
      }*/

      throwSpeculativeTxException(globalTransaction);
   }

   @Override
   public final void bootstrapProtocol() {
      this.transactionTable = getComponent(TransactionTable.class);
   }

   @Override
   public final EnumMap<InterceptorType, CommandInterceptor> buildInterceptorChain() {
      return buildDefaultInterceptorChain();
   }

   @Override
   public final boolean use1PC(LocalTransaction localTransaction) {
      return Configurations.isOnePhaseCommit(configuration) || is1PcForAutoCommitTransaction(localTransaction);
   }

   @Override
   public final boolean useTotalOrder() {
      return false;
   }

   @Override
   protected final void internalHandleData(Object data, Address from) {
      if (ACK.equals(data)) {
         ackCollector.ack(from);
      }
   }

   private boolean is1PcForAutoCommitTransaction(LocalTransaction localTransaction) {
      return configuration.transaction().use1PcForAutoCommitTransactions() && localTransaction.isImplicitTransaction();
   }

   /**
    * Asynchronously sends the Ack when all local transactions are finished
    */
   private class SendAckThread extends Thread {

      public SendAckThread() {
         super("2PC-Send-Ack-Thread");
      }

      @Override
      public void run() {
         broadcastData(ACK, false);
         try {
            ackCollector.awaitAllAck();
            awaitUntilRemoteTransactionsFinished();
         } catch (InterruptedException e) {
            //no-op
         }
         manager.safeSwitch(null);
      }
   }
}
