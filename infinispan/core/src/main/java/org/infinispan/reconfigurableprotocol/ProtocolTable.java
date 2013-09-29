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
package org.infinispan.reconfigurableprotocol;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.reconfigurableprotocol.manager.ReconfigurableReplicationManager;

import javax.transaction.Transaction;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * keeps the protocol ID of the replication protocol used to execute each local transaction
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ProtocolTable {

   private final ConcurrentMap<Transaction, String> transactionToProtocol;
   private final ThreadLocal<String> protocolId = new ThreadLocal<String>();
   private ReconfigurableReplicationManager manager;

   public ProtocolTable() {
      transactionToProtocol = new ConcurrentHashMap<Transaction, String>();
   }

   @Inject
   public void inject(ReconfigurableReplicationManager manager) {
      this.manager = manager;
   }

   /**
    * returns the protocol Id to run the transaction. If it is the first time, it maps the transaction to the current
    * protocol Id
    *
    * @param transaction the transaction
    * @return the protocol Id to run the transaction
    */
   public final String getProtocolId(Transaction transaction) {
      if (transaction == null) {
         //transaction is null when the transaction is forced to complete via recovery
         try {
            return manager.beginTransaction(transaction);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
         }
      }
      String protocolId = transactionToProtocol.get(transaction);
      if (protocolId == null) {
         try {
            protocolId = manager.beginTransaction(transaction);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
         }
         transactionToProtocol.put(transaction, protocolId);
      }
      return protocolId;
   }

   /**
    * remove the transaction
    *
    * @param transaction the transaction
    */
   public final void remove(Transaction transaction) {
      if (transaction == null) {
         return;
      }
      transactionToProtocol.remove(transaction);
   }

   /**
    * returns the replication protocol to use by the thread that invokes this method
    *
    * @return the replication protocol to use by the thread that invokes this method
    */
   public final String getThreadProtocolId() {
      String pId = protocolId.get();
      if (pId == null || pId.equals("")) {
         pId = manager.getCurrentProtocolId();
      }
      return pId;
   }

   /**
    * sets the replication protocol to use by the thread that invokes this method
    *
    * @param protocolId the protocol Id
    */
   public final void setThreadProtocolId(String protocolId) {
      this.protocolId.set(protocolId);
   }

}
