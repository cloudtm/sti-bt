package org.infinispan.transaction.totalorder;

import org.infinispan.transaction.TotalOrderRemoteTransactionState;

import java.util.Collection;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
public interface TotalOrderManager {
   void ensureOrder(TotalOrderRemoteTransactionState state, Object[][] keysWriteAndRead) throws InterruptedException;

   void release(TotalOrderRemoteTransactionState state);

   Collection<TotalOrderLatch> notifyStateTransferStart(int topologyId);

   void notifyStateTransferEnd();

   boolean hasAnyLockAcquired();
}
