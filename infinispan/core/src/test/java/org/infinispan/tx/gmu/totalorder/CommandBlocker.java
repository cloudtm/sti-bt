package org.infinispan.tx.gmu.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.jdk8backported.ConcurrentHashMapV8;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
public class CommandBlocker extends CommandInterceptor {

   private static final Log log = LogFactory.getLog(CommandBlocker.class);
   private final ConcurrentMap<GlobalTransaction, ThreadBlock> blockedTransactions;

   public CommandBlocker() {
      blockedTransactions = new ConcurrentHashMapV8<GlobalTransaction, ThreadBlock>();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         internalWait(command.getGlobalTransaction());
      }
      return invokeNextInterceptor(ctx, command);
   }

   public final void block(GlobalTransaction globalTransaction) {
      blockedTransactions.putIfAbsent(globalTransaction, new ThreadBlock());
   }

   public final void unblock(GlobalTransaction globalTransaction) {
      ThreadBlock block = new ThreadBlock();
      ThreadBlock existing = blockedTransactions.putIfAbsent(globalTransaction, block);
      if (existing != null) {
         existing.progress.countDown();
      } else {
         block.progress.countDown();
      }
   }

   public final boolean await(GlobalTransaction globalTransaction, long timeout) throws InterruptedException {
      ThreadBlock block = new ThreadBlock();
      ThreadBlock existing = blockedTransactions.putIfAbsent(globalTransaction, block);
      if (existing != null) {
         log.debugf("Wait for %s. ThreadBlock=%s", globalTransaction, existing);
         return existing.received.await(timeout, TimeUnit.MILLISECONDS);
      } else {
         log.debugf("Wait for %s. ThreadBlock=%s", globalTransaction, block);
         return block.received.await(timeout, TimeUnit.MILLISECONDS);
      }
   }

   public final void reset() {
      for (ThreadBlock block : blockedTransactions.values()) {
         block.progress.countDown();
         block.received.countDown();
      }
      blockedTransactions.clear();
   }

   private void internalWait(GlobalTransaction globalTransaction) {
      ThreadBlock block = new ThreadBlock();
      ThreadBlock existing = blockedTransactions.putIfAbsent(globalTransaction, block);
      try {
         if (existing != null) {
            existing.received.countDown();
            log.debugf("Received %s. ThreadBlock=%s", globalTransaction, existing);
            existing.progress.await();
         } else {
            block.received.countDown();
            log.debugf("Received %s. ThreadBlock=%s", globalTransaction, block);
            block.progress.await();
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
   }

   private class ThreadBlock {
      private final CountDownLatch received = new CountDownLatch(1);
      private final CountDownLatch progress = new CountDownLatch(1);
   }
}
