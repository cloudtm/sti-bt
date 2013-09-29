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
package org.infinispan.tx.gmu.totalorder;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.tx.gmu.ConsistencyTest;
import org.infinispan.util.concurrent.TimeoutException;
import org.jgroups.blocks.Response;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;

import static junit.framework.Assert.fail;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.totalorder.TotalOrderConsistencyTest")
public class TotalOrderConsistencyTest extends ConsistencyTest {

   public void testTimeoutCleanupInLocalNode() throws Exception {
      assertAtLeastCaches(2);
      final CountDownLatch block = new CountDownLatch(1);
      final CommandInterceptor interceptor = new BaseCustomInterceptor() {
         @Override
         public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
            if (!ctx.isOriginLocal()) {
               block.await();
            }
            return invokeNextInterceptor(ctx, command);
         }
      };
      final InterceptorChain chain = TestingUtil.extractComponent(cache(0), InterceptorChain.class);
      final Object key1 = newKey(0);
      final Object key2 = newKey(1);
      try {
         chain.addInterceptor(interceptor, 0);
         tm(0).begin();
         cache(0).put(key1, VALUE_1);
         cache(0).put(key2, VALUE_1);
         tm(0).commit();
         fail("Rollback expected!");
      } catch (RollbackException e) {
         //expected
      } finally {
         block.countDown();
         chain.removeInterceptor(0);
      }

      cache(0).put(key1, VALUE_2);
      cache(0).put(key2, VALUE_2);

      assertCachesValue(0, key1, VALUE_2);
      assertCachesValue(0, key2, VALUE_2);

      assertNoTransactions();
      assertNoLocks();
   }

   public void testTimeoutCleanupInLocalNode2() throws Exception {
      assertAtLeastCaches(2);
      final CountDownLatch block = new CountDownLatch(1);
      PrepareCommandBlockerInboundInvocationHandler handler = rewireInboundInvocationHandler(cache(0), block);
      final Object key1 = newKey(0);
      final Object key2 = newKey(1);
      handler.setBlocked(true);
      try {
         tm(0).begin();
         cache(0).put(key1, VALUE_1);
         cache(0).put(key2, VALUE_1);
         tm(0).commit();
         fail("Rollback expected!");
      } catch (RollbackException e) {
         //expected
      } finally {
         handler.setBlocked(false);
         block.countDown();
      }

      cache(0).put(key1, VALUE_2);
      cache(0).put(key2, VALUE_2);

      assertCachesValue(0, key1, VALUE_2);
      assertCachesValue(0, key2, VALUE_2);

      assertNoTransactions();
      assertNoLocks();
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      super.decorate(builder);
      builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER)
            .recovery().disable();
   }

   @Override
   protected final void assertNoLocks() {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (Cache cache : caches()) {
               if (TestingUtil.extractComponent(cache, TotalOrderManager.class).hasAnyLockAcquired()) {
                  return false;
               }
            }
            return true;
         }
      });
   }

   private PrepareCommandBlockerInboundInvocationHandler rewireInboundInvocationHandler(Cache cache, CountDownLatch countDownLatch)
         throws Exception {
      GlobalComponentRegistry globalComponentRegistry = cache.getAdvancedCache().getComponentRegistry()
            .getGlobalComponentRegistry();
      InboundInvocationHandler oldHandler = globalComponentRegistry.getComponent(InboundInvocationHandler.class);
      PrepareCommandBlockerInboundInvocationHandler newHandler =
            new PrepareCommandBlockerInboundInvocationHandler(oldHandler, countDownLatch);
      globalComponentRegistry.registerComponent(newHandler, InboundInvocationHandler.class);

      JGroupsTransport t = (JGroupsTransport) globalComponentRegistry.getComponent(Transport.class);
      CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
      Field f = card.getClass().getDeclaredField("inboundInvocationHandler");
      f.setAccessible(true);
      f.set(card, newHandler);
      return newHandler;
   }

   private class PrepareCommandBlockerInboundInvocationHandler implements InboundInvocationHandler {

      private final InboundInvocationHandler handler;
      private final CountDownLatch block;
      private boolean blocked;

      private PrepareCommandBlockerInboundInvocationHandler(InboundInvocationHandler handler, CountDownLatch block) {
         this.handler = handler;
         this.block = block;
         this.blocked = false;
      }

      @Override
      public void handle(CacheRpcCommand command, Address origin, Response response) throws Throwable {
         if (isBlocked() && command instanceof PrepareCommand) {
            //to fail faster!
            response.send(new ExceptionResponse(new TimeoutException()), false);
            block.await();
         }
         handler.handle(command, origin, response);
      }

      private synchronized boolean isBlocked() {
         return blocked;
      }

      public synchronized void setBlocked(boolean blocked) {
         this.blocked = blocked;
      }
   }
}
