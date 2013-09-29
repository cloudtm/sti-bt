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
package org.infinispan.stats.topK;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.transaction.WriteSkewException;

import java.util.Map;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "StreamLibStatistics", description = "Show analytics for workload monitor")
public class StreamLibInterceptor extends BaseCustomInterceptor {

   private static final int DEFAULT_TOP_KEY = 10;
   private StreamLibContainer streamLibContainer;
   private boolean statisticEnabled = false;
   private DistributionManager distributionManager;

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {

      if (statisticEnabled && ctx.isOriginLocal()) {
         streamLibContainer.addGet(command.getKey(), isRemote(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         if (statisticEnabled && ctx.isOriginLocal()) {
            streamLibContainer.addPut(command.getKey(), isRemote(command.getKey()));
         }
         return invokeNextInterceptor(ctx, command);
      } catch (WriteSkewException wse) {
         Object key = wse.getKey();
         if (key != null && ctx.isOriginLocal()) {
            streamLibContainer.addWriteSkewFailed(key);
         }
         throw wse;
      }
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (WriteSkewException wse) {
         Object key = wse.getKey();
         if (key != null && ctx.isOriginLocal()) {
            streamLibContainer.addWriteSkewFailed(key);
         }
         throw wse;
      } finally {
         streamLibContainer.tryFlushAll();
      }
   }

   @ManagedOperation(description = "Resets statistics gathered by this component",
                     displayName = "Reset Statistics (Statistics)")
   public void resetStatistics() {
      streamLibContainer.resetAll();
   }

   @ManagedOperation(description = "Set K for the top-K values",
                     displayName = "Set K")
   public void setTopKValue(@Parameter(name = "Top-K", description = "top-Kth to return") int value) {
      streamLibContainer.setCapacity(value);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys most read remotely by this instance",
                     displayName = "Top Remote Read Keys")
   public Map<String, Long> getRemoteTopGets() {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.REMOTE_GET, DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys most read remotely by this instance",
                     displayName = "N Top Remote Read Keys")
   public Map<String, Long> getNRemoteTopGets(@Parameter(name = "N th top-key") int n) {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.REMOTE_GET, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys most read locally by this instance",
                     displayName = "Top Local Read Keys")
   public Map<String, Long> getLocalTopGets() {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.LOCAL_GET, DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys most read locally by this instance",
                     displayName = "N Top Local Read Keys")
   public Map<String, Long> getNLocalTopGets(@Parameter(name = "N th top-key") int n) {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.LOCAL_GET, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys most write remotely by this instance",
                     displayName = "Top Remote Write Keys")
   public Map<String, Long> getRemoteTopPuts() {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.REMOTE_PUT, DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys most write remotely by this instance",
                     displayName = "N Top Remote Write Keys")
   public Map<String, Long> getNRemoteTopPuts(@Parameter(name = "N th top-key") int n) {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.REMOTE_PUT, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys most write locally by this instance",
                     displayName = "Top Local Write Keys")
   public Map<String, Long> getLocalTopPuts() {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.LOCAL_PUT, DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys most write locally by this instance",
                     displayName = "N Top Local Write Keys")
   public Map<String, Long> getNLocalTopPuts(@Parameter(name = "N th top-key") int n) {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.LOCAL_PUT, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys most locked",
                     displayName = "Top Locked Keys")
   public Map<String, Long> getTopLockedKeys() {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.MOST_LOCKED_KEYS, DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys most locked",
                     displayName = "N Top Locked Keys")
   public Map<String, Long> getNTopLockedKeys(@Parameter(name = "N th top-key") int n) {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.MOST_LOCKED_KEYS, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys most contended",
                     displayName = "Top Contended Keys")
   public Map<String, Long> getTopContendedKeys() {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.MOST_CONTENDED_KEYS, DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys most contended",
                     displayName = "N Top Contended Keys")
   public Map<String, Long> getNTopContendedKeys(@Parameter(name = "N th top-key") int n) {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.MOST_CONTENDED_KEYS, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys whose lock acquisition failed by timeout",
                     displayName = "Top Keys whose Lock Acquisition Failed by Timeout")
   public Map<String, Long> getTopLockFailedKeys() {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.MOST_FAILED_KEYS, DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys whose lock acquisition failed ",
                     displayName = "N Top Keys whose Lock Acquisition Failed by Timeout")
   public Map<String, Long> getNTopLockFailedKeys(@Parameter(name = "N th top-key") int n) {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.MOST_FAILED_KEYS, n);
   }

   @ManagedAttribute(description = "Show the top " + DEFAULT_TOP_KEY + " keys whose write skew check was failed",
                     displayName = "Top Keys whose Write Skew Check was failed")
   public Map<String, Long> getTopWriteSkewFailedKeys() {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS, DEFAULT_TOP_KEY);
   }

   @ManagedOperation(description = "Show the top n keys whose write skew check was failed",
                     displayName = "N Top Keys whose Write Skew Check was failed")
   public Map<String, Long> getNTopWriteSkewFailedKeys(@Parameter(name = "N th top-key") int n) {
      return streamLibContainer.getTopKFromAsKeyString(StreamLibContainer.Stat.MOST_WRITE_SKEW_FAILED_KEYS, n);
   }

   @ManagedOperation(description = "Show the top n keys whose write skew check was failed",
                     displayName = "Top Keys whose Write Skew Check was failed")
   public void setStatisticsEnabled(@Parameter(name = "Enabled?") boolean enabled) {
      statisticEnabled = enabled;
      streamLibContainer.setActive(enabled);
   }

   @Override
   protected void start() {
      super.start();
      this.distributionManager = cache.getAdvancedCache().getDistributionManager();
      this.streamLibContainer = StreamLibContainer.getOrCreateStreamLibContainer(cache);
      setStatisticsEnabled(true);
   }

   private boolean isRemote(Object k) {
      return distributionManager != null && !distributionManager.getLocality(k).isLocal();
   }
}
