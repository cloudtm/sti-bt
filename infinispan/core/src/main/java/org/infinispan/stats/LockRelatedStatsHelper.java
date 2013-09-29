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

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Diego Didona, didona@gsd.inesc-id.pt Date: 21/03/13
 */
public class LockRelatedStatsHelper {

   private static final Log log = LogFactory.getLog(LockRelatedStatsHelper.class);
   private static boolean enabled = false;

   public static void enable() {
      log.trace("Enabling the LockRelatedStatsHelper. Using GMU, then!");
      enabled = true;
   }

   /**
    * When collecting stats for locks, not always these can be taken upon visiting the Commit/RollbackCommand (or
    * Prepare for 1PC) In some cases the locks are released when the relevant xact does not exist anymore (e.g., in GMU
    * async commit or whenever the locks are released through the TxCompletionNotificationCommand, e.g., RR with sync
    * Commit)
    *
    * @param conf     configuration of the cache
    * @param isCommit true if the transaction relevant to the locks is committing
    * @param isRemote true if the transaction relevant to the locks is remote
    * @return true if locks statistics cannot be sampled right away
    */
   //TODO: consider also recovery=true/false             -->now only considering recovery =  false
   public static boolean shouldAppendLocks(Configuration conf, boolean isCommit, boolean isRemote) {
      if (!enabled) {
         if (log.isTraceEnabled())
            log.trace("LockRelatedHelper not enabled. Returning FALSE");
         return false;
      }
      boolean isGmu = conf.versioning().scheme().equals(VersioningScheme.GMU);
      boolean isSyncCommit = conf.transaction().syncCommitPhase();
      if (log.isTraceEnabled())
         log.trace("LockRelatedHelper enabled. " + (isGmu ? " GMU is on" : "RR") + (isSyncCommit ? " sync commit" : " async commit") + (isRemote ? "remote" : "local"));
      return isRemote && isGmu && isCommit && !isSyncCommit;
   }

   public static boolean maybePendingLocks(GlobalTransaction lockOwner) {
      return enabled && lockOwner.isRemote();
   }

}
