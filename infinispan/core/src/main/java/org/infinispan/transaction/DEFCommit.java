package org.infinispan.transaction;

import java.io.Serializable;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.transaction.xa.GlobalTransaction;

public class DEFCommit implements DistributedCallable, Serializable {

   private GlobalTransaction tx;
   private Cache cache;
   private EntryVersion commitVersion;
   private boolean roCommit;

   public DEFCommit(GlobalTransaction tx, EntryVersion commitVersion) {
      this.tx = tx;
      this.commitVersion = commitVersion;
   }
   
   public DEFCommit(GlobalTransaction tx, EntryVersion commitVersion, boolean roCommit) {
      this.roCommit = true;
      this.tx = tx;
      this.commitVersion = commitVersion;
   }

   @Override
   public Object call() throws Exception {
      LocalTransaction local = this.cache.getAdvancedCache().getTxTable().getLocalTransaction(tx);
      DummyTransactionManager tm = (DummyTransactionManager) this.cache.getAdvancedCache().getTransactionManager();
      javax.transaction.Transaction jpaTx = local.getTransaction();
      tm.resume(jpaTx);
      local.setTransactionVersion(commitVersion);
      try {
         if (this.roCommit) {
            tm.commit();
         } else {
            tm.commitOrder();
         }
      } finally {
         tm.suspend();
      }
      return null;
   }

   @Override
   public void setEnvironment(Cache cache, Set inputKeys) {
      this.cache = cache;
   }

}