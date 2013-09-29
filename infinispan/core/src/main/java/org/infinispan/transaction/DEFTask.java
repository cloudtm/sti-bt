package org.infinispan.transaction;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.gmu.GMUDistributedVersion;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import javax.transaction.*;

public class DEFTask<K, V, T> implements DistributedCallable<K, V, DEFResult<T>>, Serializable {

   private CacheCallable<T> task;
   private EntryVersion version;
   private Set<Address> readFrom;
   private Cache<K, V> cache;

   public DEFTask(CacheCallable<T> task, EntryVersion version, Set<Address> readFrom) {
      this.version = version;
      this.readFrom = readFrom;
      this.task = task;
   }

   @Override
   public DEFResult<T> call() throws Exception {
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      tm.begin();
      CommitLog.forcedRemoteVersion.set(this.version);
      CommitLog.forcedReadFrom.set(this.readFrom);

      T result = null;
      try {
         result = this.task.call();
      } catch(Exception e) {
         try {
            tm.rollback();
         } catch (Exception e1) {}
         throw e;
      } finally {
         CommitLog.forcedRemoteVersion.set(null);
         CommitLog.forcedReadFrom.set(null);
      }
      TransactionXaAdapter adapter = (TransactionXaAdapter) ((DummyTransaction) tm.getTransaction()).getEnlistedResources().iterator().next();
      LocalTransaction localTx = adapter.getLocalTransaction();
      EntryVersion newVersion = localTx.getTransactionVersion();
      Address myAddr = cache.getCacheManager().getTransport().getAddress();
      GlobalTransaction global = localTx.getGlobalTransaction();
      List<WriteCommand> modifications = localTx.getModifications();
      Transaction tx = tm.suspend();
      return new DEFResult<T>(((GMUDistributedVersion) newVersion).getVersionValue(myAddr), modifications != null && !modifications.isEmpty(), myAddr, global, result);
   }

   @Override
   public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
      this.cache = cache;
      this.task.setCache(this.cache);
   }

}
