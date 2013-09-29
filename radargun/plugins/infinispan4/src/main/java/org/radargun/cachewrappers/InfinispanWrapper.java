package org.radargun.cachewrappers;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.radargun.utils.Utils.mBeanAttributes2String;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionCoordinator;
import org.radargun.CacheWrapper;
import org.radargun.CallableWrapper;
import org.radargun.DEF;
import org.radargun.DEFTask;
import org.radargun.LocatedKey;
import org.radargun.cachewrappers.parser.StatisticComponent;
import org.radargun.cachewrappers.parser.StatsParser;
import org.radargun.utils.TypedProperties;
import org.radargun.utils.Utils;

import com.arjuna.ats.arjuna.common.arjPropertyManager;
import com.arjuna.ats.internal.arjuna.objectstore.VolatileStore;

public class InfinispanWrapper implements CacheWrapper {
   private static final String GET_ATTRIBUTE_ERROR = "Exception while obtaining the attribute [%s] from [%s]";

   static {
      // Set up transactional stores for JBoss TS
      arjPropertyManager.getCoordinatorEnvironmentBean().setCommunicationStore(VolatileStore.class.getName());
      arjPropertyManager.getObjectStoreEnvironmentBean().setObjectStoreType(VolatileStore.class.getName());
      arjPropertyManager.getCoordinatorEnvironmentBean().setDefaultTimeout(300); //300 seconds == 5 min
   }

   private static Log log = LogFactory.getLog(InfinispanWrapper.class);
   DefaultCacheManager cacheManager;
   private Cache<Object, Object> cache;
   public static transient Cache STATIC_CACHE;
   private Cache<Object, Object> cacheNoRead;
   private Cache<Object, Object> cacheGhost;
   TransactionManager tm;
   boolean started = false;
   String config;
   private volatile boolean enlistExtraXAResource;
   Transport transport;
   Method isPassiveReplicationMethod = null;

   private final List<StatisticComponent> statisticComponents = StatsParser.parse("all-stats.xml");

   public void setUp(String config, boolean isLocal, int nodeIndex, TypedProperties confAttributes) throws Exception {
      this.config = config;
      String configFile  = confAttributes.containsKey("file") ? confAttributes.getProperty("file") : config;
      String cacheName = confAttributes.containsKey("cache") ? confAttributes.getProperty("cache") : "asdkaskd";

      log.trace("Using config file: " + configFile + " and cache name: " + cacheName);

      if (!started) {
         cacheManager = new DefaultCacheManager(configFile);
         String cacheNames = cacheManager.getDefinedCacheNames();
         if (!cacheNames.contains(cacheName))
            throw new IllegalStateException("The requested cache(" + cacheName + ") is not defined. Defined cache " +
                                                  "names are " + cacheNames);
         cache = cacheManager.getCache(cacheName);
         STATIC_CACHE = cache;
         cacheNoRead = cache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
         cacheGhost = cache.getAdvancedCache().withFlags(Flag.READ_WITHOUT_REGISTERING);
         started = true;
         tm = cache.getAdvancedCache().getTransactionManager();
         log.info("Using transaction manager: " + tm);
         transport = cacheManager.getTransport();
         try {
            isPassiveReplicationMethod = Configuration.class.getMethod("isPassiveReplication");
         } catch (Exception e) {
            //just ignore
            isPassiveReplicationMethod = null;
         }
         TransactionCoordinator.des = new DefaultExecutorService(cache);
      }
      log.debug("Loading JGroups from: " + org.jgroups.Version.class.getProtectionDomain().getCodeSource().getLocation());
      log.info("JGroups version: " + org.jgroups.Version.printDescription());
      log.info("Using config attributes: " + confAttributes);
      blockForRehashing();
      injectEvenConsistentHash(confAttributes);
   }
   
   @Override
    public int getMyNode() {
        return MagicKey.NODE_INDEX;
    }
   
   @Override
    public LocatedKey createKey(String key, int node) {
        return new MagicKey(key, node);
    }
   
   public void tearDown() throws Exception {
      List<Address> addressList = cacheManager.getMembers();
      if (started) {
         cacheManager.stop();
         log.trace("Stopped, previous view is " + addressList);
         started = false;
      }
   }

   public void put(String bucket, Object key, Object value) throws Exception {
      cache.put(key, value);
   }

   @Override
   public void putIfLocal(String bucket, Object key, Object value) throws Exception {
      AdvancedCache<Object, Object> advancedCache = cache.getAdvancedCache();
      DistributionManager distributionManager = advancedCache.getDistributionManager();
      if (distributionManager == null || distributionManager.getLocality(key).isLocal()) {
         advancedCache.withFlags(Flag.CACHE_MODE_LOCAL).put(key, value);
      }
   }

   public Object get(String bucket, Object key) {
      return cache.get(key);
   }

   public void empty() throws Exception {
      RpcManager rpcManager = cache.getAdvancedCache().getRpcManager();
      int clusterSize = 0;
      if (rpcManager != null) {
         clusterSize = rpcManager.getTransport().getMembers().size();
      }
      //use keySet().size() rather than size directly as cache.size might not be reliable
      log.info("Cache size before clear (cluster size= " + clusterSize +")" + cache.keySet().size());

      cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
      log.info("Cache size after clear: " + cache.keySet().size());
   }

   public int getNumMembers() {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      if (componentRegistry.getStatus().startingUp()) {
         log.trace("We're in the process of starting up.");
      }
      if (cacheManager.getMembers() != null) {
         log.trace("Members are: " + cacheManager.getMembers());
      }
      return cacheManager.getMembers() == null ? 0 : cacheManager.getMembers().size();
   }

   public String getInfo() {
      //Important: don't change this string without validating the ./dist.sh as it relies on its format!!
      return "Running : " + cache.getVersion() +  ", config:" + config + ", cacheName:" + cache.getName();
   }

   public Object getReplicatedData(String bucket, String key) throws Exception {
      return get(bucket, key);
   }

   @Override
   public void startTransaction(boolean isReadOnly) {
      assertTm();
      try {
         tm.begin();
         Transaction transaction = tm.getTransaction();
         if (enlistExtraXAResource) {
            transaction.enlistResource(new DummyXAResource());
         }
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public void endTransaction(boolean successful) {
      assertTm();
      try {
         if (successful)
            tm.commit();
         else
            tm.rollback();
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public boolean isInTransaction() {
      try {
         return tm != null && tm.getStatus() != Status.STATUS_NO_TRANSACTION;
      } catch (SystemException e) {
         //
      }
      return false;
   }

   private void blockForRehashing() throws InterruptedException {
      // should we be blocking until all rehashing, etc. has finished?
      long gracePeriod = MINUTES.toMillis(15);
      long giveup = System.currentTimeMillis() + gracePeriod;
      if (cache.getConfiguration().getCacheMode().isDistributed()) {
         while (!cache.getAdvancedCache().getDistributionManager().isJoinComplete() && System.currentTimeMillis() < giveup)
            Thread.sleep(200);
      }

      if (cache.getConfiguration().getCacheMode().isDistributed() && !cache.getAdvancedCache().getDistributionManager().isJoinComplete())
         throw new RuntimeException("Caches haven't discovered and joined the cluster even after " + Utils.prettyPrintMillis(gracePeriod));
   }

   private void injectEvenConsistentHash(TypedProperties confAttributes) {
      if (cache.getConfiguration().getCacheMode().isDistributed()) {
         ConsistentHash ch = cache.getAdvancedCache().getDistributionManager().getConsistentHash();
      }
   }

   private void assertTm() {
      if (tm == null) throw new RuntimeException("No configured TM!");
   }

   public void setEnlistExtraXAResource(boolean enlistExtraXAResource) {
      this.enlistExtraXAResource = enlistExtraXAResource;
   }

   @Override
   public int getCacheSize() {
      return cache.size();
   }

   @Override
   public void resetAdditionalStats() {
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      String domain = cacheManager.getGlobalConfiguration().getJmxDomain();
      for(ObjectName name : mBeanServer.queryNames(null, null)) {
         if(name.getDomain().equals(domain)) {
            tryResetStats(name, mBeanServer);
         }
      }
   }


   @Override
   public Map<String, String> getAdditionalStats() {
      Map<String, String> results = new HashMap<String, String>();
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      String cacheComponentString = getCacheComponentBaseString(mBeanServer);

      if (cacheComponentString != null) {
         saveStatsFromStreamLibStatistics(cacheComponentString, mBeanServer);

         for (StatisticComponent statisticComponent : statisticComponents) {
            getStatsFrom(cacheComponentString, mBeanServer, results, statisticComponent);
         }
      } else {
         log.info("Not collecting additional stats. Infinispan MBeans not found");
      }
      return results;
   }

   @Override
   public boolean isPassiveReplication() {
      try {
         return isPassiveReplicationMethod != null && (isPassiveReplicationWithSwitch() ||
                                                             (Boolean) isPassiveReplicationMethod.invoke(cache.getConfiguration()));
      } catch (Exception e) {
         log.debug("isPassiveReplication method not found or can't be invoked. Assuming *no* passive replication in use");
      }
      return false;
   }

   @Override
   public boolean isTheMaster() {
      return transport.isCoordinator();
   }

   private boolean isPassiveReplicationWithSwitch() {
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      String cacheComponentString = getCacheComponentBaseString(mBeanServer);

      if (cacheComponentString != null) {
         try {
            return "PB".equals(getAsStringAttribute(mBeanServer,
                                                    new ObjectName(cacheComponentString + "ReconfigurableReplicationManager"),
                                                    "currentProtocolId"));
         } catch (Exception e) {
            log.warn("Unable to check for Passive Replication protocol");
         }
      }
      return false;

   }

   //================================================= JMX STATS ====================================================

   private void tryResetStats(ObjectName component, MBeanServer mBeanServer) {
      Object[] emptyArgs = new Object[0];
      String[] emptySig = new String[0];
      try {
         log.trace("Try to reset stats in " + component);
         mBeanServer.invoke(component, "resetStatistics", emptyArgs, emptySig);
         return;
      } catch (Exception e) {
         log.debug("resetStatistics not found in " + component);
      }
      try {
         mBeanServer.invoke(component, "resetStats", emptyArgs, emptySig);
         return;
      } catch (Exception e) {
         log.debug("resetStats not found in " + component);
      }
      try {
         mBeanServer.invoke(component, "reset", emptyArgs, emptySig);
         return;
      } catch (Exception e) {
         log.debug("reset not found in " + component);
      }
      log.warn("No stats were reset for component " + component);
   }


   private String getCacheComponentBaseString(MBeanServer mBeanServer) {
      String domain = cacheManager.getGlobalConfiguration().getJmxDomain();
      for (ObjectName name : mBeanServer.queryNames(null, null)) {
         if (name.getDomain().equals(domain)) {

            if ("Cache".equals(name.getKeyProperty("type"))) {
               String cacheName = name.getKeyProperty("name");
               String cacheManagerName = name.getKeyProperty("manager");
               return new StringBuilder(domain)
                     .append(":type=Cache,name=")
                     .append(cacheName.startsWith("\"") ? cacheName :
                                   ObjectName.quote(cacheName))
                     .append(",manager=").append(cacheManagerName.startsWith("\"") ? cacheManagerName :
                                                       ObjectName.quote(cacheManagerName))
                     .append(",component=").toString();
            }
         }
      }
      return null;
   }

   private void saveStatsFromStreamLibStatistics(String baseName, MBeanServer mBeanServer) {
      try {
         ObjectName streamLibStats = new ObjectName(baseName + "StreamLibStatistics");

         if (!mBeanServer.isRegistered(streamLibStats)) {
            log.info("Not collecting statistics from Stream Lib component. It is no registered");
            return;
         }

         String filePath = "top-keys-" + transport.getAddress();

         log.info("Collecting statistics from Stream Lib component [" + streamLibStats + "] and save them in " +
                        filePath);
         log.debug("Attributes available are " +
                         mBeanAttributes2String(mBeanServer.getMBeanInfo(streamLibStats).getAttributes()));

         BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath));

         bufferedWriter.write("RemoteTopGets=" + getMapAttribute(mBeanServer, streamLibStats, "RemoteTopGets")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("LocalTopGets=" + getMapAttribute(mBeanServer, streamLibStats, "LocalTopGets")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("RemoteTopPuts=" + getMapAttribute(mBeanServer, streamLibStats, "RemoteTopPuts")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("LocalTopPuts=" + getMapAttribute(mBeanServer, streamLibStats, "LocalTopPuts")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("TopLockedKeys=" + getMapAttribute(mBeanServer, streamLibStats, "TopLockedKeys")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("TopContendedKeys=" + getMapAttribute(mBeanServer, streamLibStats, "TopContendedKeys")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("TopLockFailedKeys=" + getMapAttribute(mBeanServer, streamLibStats, "TopLockFailedKeys")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("TopWriteSkewFailedKeys=" + getMapAttribute(mBeanServer, streamLibStats, "TopWriteSkewFailedKeys")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.flush();
         bufferedWriter.close();

      } catch (Exception e) {
         log.warn("Unable to collect stats from Stream Lib Statistic component");
      }
   }

   private void getStatsFrom(String baseName, MBeanServer mBeanServer, Map<String, String> results,
                             StatisticComponent statisticComponent) {
      try {
         ObjectName objectName = new ObjectName(baseName + statisticComponent.getName());

         if (!mBeanServer.isRegistered(objectName)) {
            log.info("Not collecting statistics from [" + objectName + "]. It is not registered");
            return;
         }

         log.info("Collecting statistics from component [" + objectName + "]");
         log.debug("Attributes available are " +
                         mBeanAttributes2String(mBeanServer.getMBeanInfo(objectName).getAttributes()));
         log.trace("Attributes to be reported are " + statisticComponent.getStats());

         for (Map.Entry<String, String> entry : statisticComponent.getStats()) {
            results.put(entry.getKey(), getAsStringAttribute(mBeanServer, objectName, entry.getValue()));
         }
      } catch (Exception e) {
         log.warn("Unable to collect stats from Total Order Validator component");
      }
   }

   @SuppressWarnings("unchecked")
   private Map<Object, Object> getMapAttribute(MBeanServer mBeanServer, ObjectName component, String attr) {
      try {
         return (Map<Object, Object>) mBeanServer.getAttribute(component, attr);
      } catch (Exception e) {
         log.warn(String.format(GET_ATTRIBUTE_ERROR, attr, component));
         log.debug(e);
      }
      return Collections.emptyMap();
   }

   private String getAsStringAttribute(MBeanServer mBeanServer, ObjectName component, String attr) {
      try {
         return String.valueOf(mBeanServer.getAttribute(component, attr));
      } catch (Exception e) {
         log.warn(String.format(GET_ATTRIBUTE_ERROR, attr, component));
         log.debug(e);
      }
      return "Not_Available";
   }
   
   @Override
   public LocatedKey createGroupingKey(String key, int group) {
       return new GroupingKey(key, group);
   }
   
   @Override
    public LocatedKey createGroupingKeyWithRepl(String key, int group, int repl) {
	return new GroupingKey(key, group, repl);
    }

   private final Random random = new Random();
   
   private static transient Map<Object, Integer> groupMapping = null;
   private static transient Integer myGroup = null; 
   
   @Override
   public Map<Object, Integer> getGroups() {
	if (groupMapping == null) {
	    Map<Object, Integer> mapping = new HashMap<Object, Integer>();
	    List<Address> members = cache.getCacheManager().getTransport().getMembers();
	    ConsistentHash ch = cache.getAdvancedCache().getDistributionManager().getConsistentHash();
	    for (Address addr : members) {
		for (int i = 1; ; i++) {
		    if (ch.locatePrimaryOwner("" + i).equals(addr)) {
			mapping.put(addr, i);
			break;
		    }
		}
	    }
	    groupMapping = mapping;
	}
	return groupMapping;
   }
   
   @Override
   public int getLocalGrouping() {
	if (myGroup == null) {
	    Map<Object, Integer> groups = getGroups();
	    myGroup = groups.get(cache.getCacheManager().getTransport().getAddress());
	}
	return myGroup;
   }
   
   @Override
   public Object getMyAddress() {
       return transport.getAddress();
   }
   
   @Override
   public List getAllAddresses() {
       return transport.getMembers();
   }
   
   @Override
   public DEF createDEF() {
       return new ISPNDEF(cache);
   }
   
   @Override
   public <T> DEFTask<T> createTask(Callable<T> callable) {
       return new ISPNDEFTask(callable);
   }

   @Override
   public <T> CallableWrapper<T> createCacheCallable(CallableWrapper<T> callable) {
       return new CacheCallableWrapper<T>(callable);
   }
   
   @Override
   public <T> T execDEF(CallableWrapper<T> callable, Object key) throws Exception {
       return cache.executeDEF((CacheCallableWrapper<T>) callable, key);
   }
   
   @Override
   public void clusterFormed(int expected) {

   }

   @Override
   public Object get(Object key) {
       return this.cache.get(key);
   }

   @Override
   public Object getGhost(Object key)  {
       return this.cacheGhost.get(key);
   }

   @Override
   public void registerKey(Object key) {
       try {
	   AdvancedCache advCache = cache.getAdvancedCache();
	   javax.transaction.Transaction tx = advCache.getTransactionManager().getTransaction();
	   if (tx != null) {
               LocalTransaction localTx = advCache.getTxTable().getLocalTransaction(tx);
               if (localTx != null) {
                   localTx.addReadKey(key);
               }
           }
       } catch (SystemException e) {
	   e.printStackTrace();
	   System.exit(-1);
       }
   }

   @Override
   public void put(Object key, Object value)  {
       this.cacheNoRead.put(key, value);
   }

   @Override
   public void initDEF() {
       TransactionCoordinator.des = new DefaultExecutorService(this.cache);
   }
   
   @Override
   public void addDelayed(Object key, int count) {
       this.cache.delayedComputation(new ChangeSizeComputation((GroupingKey) key, count));
   }
}
