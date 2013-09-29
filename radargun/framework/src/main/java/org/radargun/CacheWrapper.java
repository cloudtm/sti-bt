package org.radargun;


import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.radargun.utils.TypedProperties;

/**
 * CacheWrappers wrap caching products tp provide RadarGun with a standard way of
 * accessing and manipulating a cache.
 *
 * @author Manik Surtani (manik@surtani.org)
 */
public interface CacheWrapper
{
   /**
    * Initialises the cache.  Typically this step will configure the
    * caching product with various params passed in, described in
    * benchmark.xml for a particular caching product, which is
    * usually the name or path to a config file specific to the
    * caching product being tested.
    *
    * @param config
    * @param nodeIndex
    * @param confAttributes
    */
   void setUp(String config, boolean isLocal, int nodeIndex, TypedProperties confAttributes) throws Exception;

   /**
    * This is called at the very end of all tests on this cache, and is used for clean-up
    * operations.
    */
   void tearDown() throws Exception;

   /**
    * This method is called when the framework needs to put an object in cache.  This method is treated
    * as a black box, and is what is timed, so it should be implemented in the most efficient (or most
    * realistic) way possible.
    *
    * @param bucket a bucket is a group of keys. Some implementations might ignore the bucket (e.g. InfinispanWrapper}}
    * so in order to avoid key collisions, one should make sure that the keys are unique even between different buckets.
    * @param key
    * @param value
    */
   void put(String bucket, Object key, Object value) throws Exception;

   void putIfLocal(String bucket, Object key, Object value) throws Exception;

   /**
    * @see #put(String, Object, Object)
    */
   Object get(String bucket, Object key);
   
   Object get(Object key);
   
   Object getGhost(Object key);
   
   void registerKey(Object key);
   
   void put(Object key, Object value);

   /**
    * This is called after each test type (if emptyCacheBetweenTests is set to true in benchmark.xml) and is
    * used to flush the cache.
    */
   void empty() throws Exception;

   /**
    * @return the number of members in the cache's cluster
    */
   int getNumMembers();

   /**
    * @return Some info about the cache contents, perhaps just a count of objects.
    */
   String getInfo();

   /**
    * Some caches (e.g. JBossCache with  buddy replication) do not store replicated data directlly in the main
    * structure, but use some additional structure to do this (replication tree, in the case of buddy replication).
    * This method is a hook for handling this situations.
    */
   Object getReplicatedData(String bucket, String key) throws Exception;

   /**
    * Starts a transaction against the cache node. All the put, get, empty invocations after this method returns will
    * take place in the scope of the transaction started. The transaction will be completed by invoking {@link #endTransaction(boolean)}.
    * @throws RuntimeException if a particular cache implementation does not support transactions it should throw a
    * RuntimeException to signal that.
    */
   void startTransaction(boolean isReadOnly);

   /**
    * Called in conjunction with {@link #startTransaction()} in order to complete a transaction by either committing or rolling it back.
    * @param successful commit or rollback?
    */
   void endTransaction(boolean successful);

   /**
    * returns true if the current thread is inside a transaction when the method is invoked
    *
    * @return  true if the current thread is inside a transaction when the method is invoked
    */
   boolean isInTransaction();

   /**
    * returns a map with cache dependent statistics 
    * @return the map with cache dependent statistics or an empty map of no statistics are available
    */
   Map<String, String> getAdditionalStats();

   /**
    * returns the number of keys in this cache
    * @return the number of keys in this cache
    */
   int getCacheSize();

   /**
    * return true if the replication protocol is passive replication (single master protocol!)
    * @return  true if is passive replication, false otherwise
    */
   boolean isPassiveReplication();

   /**
    * returns true if this cache wrapper is *the* master in passive replication
    * @return true if this cache wrapper is *the* master, false otherwise
    */
   boolean isTheMaster();

   /**
    * it resets the additional stats
    */
   void resetAdditionalStats();

   void clusterFormed(int expected);
   
   int getMyNode();
   
   LocatedKey createKey(String key, int node);
   
   LocatedKey createGroupingKey(String key, int group);
   
   LocatedKey createGroupingKeyWithRepl(String key, int group, int repl);
   
   Map<Object,Integer> getGroups();
   
   int getLocalGrouping();
   
   Object getMyAddress();
   
   List getAllAddresses();
   
   DEF createDEF();
   
   <T> DEFTask<T> createTask(Callable<T> callable);
   
   public <T> CallableWrapper<T> createCacheCallable(CallableWrapper<T> callable);
   
   public <T> T execDEF(CallableWrapper<T> callable, Object key) throws Exception;
   
   public void initDEF();
   
   public void addDelayed(Object key, int count);
}
