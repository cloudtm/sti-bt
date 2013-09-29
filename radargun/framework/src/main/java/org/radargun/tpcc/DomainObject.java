package org.radargun.tpcc;

import org.radargun.CacheWrapper;

/**
 * Represents a tpcc domain object
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public interface DomainObject {

   /**
    * it stores the domain object in the cache wrapper
    * 
    * @param wrapper    the cache wrapper
    * @param nodeIndex  the node index which stores this object               
    * @throws Throwable if something wrong occurs
    */
   void store(CacheWrapper wrapper, int nodeIndex) throws Throwable;

   /**
    * it stores the domain object in the cache wrapper
    *
    * @param wrapper    the cache wrapper
    * @param nodeIndex  the node index which stores this object               
    * @throws Throwable if something wrong occurs
    */
   void storeToPopulate(CacheWrapper wrapper, int nodeIndex, boolean localOnly) throws Throwable;

   /**
    * it loads the domain object from the cache wrapper
    * @param wrapper the cache wrapper
    * @return true if the domain object was found, false otherwise
    * @throws Throwable if something wrong occurs
    */
   boolean load(CacheWrapper wrapper, int nodeIndex) throws Throwable;
}
