package org.infinispan.transaction;
import java.io.Serializable;

import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;


public class DEFResult<T> implements Serializable {

   private long updatedVersion;
   private boolean wrote;
   private Address node;
   private GlobalTransaction globalTx;
   private T data;

   public DEFResult(long updatedVersion, boolean wroteSomething, Address node, GlobalTransaction globalTx, T data) {
      this.updatedVersion = updatedVersion;
      this.wrote = wroteSomething;
      this.node = node;
      this.globalTx = globalTx;
      this.data = data;
   }
   
   public boolean wroteSomething() {
       return this.wrote;
   }

   public long getUpdatedVersion() {
      return this.updatedVersion;
   }

   public Address getNode() {
      return this.node;
   }

   public GlobalTransaction getGlobalTx() {
      return this.globalTx;
   }

   public T getData() {
      return this.data;
   }

}
