package org.radargun.tpcc.domain;

import org.radargun.CacheWrapper;
import org.radargun.tpcc.DomainObject;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CustomerLookup implements Externalizable, DomainObject {

   private long c_w_id;

   private long c_d_id;

   private String c_last;

   private List<Long> ids;

   public CustomerLookup(){

      this.ids = null;
      this.c_d_id = -1;
      this.c_w_id = -1;

      this.c_last = null;
   }

   public CustomerLookup(String c_last, long c_w_id, long c_d_id){

      this.ids = null;
      this.c_d_id = c_d_id;
      this.c_w_id = c_w_id;

      this.c_last = c_last;
   }

   public long getC_w_id() {
      return c_w_id;
   }

   public void setC_w_id(long c_w_id) {
      this.c_w_id = c_w_id;
   }

   public long getC_d_id() {
      return c_d_id;
   }

   public void setC_d_id(long c_d_id) {
      this.c_d_id = c_d_id;
   }

   public String getC_last() {
      return c_last;
   }

   public void setC_last(String c_last) {
      this.c_last = c_last;
   }

   public List<Long> getIds() {
      return ids;
   }

   public void setIds(List<Long> ids) {
      this.ids = ids;
   }

   public void addId(long newId){

      if(this.ids == null){
         this.ids = new LinkedList<Long>();
      }

      this.ids.add(newId);
   }

   private String getKey(){
      return "CUSTOMER_LOOKUP_"+this.c_last+"_"+this.c_w_id+"_"+this.c_d_id;
   }

   @Override
   public void store(CacheWrapper wrapper, int nodeIndex) throws Throwable {
       wrapper.put(null, wrapper.createKey(this.getKey(), nodeIndex), this);
   }

   @Override
   public void storeToPopulate(CacheWrapper wrapper, int nodeIndex, boolean localOnly) throws Throwable {      
      if (localOnly) {
         wrapper.putIfLocal(null, getKey(), this);
      } else {
         store(wrapper, nodeIndex);
      }
   }

   @Override
   public boolean load(CacheWrapper wrapper, int nodeIndex)throws Throwable{

      CustomerLookup loaded=(CustomerLookup)wrapper.get(null, wrapper.createKey(this.getKey(), nodeIndex));

      if(loaded==null) return false;

      this.c_w_id = loaded.c_w_id;
      this.c_d_id = loaded.c_d_id;
      this.c_last = loaded.c_last;
      this.ids = loaded.ids;


      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (c_d_id ^ (c_d_id >>> 32));
      result = prime * result + ((c_last == null) ? 0 : c_last.hashCode());
      result = prime * result + (int) (c_w_id ^ (c_w_id >>> 32));
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      CustomerLookup other = (CustomerLookup) obj;
      if (c_d_id != other.c_d_id)
         return false;
      if (c_last == null) {
         if (other.c_last != null)
            return false;
      } else if (!c_last.equals(other.c_last))
         return false;
      if (c_w_id != other.c_w_id)
         return false;
      return true;
   }

   @Override
   public void readExternal(ObjectInput in) throws IOException,
                                                   ClassNotFoundException {

      this.c_w_id = in.readLong();
      this.c_d_id = in.readLong();
      this.c_last = in.readUTF();

      int sizeIds = in.readInt();

      if(sizeIds == 0){
         this.ids = null;
      }
      else{
         this.ids = new LinkedList<Long> ();

         for(int i =0; i<sizeIds; i++){

            this.ids.add(in.readLong());
         }
      }


   }

   @Override
   public void writeExternal(ObjectOutput out) throws IOException {

      out.writeLong(this.c_w_id);
      out.writeLong(this.c_d_id);
      out.writeUTF(this.c_last);

      if(this.ids == null || this.ids.isEmpty()){
         out.writeInt(0);
      }
      else{
         out.writeInt(this.ids.size());

         Iterator<Long> itr = this.ids.iterator();
         while(itr.hasNext()){
            out.writeLong(itr.next());
         }
      }

   }

   //Pedro
   public void save(Map<String, CustomerLookup> map) {
      if (!map.containsKey(getKey())) {
         map.put(getKey(), this);
      }
   }

   public CustomerLookup load(Map<String, CustomerLookup> map) {
      CustomerLookup customerLookup = map.get(getKey());
      return customerLookup != null ? customerLookup : this;
   }
}

