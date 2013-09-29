package org.radargun.tpcc.domain;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.tpcc.DomainObject;
import org.radargun.tpcc.TpccTools;

import java.io.Serializable;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 */
public class Warehouse implements Serializable, DomainObject {

   private long w_id;

   private String w_name;

   private String w_street1;

   private String w_street2;

   private String w_city;

   private String w_state;

   private String w_zip;

   private double w_tax;

    private double w_ytd;


   public Warehouse() {

   }

   public Warehouse(CacheWrapper wrapper, int nodeIndex, long w_id, String w_name, String w_street1, String w_street2, String w_city, String w_state, String w_zip, double w_tax, double w_ytd) {
      this.w_id = w_id;
      this.w_name = w_name;
      this.w_street1 = w_street1;
      this.w_street2 = w_street2;
      this.w_city = w_city;
      this.w_state = w_state;
      this.w_zip = w_zip;
      this.w_tax = w_tax;
      this.w_ytd = w_ytd;
      // setW_ytd(wrapper, nodeIndex, w_ytd);
   }

   public void setW_id(long w_id) {
      this.w_id = w_id;
   }

   public void setW_name(String w_name) {
      this.w_name = w_name;
   }

   public void setW_street1(String w_street1) {
      this.w_street1 = w_street1;
   }

   public void setW_street2(String w_street2) {
      this.w_street2 = w_street2;
   }

   public void setW_city(String w_city) {
      this.w_city = w_city;
   }

   public void setW_state(String w_state) {
      this.w_state = w_state;
   }

   public void setW_zip(String w_zip) {
      this.w_zip = w_zip;
   }

   public void setW_tax(double w_tax) {
      this.w_tax = w_tax;
   }

   public void setW_ytd(double w_ytd) {
       this.w_ytd = w_ytd;
//      TpccTools.put(wrapper, wrapper.createKey(getKeyW_ytd(), nodeIndex), w_ytd);
   }
   
//   public void incW_ytd(CacheWrapper wrapper, int nodeIndex, double inc) {
//       LocatedKey key = wrapper.createKey(getKeyW_ytd(), nodeIndex);
//       double newValue = ((Double) wrapper.getDelayed(key)) + inc;
//       wrapper.putDelayed(key, newValue);
//   }
   
   public String getKeyW_ytd() {
       return this.getKey() + ":w_ytd";
   }
   
   public long getW_id() {

      return w_id;
   }

   public String getW_name() {
      return w_name;
   }

   public String getW_street1() {
      return w_street1;
   }

   public String getW_street2() {
      return w_street2;
   }

   public String getW_city() {
      return w_city;
   }

   public String getW_state() {
      return w_state;
   }

   public String getW_zip() {
      return w_zip;
   }

   public double getW_tax() {
      return w_tax;
   }

   public Double getW_ytd() {
      //return TpccTools.get(wrapper, wrapper.createKey(getKeyW_ytd(), nodeIndex));
       return w_ytd;
   }

   private String getKey() {
      return "WAREHOUSE_" + this.w_id;
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
   public boolean load(CacheWrapper wrapper, int nodeIndex) throws Throwable {

      Warehouse loaded = (Warehouse) wrapper.get(null, wrapper.createKey(this.getKey(), nodeIndex));

      if (loaded == null) return false;

      this.w_city = loaded.w_city;
      this.w_name = loaded.w_name;
      this.w_state = loaded.w_state;
      this.w_street1 = loaded.w_street1;
      this.w_street2 = loaded.w_street2;
      this.w_tax = loaded.w_tax;
      this.w_zip = loaded.w_zip;

      return true;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Warehouse warehouse = (Warehouse) o;

      if (w_id != warehouse.w_id) return false;
      if (Double.compare(warehouse.w_tax, w_tax) != 0) return false;
      if (w_city != null ? !w_city.equals(warehouse.w_city) : warehouse.w_city != null) return false;
      if (w_name != null ? !w_name.equals(warehouse.w_name) : warehouse.w_name != null) return false;
      if (w_state != null ? !w_state.equals(warehouse.w_state) : warehouse.w_state != null) return false;
      if (w_street1 != null ? !w_street1.equals(warehouse.w_street1) : warehouse.w_street1 != null) return false;
      if (w_street2 != null ? !w_street2.equals(warehouse.w_street2) : warehouse.w_street2 != null) return false;
      if (w_zip != null ? !w_zip.equals(warehouse.w_zip) : warehouse.w_zip != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result;
      long temp;
      result = (int) (w_id ^ (w_id >>> 32));
      result = 31 * result + (w_name != null ? w_name.hashCode() : 0);
      result = 31 * result + (w_street1 != null ? w_street1.hashCode() : 0);
      result = 31 * result + (w_street2 != null ? w_street2.hashCode() : 0);
      result = 31 * result + (w_city != null ? w_city.hashCode() : 0);
      result = 31 * result + (w_state != null ? w_state.hashCode() : 0);
      result = 31 * result + (w_zip != null ? w_zip.hashCode() : 0);
      temp = w_tax != +0.0d ? Double.doubleToLongBits(w_tax) : 0L;
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      return result;
   }

}
