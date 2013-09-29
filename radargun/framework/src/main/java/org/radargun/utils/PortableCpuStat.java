
package org.radargun.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 22/05/12
 * Sort of copy paste from a code by Galder Zamarreno
 *
 * @deprecated it is not tested!
 */
@Deprecated
public class PortableCpuStat implements CpuStat{

   private final MBeanServerConnection mBeanServerConnection;
   private final OperatingSystemMXBean osBean;
   private final ObjectName OS_NAME;
   private final ObjectName RUNTIME_NAME;

   private long upTime,cpuTime;

   private static final Log log = LogFactory.getLog(PortableCpuStat.class);


   public PortableCpuStat() throws Exception{
      this.mBeanServerConnection = ManagementFactory.getPlatformMBeanServer();
      this.osBean = ManagementFactory.newPlatformMXBeanProxy(mBeanServerConnection, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME, OperatingSystemMXBean.class);

      try {
         OS_NAME = new ObjectName(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);
         RUNTIME_NAME = new ObjectName(ManagementFactory.RUNTIME_MXBEAN_NAME);
      } catch (MalformedObjectNameException ex) {
         throw new RuntimeException(ex);
      }
      reset();
   }


   /**
    *
    * @return cpu usage since last reset
    */
   public final double getCpuUsage() {
      try {

         long prevCpuTime = cpuTime;
         long prevUpTime = upTime;
         long procCount = this.getProcCount();

         Long jmxCpuTime = getActualCpuTime()  * this.getCpuMultiplier();
         Long jmxUpTime = getActualUpTime();

         long upTimeDiff = (jmxUpTime * 1000000) - (prevUpTime * 1000000);

         long procTimeDiff = (jmxCpuTime / procCount) - (prevCpuTime / procCount);

         long usage = upTimeDiff > 0 ? Math.min((long)  (1000 * (float) procTimeDiff / (float) upTimeDiff), 1000) : 0;
         return usage / 1000.0; //returns between 0 and 1
      } catch (Exception e) {
         log.warn("Exception caught when obtaining the CPU usage", e);
         return -1D;
      }

   }

   /**
    *
    * @return cpu usage since last reset; it also performs a reset
    */
   public final double getCpuUsageAndReset() {
      double ret = -1D;
      try{
         ret = getCpuUsage();
         reset();
      } catch(Exception e){
         log.warn("Exception caught when resetting the CPU usage", e);
      }
      return ret;
   }


   public final void reset() throws Exception {
      cpuTime = getActualCpuTime() * getCpuMultiplier();
      upTime = getActualUpTime();

   }

   private long getProcCount(){
      return osBean.getAvailableProcessors();
   }

   private long getActualCpuTime() throws Exception {
      return (Long) mBeanServerConnection.getAttribute(OS_NAME, "ProcessCpuTime");
   }

   private long getActualUpTime() throws Exception {
      return (Long) mBeanServerConnection.getAttribute(RUNTIME_NAME, "Uptime");
   }

   private long getCpuMultiplier() throws Exception {
      Number num;
      try {
         num = (Number) mBeanServerConnection.getAttribute(OS_NAME, "ProcessingCapacity");
      } catch (AttributeNotFoundException e) {
         num = 1;
      }
      return num.longValue();
   }
}
