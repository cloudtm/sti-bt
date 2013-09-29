package org.radargun.utils;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 22/05/12
 */
public interface CpuStat  {

   double getCpuUsage();
   double getCpuUsageAndReset();
   void reset() throws Exception;
}
