package org.radargun.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 21/05/12
 *
 * We could use the same approach Galder uses in Radargun, which is much more elegant and, I think, portable
 * This approach involves explicit command-line invocations and relies on the presence of the proc fs
 */
public class ProcCpuStat implements CpuStat{

   private static final Log log = LogFactory.getLog(ProcCpuStat.class);

   private static final String[] COMMANDS = {"cat", "/proc/stat"};

   private static final int USER = 0;
   private static final int NICE = 1;
   private static final int SYSTEM = 2;
   private static final int IDLE = 3;

   private final boolean active;

   private long[] cpuTimes;

   public ProcCpuStat() {
      //check if the file exits
      File f = new File(COMMANDS[1]);
      active = f.exists();
      if (active) {
         this.cpuTimes = this.parseCpuTime();
      } else {
         log.warn("File " + COMMANDS[1] + "does not exists. Not CPU stats will be collected");
      }
   }

   public final double getCpuUsage() {
      if (!active) {
         return -1D;
      }
      long[] current = this.parseCpuTime();
      return this.getCpuUsage(current);
   }

   public final double getCpuUsageAndReset(){
      if (!active) {
         return -1D;
      }
      double ret = this.getCpuUsage();
      this.reset();
      return ret;
   }

   public final void reset(){
      if (!active) {
         return ;
      }
      this.cpuTimes = this.parseCpuTime();
   }

   private double getCpuUsage(long[] usages) {

      long[] temp = new long[4];

      //obtain usages relevant to last monitoring window
      for (int i = 0; i < 4; i++) {
         temp[i] = usages[i] - this.cpuTimes[i];
      }

      double cpuUsage = (temp[USER] + temp[NICE] + temp[SYSTEM]);
      double totalCpuTime = cpuUsage + temp[IDLE];

      return cpuUsage / totalCpuTime;
   }

   private long[] parseCpuTime() {
      Runtime rt = Runtime.getRuntime();
      String[] temp;
      long[] ret = new long[4];
      try {
         Process p = rt.exec(COMMANDS);
         java.io.BufferedReader stdInput = new java.io.BufferedReader(new java.io.InputStreamReader(p.getInputStream()));
         String actual = stdInput.readLine();

         temp = actual.split(" ");

         //The output has two spaces after the first token!!!
         for (int i = 2; i < 6; i++) {
            ret[i - 2] = Long.parseLong(temp[i]);
         }
      } catch (IOException ioe) {
         log.warn("Exception caught when parsing the CPU time: " + ioe.getMessage());
      }
      return ret;
   }
}
