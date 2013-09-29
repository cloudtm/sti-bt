package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class OrderKeysUpdate {

   private static final String FILE_PREFIX = ".keys";

   public static void main(String[] args) {
      Log log = LogFactory.getLog(OrderKeysUpdate.class);

      if (args.length == 0) {
         log.fatal("File paths are expected as arguments");
         System.exit(1);
      }
      log.info("Start processing file in " + Arrays.asList(args));
      long start = System.currentTimeMillis();

      for (String s : args) {
         new ProcessLogFile(s).run();
      }
      log.info("Finish processing files. Duration (ms) is " + (System.currentTimeMillis() - start));
      System.exit(0);
   }

   private static class ProcessLogFile implements Runnable {

      private final String filePath;
      private final Log log = LogFactory.getLog(ProcessLogFile.class);
      private final Map<String, List<String>> allKeysUpdates;

      private ProcessLogFile(String filePath) {
         this.filePath = filePath;
         this.allKeysUpdates = new TreeMap<String, List<String>>();
      }

      @Override
      public void run() {
         log.info("Start analyzing log file in " + filePath);
         analyze();
         log.info("Start writing output to " + filePath + FILE_PREFIX);
         writeResult();
      }

      private void analyze() {
         BufferedReader reader = getBufferedReader();
         if (reader == null) {
            return;
         }

         try {
            String line;
            //11:08:56,034 TRACE [ReadCommittedEntry] Updating entry (key=KEY_0 removed=false valid=true changed=true created=true value=<value> newVersion=null]
            while ((line = reader.readLine()) != null) {
               if (!line.contains("Updating entry")) {
                  continue;
               }

               String key = getKey(line);
               String value = getValue(line);
               String version = getVersion(line);

               //if (value.length() > 10) {
               //   value = value.substring(0, 10);
               //}
               if (log.isTraceEnabled()) {
                  log.trace("Analyzing line... key=" + key + ", value=" + value + ", version=" + version);
               }

               List<String> list = allKeysUpdates.get(key);
               if (list == null) {
                  list = new LinkedList<String>();
                  allKeysUpdates.put(key, list);
               }
               list.add(value + ":" + version);
            }
         } catch (IOException e) {
            if (log.isDebugEnabled()) {
               log.debug("IOException while reading " + filePath, e);
            } else {
               log.error("IOException while reading " + filePath + ". " + e.getMessage());
            }
         } finally {
            close(reader);
         }
      }

      private void writeResult() {
         BufferedWriter writer = getBufferedWriter();
         if (writer == null) {
            return;
         }
         try {
            for (Map.Entry<String, List<String>> entry : allKeysUpdates.entrySet()) {
               writer.write(entry.getKey());
               writer.write("==>");
               for (String s : entry.getValue()) {
                  writer.write(s);
                  writer.write("|");
               }
               writer.newLine();
               writer.flush();
            }
         } catch (IOException e) {
            if (log.isDebugEnabled()) {
               log.debug("IOException while writing " + filePath + FILE_PREFIX, e);
            } else {
               log.error("IOException while reading " + filePath + FILE_PREFIX + ". " + e.getMessage());
            }
         } finally {
            close(writer);
         }
      }

      private BufferedReader getBufferedReader() {
         try {
            return new BufferedReader(new FileReader(filePath));
         } catch (FileNotFoundException e) {
            if (log.isDebugEnabled()) {
               log.debug(filePath + " not found!", e);
            } else {
               log.fatal(filePath + " not found!" + e.getMessage());
            }
         }
         return null;
      }

      private BufferedWriter getBufferedWriter() {
         try {
            return new BufferedWriter(new FileWriter(filePath + FILE_PREFIX));
         } catch (FileNotFoundException e) {
            if (log.isDebugEnabled()) {
               log.debug(filePath + " not found!", e);
            } else {
               log.fatal(filePath + " not found!" + e.getMessage());
            }
         } catch (IOException e) {
            if (log.isDebugEnabled()) {
               log.debug("IOException while opening " + filePath + FILE_PREFIX, e);
            } else {
               log.fatal("IOException while opening " + filePath + FILE_PREFIX + ". " + e.getMessage());
            }
         }
         return null;
      }

      private void close(Closeable closeable) {
         try {
            closeable.close();
         } catch (IOException e) {
            if (log.isDebugEnabled()) {
               log.debug("Error closing reader/writer for " + filePath, e);
            } else {
               log.warn("Error closing reader/writer for " + filePath + ". " + e.getMessage());
            }
         }
      }

      private String getKey(String key) {
         return getSubstringBetween("key=", " removed=", key);
      }

      private String getValue(String value) {
         String classAndHash = getSubstringBetween("value=", " newVersion=", value);
         String[] array = classAndHash.split("\\.");
         return array[array.length - 1];
      }

      private String getVersion(String version) {
         String aux = getSubstringBetween("newVersion=", "]", version);
         if (aux != null && aux.contains("version=")) {
            return getSubstringBetween("version=", "}", aux);
         }
         return aux;
      }

      private String getSubstringBetween(String begin, String end, String line) {
         int beginIdx = begin != null ? line.indexOf(begin) : 0;
         if (beginIdx == -1) {
            return null;
         }
         beginIdx += begin != null ? begin.length() : 0;

         int endIdx = end != null ? line.indexOf(end, beginIdx) : line.length();

         if (endIdx == -1) {
            return null;
         }

         if (beginIdx > endIdx || endIdx > line.length()) {
            return null;
         }

         return line.substring(beginIdx, endIdx);
      }
   }
}
