package org.radargun;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * fixes the throughput for the PB protocol (or other) based on the expected write percentage value
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class FixThroughput implements Runnable {

   private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance(Locale.US);

   private static enum Header {
      THROUGHPUT("Throughput"),
      WRITE_THROUGHPUT("WriteTxThroughput"),
      READ_THROUGHPUT("ReadTxThroughput"),
      EXPECTED_WRITE_PERCENTAGE("ExpectedWritePercentage");

      private final String headerName;

      Header(String headerName) {
         this.headerName = headerName;
      }

      public static Header fromString(String headerName) {
         for (Header header : values()) {
            if (header.headerName.equals(headerName)) {
               return header;
            }
         }
         return null;
      }

      @Override
      public String toString() {
         return headerName;
      }
   }

   private final String filePath;
   private final String outputFilePath;
   private final Map<Header, Integer> headerPosition;

   public FixThroughput(String filePath) {
      this.filePath = filePath;
      outputFilePath = computeOutputFilePath(filePath);
      headerPosition = new EnumMap<Header, Integer>(Header.class);
   }

   public static void main(String[] args) {
      System.out.println("Fixing " + Arrays.asList(args));

      for (String filePath : args) {
         new FixThroughput(filePath).run();
      }

      System.out.println("Finished");
      System.exit(0);
   }

   /**
    * fix the throughput in all line. this implements the Runnable in order to support multi-thread file processing if
    * needed
    */
   @Override
   public void run() {
      BufferedReader reader = getBufferedReader();
      BufferedWriter writer = getBufferedWriter();

      if (reader == null) {
         System.err.println("null reader");
         System.exit(1);
      }

      if (writer == null) {
         System.err.println("null writer");
         System.exit(1);
      }

      String[] line = readLine(reader);
      if (line == null) {
         System.err.println("nothing to read");
         System.exit(2);
      }

      setHeaderPosition(line);
      if (!isAllHeadersPositionValid()) {
         System.err.println("some headers are missing");
         List<Header> missing = new LinkedList<Header> (Arrays.asList(Header.values()));
         missing.removeAll(headerPosition.keySet());
         System.err.println("missing headers are " + missing);
         System.exit(3);
      }

      writeLine(line, writer);

      while ((line = readLine(reader)) != null) {
         int throughput = fixThroughput(line);
         writeNumber(line, Header.THROUGHPUT, throughput);
         writeLine(line, writer);
      }

      close(reader);
      close(writer);
   }

   /**
    * returns the buffered reader for the input file
    *
    * @return  the buffered reader for the input file or null if the some error occurs (file cannot be open or it is 
    * not found)
    */
   private BufferedReader getBufferedReader() {
      try {
         return new BufferedReader(new FileReader(filePath));
      } catch (Exception e) {
         e.printStackTrace();
      }
      return null;
   }

   /**
    * returns the buffered writer for the output file
    *
    * @return  the buffered writer for the output file or null if some error occurs (file cannot be written or others)
    */
   private BufferedWriter getBufferedWriter() {
      try {
         return new BufferedWriter(new FileWriter(outputFilePath));
      } catch (Exception e) {
         e.printStackTrace();
      }
      return null;
   }

   /**
    * close a closeable instance
    *
    * @param closeable  the instance to close
    */
   private void close(Closeable closeable) {
      try {
         closeable.close();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   /**
    * reads a line from the csv file and parse it splitting its values for each position in array
    *
    * @param reader  the buffered reader for the file
    * @return        the line split
    */
   private String[] readLine(BufferedReader reader) {
      String line = null;
      try {
         line = reader.readLine();
      } catch (Exception e) {
         e.printStackTrace();
      }
      return line == null ? null :line.split(",");
   }

   /**
    * writes a lines in the output file
    *
    * @param line    the array with each position of the csv values
    * @param writer  the buffered writer
    */
   private void writeLine(String[] line, BufferedWriter writer) {
      try {
         writer.write(line[0]);
         for (int i = 1; i < line.length; ++i) {
            writer.write(",");
            writer.write(line[i]);
         }
         writer.newLine();
         writer.flush();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   /**
    * sets the headers position to get the values needed in each line
    *
    * @param line the header csv line
    */
   private void setHeaderPosition(String[] line) {
      for (int idx = 0; idx < line.length; ++idx) {
         Header header = Header.fromString(line[idx]);
         if (header != null) {
            headerPosition.put(header, idx);
         }
      }
   }

   /**
    * check if it has all the headers needed to perform the fix
    *
    * @return  true if the all headers needed are present, false otherwise
    */
   private boolean isAllHeadersPositionValid() {
      for (Header header : Header.values()) {
         if (!headerPosition.containsKey(header)) {
            return false;
         }
      }
      return true;
   }

   /**
    * fixes the throughput for this line
    *
    * @param line the csv line
    * @return     the new throughput value
    */
   private int fixThroughput(String[] line) {
      Number expectedWritePercentage = readNumber(line, Header.EXPECTED_WRITE_PERCENTAGE);

      if (expectedWritePercentage == null) {
         Number throughput = readNumber(line, Header.THROUGHPUT);
         return throughput == null ? 0 : throughput.intValue();
      } else {

         Number writeThroughput = readNumber(line,  Header.WRITE_THROUGHPUT);
         Number readThroughput = readNumber(line, Header.READ_THROUGHPUT);

         if (writeThroughput == null || readThroughput == null) {
            Number throughput = readNumber(line, Header.THROUGHPUT);
            return throughput == null ? 0 : throughput.intValue();
         }

         return (int) Math.min(writeThroughput.doubleValue() / expectedWritePercentage.doubleValue(),
                               readThroughput.doubleValue() / (1 - expectedWritePercentage.doubleValue()));
      }
   }

   private Number readNumber(String[] line, Header header) {
      int idx = headerPosition.get(header);

      if (idx >= line.length) {
         return null;
      }

      String number = line[idx];
      if (number == null || number.isEmpty()) {
         return null;
      }
      try {
         return NUMBER_FORMAT.parse(number);
      } catch (Exception e) {
         return null;
      }
   }

   private void writeNumber(String[] line, Header header, Number value) {
      int idx = headerPosition.get(header);

      if (idx >= line.length) {
         return;
      }
      line[idx] = value.toString();
   }

   /**
    * calculates the output file path based on the input file path. the output file has the same name as the input file
    * but with the suffix -fix
    *
    * @param inputFilePath the input file path
    * @return              the output file path
    */
   private String computeOutputFilePath(String inputFilePath) {
      String[] array = inputFilePath.split("\\.");
      int idx = 0;
      String outputFilePath = array[idx++];

      while (idx < array.length - 1) {
         outputFilePath += ".";
         outputFilePath += array[idx++];
      }

      outputFilePath += "-fix.";
      outputFilePath += array[idx];
      return outputFilePath;
   }
}
