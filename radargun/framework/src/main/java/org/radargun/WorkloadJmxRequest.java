package org.radargun;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class WorkloadJmxRequest {

   private enum Option {
      HIGH_CONTENTION("-high", true),
      LOW_CONTENTION("-low", true),
      RANDOM_CONTENTION("-random", true),
      LARGE_WRITE_SET("-large-ws", true),
      PAYMENT_PERCENTAGE("-payment-percent", false),
      ORDER_STATUS_PERCENTAGE("-order-percent", false),
      NUMBER_THREADS("-nr-thread", false),
      JMX_HOSTNAME("-hostname", false),
      JMX_PORT("-port", false),
      JMX_COMPONENT("-jmx-component", false);

      private final String arg;
      private final boolean isBoolean;

      Option(String arg, boolean isBoolean) {
         if (arg == null) {
            throw new IllegalArgumentException("Null not allowed in Option name");
         }
         this.arg = arg;
         this.isBoolean = isBoolean;
      }

      public final String getArgName() {
         return arg;
      }

      public final boolean isBoolean() {
         return isBoolean;
      }

      public final String toString() {
         return arg;
      }

      public static Option fromString(String optionName) {
         for (Option option : values()) {
            if (option.getArgName().equalsIgnoreCase(optionName)) {
               return option;
            }
         }
         return null;
      }
   }

   public static enum Workload {
      HIGH("highContention"),
      LOW("lowContention"),
      RANDOM("randomContention");

      private final String methodName;

      Workload(String methodName) {
         this.methodName = methodName;
      }

      public String getMethodName() {
         return methodName;
      }
   }

   private static final String COMPONENT_PREFIX = "org.radargun:stage=";
   private static final String DEFAULT_COMPONENT = "TpccBenchmark";
   private static final String DEFAULT_JMX_PORT = "9998";

   private final ObjectName benchmarkComponent;
   private final Workload workload;
   private final MBeanServerConnection mBeanServerConnection;   
   private final int orderPercentage;
   private final int paymentPercentage;
   private final int nrThreads;

   public static void main(String[] args) throws Exception {
      Arguments arguments = new Arguments();
      arguments.parse(args);
      arguments.validate();

      System.out.println("Options are " + arguments.printOptions());

      Workload workload = null;

      if (arguments.hasOption(Option.HIGH_CONTENTION)) {
         workload = Workload.HIGH;
      } else if (arguments.hasOption(Option.LOW_CONTENTION)) {
         workload = Workload.LOW;
      } else if (arguments.hasOption(Option.RANDOM_CONTENTION)){
         workload = Workload.RANDOM;
      }

      boolean hasWorkload = workload != null;
      boolean hasThread = arguments.hasOption(Option.NUMBER_THREADS);

      WorkloadJmxRequest workloadJmxRequest;

      if (hasThread && hasWorkload) {
         workloadJmxRequest = new WorkloadJmxRequest(arguments.getValue(Option.JMX_COMPONENT),
                                                     arguments.getValue(Option.JMX_HOSTNAME),
                                                     arguments.getValue(Option.JMX_PORT),
                                                     workload,
                                                     Integer.parseInt(arguments.getValue(Option.ORDER_STATUS_PERCENTAGE)),
                                                     Integer.parseInt(arguments.getValue(Option.PAYMENT_PERCENTAGE)),                                                     
                                                     Integer.parseInt(arguments.getValue(Option.NUMBER_THREADS)));
      } else if (!hasThread && hasWorkload) {
         workloadJmxRequest = new WorkloadJmxRequest(arguments.getValue(Option.JMX_COMPONENT),
                                                     arguments.getValue(Option.JMX_HOSTNAME),
                                                     arguments.getValue(Option.JMX_PORT),
                                                     workload,
                                                     Integer.parseInt(arguments.getValue(Option.ORDER_STATUS_PERCENTAGE)),
                                                     Integer.parseInt(arguments.getValue(Option.PAYMENT_PERCENTAGE)));
      } else {
         workloadJmxRequest = new WorkloadJmxRequest(arguments.getValue(Option.JMX_COMPONENT),
                                                     arguments.getValue(Option.JMX_HOSTNAME),
                                                     arguments.getValue(Option.JMX_PORT),
                                                     Integer.parseInt(arguments.getValue(Option.NUMBER_THREADS)));
      }

      workloadJmxRequest.doRequest();
   }

   private WorkloadJmxRequest(String component, String hostname, String port, Workload workload, int orderPercentage,
                              int paymentPercentage) throws Exception {
      String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
      mBeanServerConnection = connector.getMBeanServerConnection();
      benchmarkComponent = new ObjectName(COMPONENT_PREFIX + component);
      this.workload = workload;
      this.orderPercentage = orderPercentage;
      this.paymentPercentage = paymentPercentage;
      this.nrThreads = -1;
   }

   private WorkloadJmxRequest(String component, String hostname, String port, Workload workload, int orderPercentage,
                              int paymentPercentage, int nrThreads) throws Exception {
      String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
      mBeanServerConnection = connector.getMBeanServerConnection();
      benchmarkComponent = new ObjectName(COMPONENT_PREFIX + component);
      this.workload = workload;
      this.orderPercentage = orderPercentage;
      this.paymentPercentage = paymentPercentage;
      this.nrThreads = nrThreads;
   }

   private WorkloadJmxRequest(String component, String hostname, String port, int nrThreads) throws Exception {
      String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
      mBeanServerConnection = connector.getMBeanServerConnection();
      benchmarkComponent = new ObjectName(COMPONENT_PREFIX + component);
      this.workload = null;
      this.orderPercentage = -1;
      this.paymentPercentage = -1;
      this.nrThreads = nrThreads;
   }

   public void doRequest() throws Exception {
      if (benchmarkComponent == null) {
         throw new NullPointerException("Component does not exists");
      }

      if (workload != null) {
         mBeanServerConnection.invoke(benchmarkComponent, workload.getMethodName(), 
                                      new Object[] {paymentPercentage, orderPercentage},
                                      new String[] {"int", "int"});
      }
      if (nrThreads != -1) {
         mBeanServerConnection.invoke(benchmarkComponent, "setNumberOfActiveThreads", new Object[] {nrThreads},
                                      new String[] {"int"});
      }
      System.out.println("Workload changed!");
   }

   private static class Arguments {

      private final Map<Option, String> argsValues;

      private Arguments() {
         argsValues = new EnumMap<Option, String>(Option.class);
         argsValues.put(Option.JMX_COMPONENT, DEFAULT_COMPONENT);
         argsValues.put(Option.JMX_PORT, DEFAULT_JMX_PORT);
      }

      public final void parse(String[] args) {
         int idx = 0;
         while (idx < args.length) {
            Option option = Option.fromString(args[idx]);
            if (option == null) {
               throw new IllegalArgumentException("unkown option: " + args[idx] + ". Possible options are: " +
                                                        Arrays.asList(Option.values()));
            }
            idx++;
            if (option.isBoolean()) {
               argsValues.put(option, "true");
               continue;
            }
            if (idx >= args.length) {
               throw new IllegalArgumentException("expected a value for option " + option);
            }
            argsValues.put(option, args[idx++]);
         }
      }

      public final void validate() {
         if (!hasOption(Option.JMX_HOSTNAME)) {
            throw new IllegalArgumentException("Option " + Option.JMX_HOSTNAME + " is required");
         }

         if (hasOption(Option.HIGH_CONTENTION) || hasOption(Option.LOW_CONTENTION) ||
               hasOption(Option.RANDOM_CONTENTION)) {
            if (!hasOption(Option.ORDER_STATUS_PERCENTAGE)) {
               throw new IllegalArgumentException("Option " + Option.ORDER_STATUS_PERCENTAGE + " is required");
            }
            if (!hasOption(Option.PAYMENT_PERCENTAGE)) {
               throw new IllegalArgumentException("Option " + Option.PAYMENT_PERCENTAGE + " is required");
            }
         }

         String writePercentage = argsValues.get(Option.PAYMENT_PERCENTAGE);
         if (writePercentage != null) {
            int value = Integer.parseInt(writePercentage);
            if (value < 0 || value > 100) {
               throw new IllegalArgumentException("Payment percentage should be between 0 and 100. Value is " +
                                                        writePercentage);
            }
         }

         writePercentage = argsValues.get(Option.ORDER_STATUS_PERCENTAGE);
         if (writePercentage != null) {
            int value = Integer.parseInt(writePercentage);
            if (value < 0 || value > 100) {
               throw new IllegalArgumentException("Order status percentage should be between 0 and 100. Value is " +
                                                        writePercentage);
            }
         }

         String nrThreads = argsValues.get(Option.NUMBER_THREADS);
         if (nrThreads != null) {
            int value = Integer.parseInt(nrThreads);
            if (value <= 0) {
               throw new IllegalArgumentException("Write percentage should be greater than 0. Value is " + nrThreads);
            }
         }
      }

      public final String getValue(Option option) {
         return argsValues.get(option);
      }

      public final boolean hasOption(Option option) {
         return argsValues.containsKey(option);
      }

      public final String printOptions() {
         return argsValues.toString();
      }

      @Override
      public final String toString() {
         return "Arguments{" +
               "argsValues=" + argsValues +
               '}';
      }
   }

}
