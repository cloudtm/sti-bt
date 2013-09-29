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
 * implements a switch request to infinispan
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class SwitchJmxRequest {

   private enum Option {
      PRINT_STATS("-print-stats", true),
      PRINT_STATE("-print-state", true),
      PROTOCOL("-protocol", false),
      FORCE_STOP("-force-stop", true),
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

   private static final String JMX_DOMAIN = "org.infinispan";
   private static final String DEFAULT_COMPONENT = "ReconfigurableReplicationManager";
   private static final String DEFAULT_JMX_PORT = "9998";

   private final ObjectName switchComponent;
   private final String newProtocolId;
   private final MBeanServerConnection mBeanServerConnection;
   private final boolean printStatusOnly;
   private final boolean printStateOnly;
   private final boolean forceStop;

   public static void main(String[] args) throws Exception {
      Arguments arguments = new Arguments();
      arguments.parse(args);
      arguments.validate();

      System.out.println("Options are " + arguments.printOptions());

      SwitchJmxRequest switchJmxRequest;
      if (arguments.hasOption(Option.PRINT_STATE)) {
         switchJmxRequest = SwitchJmxRequest.createPrintStateRequest(arguments.getValue(Option.JMX_HOSTNAME),
                                                                     arguments.getValue(Option.JMX_PORT),
                                                                     arguments.getValue(Option.JMX_COMPONENT));
      } else if (arguments.hasOption(Option.PRINT_STATS)) {
         switchJmxRequest = SwitchJmxRequest.createPrintStatsRequest(arguments.getValue(Option.JMX_HOSTNAME),
                                                                     arguments.getValue(Option.JMX_PORT),
                                                                     arguments.getValue(Option.JMX_COMPONENT));
      } else {
         switchJmxRequest = SwitchJmxRequest.createSwitchRequest(arguments.getValue(Option.JMX_HOSTNAME),
                                                                 arguments.getValue(Option.JMX_PORT),
                                                                 arguments.getValue(Option.JMX_COMPONENT),
                                                                 arguments.getValue(Option.PROTOCOL),
                                                                 arguments.hasOption(Option.FORCE_STOP));
      }

      switchJmxRequest.doRequest();
   }

   private SwitchJmxRequest(String component, String newProtocolId, String hostname, String port, boolean forceStop) throws Exception {
      String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
      mBeanServerConnection = connector.getMBeanServerConnection();
      switchComponent = getCacheComponent(component);
      this.newProtocolId = newProtocolId;
      this.printStatusOnly = false;
      this.printStateOnly = false;
      this.forceStop = forceStop;
   }

   private SwitchJmxRequest(String component, String hostname, String port, boolean state) throws Exception {
      String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
      mBeanServerConnection = connector.getMBeanServerConnection();
      switchComponent = getCacheComponent(component);
      this.newProtocolId = null;
      this.printStatusOnly = !state;
      this.printStateOnly = state;
      this.forceStop = false;
   }

   @SuppressWarnings("StringBufferReplaceableByString")
   private ObjectName getCacheComponent(String component) throws Exception {
      for (ObjectName name : mBeanServerConnection.queryNames(null, null)) {
         if (name.getDomain().equals(JMX_DOMAIN)) {

            if ("Cache".equals(name.getKeyProperty("type"))) {
               String cacheName = name.getKeyProperty("name");
               String cacheManagerName = name.getKeyProperty("manager");
               String objectNameString = new StringBuilder(JMX_DOMAIN)
                     .append(":type=Cache,name=")
                     .append(cacheName.startsWith("\"") ? cacheName :
                                   ObjectName.quote(cacheName))
                     .append(",manager=").append(cacheManagerName.startsWith("\"") ? cacheManagerName :
                                                       ObjectName.quote(cacheManagerName))
                     .append(",component=").append(component).toString();
               return new ObjectName(objectNameString);
            }
         }
      }
      return null;
   }

   public void doRequest() throws Exception {
      if (switchComponent == null) {
         throw new NullPointerException("Component does not exists");
      }
      if (printStateOnly) {
         Object stats = mBeanServerConnection.invoke(switchComponent, "printState", new Object[0], new String[0]);
         System.out.println();
         System.out.println(stats);
         System.out.println();
      } else if (printStatusOnly) {
         Object stats = mBeanServerConnection.invoke(switchComponent, "printSwitchAvgDurations", new Object[0], new String[0]);
         System.out.println();
         System.out.println(stats);
         System.out.println();
      } else {
         mBeanServerConnection.invoke(switchComponent, "switchTo", new Object[] {newProtocolId, forceStop},
                                      new String[] {"java.lang.String", "boolean"});
         System.out.println("Switch done!");
      }
   }

   public static SwitchJmxRequest createPrintStatsRequest(String hostname, String port, String component) throws Exception {
      return new SwitchJmxRequest(component, hostname, port, false);
   }

   public static SwitchJmxRequest createPrintStateRequest(String hostname, String port, String component) throws Exception {
      return new SwitchJmxRequest(component, hostname, port, true);
   }

   public static SwitchJmxRequest createSwitchRequest(String hostname, String port, String component, String protocol,
                                                      boolean forceStop) throws Exception {
      return new SwitchJmxRequest(component, protocol, hostname, port, forceStop);
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
         if (hasOption(Option.PRINT_STATS) || hasOption(Option.PRINT_STATE)) {
            if (!hasOption(Option.JMX_HOSTNAME)) {
               throw new IllegalArgumentException("Option " + Option.PRINT_STATS + " requires " + Option.JMX_HOSTNAME);
            }
            return;
         }
         if (!hasOption(Option.JMX_HOSTNAME)) {
            throw new IllegalArgumentException("Option " + Option.JMX_HOSTNAME + " is required");
         } else if (!hasOption(Option.PROTOCOL)) {
            throw new IllegalArgumentException("Option " + Option.PROTOCOL + " is required");
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
