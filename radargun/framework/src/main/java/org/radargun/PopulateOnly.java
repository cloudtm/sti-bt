package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.tpcc.ThreadParallelTpccPopulation;
import org.radargun.tpcc.TpccPopulation;
import org.radargun.utils.TypedProperties;
import org.radargun.utils.Utils;

import java.io.FileReader;
import java.io.IOException;
import java.net.URLClassLoader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

/**
 * This classes is used to perform the population in the cache wrapper. The main idea is to save the data in a persistent
 * storage (possibly local) and during the benchmark, each instance loads the data from there.
 *
 * Load the data from a (local) persistent storage will allow speed-up the population phase, when using a large number
 * of instances and warehouses
 *
 * This class receives a properties file with the description of the population. The properties allowed are:
 *
 * - tpcc.numWarehouses the number of warehouses to be populated.
 * - tpcc.cLastMask     the mask used to generate non-uniformly distributed random customer last names.
 * - tpcc.olIdMask      the mask used to generate non-uniformly distributed random item numbers.
 * - tpcc.cIdMask       the mask used to generate non-uniformly distributed random customer numbers.
 * - product.name       the product name (infinispan4, etc...)
 * - product.config     the configuration file that is passed to the product
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class PopulateOnly {

   private static Log log = LogFactory.getLog(PopulateOnly.class);

   public static void main(String[] args) {

      String filePath = "conf/tpcc-gen.properties";

      if (args.length > 0) {
         filePath = args[0];
      }

      log.info("Loading population properties from " + filePath);

      PopulationProperties properties = null;
      try {
         properties = new PopulationProperties(filePath);
      } catch (IOException e) {
         log.fatal("Error loading properties from " + filePath, e);
         System.exit(1);
      } catch (Exception e) {
         log.fatal("Unknown exception occurred", e);
         System.exit(2);
      }

      int numWarehouses = properties.getNumberOfWarehouses();
      long cLastMask = properties.getCLastMask();
      long olIdMask = properties.getOlIdMask();
      long cIdMask = properties.getCIdMask();

      String product = properties.getProduct();
      String config = properties.getProductConfig();
      log.info("Create cache wrapper for " + product + " and configure it using " + config);

      CacheWrapper cacheWrapper = null;
      try {
         cacheWrapper = getCacheWrapper(product);
      } catch (Exception e) {
         log.fatal("Exception while creating the cache wrapper", e);
         System.exit(3);
      }

      try {
         cacheWrapper.setUp(config, true, 0, new TypedProperties());
      } catch (Exception e) {
         log.fatal("Exception while configuring the cache wrapper", e);
         System.exit(4);
      }

      //using transaction is faster than do always a single put... but lets set the number of threads to 1
      TpccPopulation population = new ThreadParallelTpccPopulation(cacheWrapper, numWarehouses, 0, 1,
                                                                   cLastMask, olIdMask, cIdMask, 1, 10000);

      log.info("Starting the population of " + cacheWrapper + " with " + numWarehouses + " warehouses. C_Last_Mask=" +
                     cLastMask + ", Ol_ID_Mask=" + olIdMask + ", C_ID_Mask=" + cIdMask);

      long start = System.currentTimeMillis();
      population.performPopulation();
      long duration = System.currentTimeMillis() - start;

      int cacheSize = cacheWrapper.getCacheSize();

      DateFormat df = new SimpleDateFormat("HH:mm.ss");
      df.setTimeZone(TimeZone.getTimeZone("GMT"));
      log.info("Population took " + df.format(new Date(duration)) + " and it has put " + cacheSize + " keys/entries");
      try {
         cacheWrapper.tearDown();
      } catch (Exception e) {
         log.warn("Exception while tear down the cache wrapper. " + e);
      }
   }

   public static CacheWrapper getCacheWrapper(String product) throws Exception {
      String fqnClass = Utils.getCacheWrapperFqnClass(product);
      URLClassLoader loader = Utils.buildProductSpecificClassLoader(product, PopulateOnly.class.getClassLoader());
      Thread.currentThread().setContextClassLoader(loader);
      return (CacheWrapper) loader.loadClass(fqnClass).newInstance();
   }

   private static class PopulationProperties {
      private Properties properties;

      private enum Property {
         NUM_WAREHOUSE("tpcc.numWarehouses", "1"),
         C_LAST_MASK("tpcc.cLastMask", "0"),
         OL_ID_MASK("tpcc.olIdMask", "0"),
         C_ID_MASK("tpcc.cIdMask", "0"),
         PRODUCT_NAME("product.name", null),
         PRODUCT_CONFIG("product.config", null);


         String propertyName;
         String defaultValue;

         Property(String name, String value) {
            this.propertyName = name;
            this.defaultValue = value;
         }

         public String getPropertyName() {
            return propertyName;
         }

         public String getDefaultValue() {
            return defaultValue;
         }
      }

      public PopulationProperties(String filePath) throws IOException {
         properties = new Properties();
         properties.load(new FileReader(filePath));
      }

      public int getNumberOfWarehouses() {
         return getInt(Property.NUM_WAREHOUSE);
      }

      public int getCLastMask() {
         return getInt(Property.C_LAST_MASK);
      }

      public int getOlIdMask() {
         return getInt(Property.OL_ID_MASK);
      }

      public int getCIdMask() {
         return getInt(Property.C_ID_MASK);
      }

      public String getProduct() {
         return properties.getProperty(Property.PRODUCT_NAME.getPropertyName(), Property.PRODUCT_NAME.getDefaultValue());
      }

      public String getProductConfig() {
         return properties.getProperty(Property.PRODUCT_CONFIG.getPropertyName(), Property.PRODUCT_CONFIG.getDefaultValue());
      }

      private int getInt(Property property) {
         try {
            return Integer.parseInt(properties.getProperty(property.getPropertyName(), property.getDefaultValue()));
         } catch (NumberFormatException nfe) {
            return Integer.parseInt(property.getDefaultValue());
         }
      }

   }
}