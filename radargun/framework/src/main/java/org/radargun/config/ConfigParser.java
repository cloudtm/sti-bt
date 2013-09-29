package org.radargun.config;

/**
 * Abstracts the logic of parsing an configuration file.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class ConfigParser {

   public abstract MasterConfig parseConfig(String config) throws Exception;

   public static ConfigParser getConfigParser() {
      if (System.getProperties().contains("radargun.oldLauncher")) {
         return new JaxbConfigParser();
      }
      return new DomConfigParser();
   }
}
