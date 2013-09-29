package org.radargun.cachewrappers.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Keeps information about the name of the component, the attributes names and display names in report of the stats
 * to be collected
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class StatisticComponent {
   private String name;
   private Map<String, String> statsName = new HashMap<String, String>();

   public StatisticComponent(String name) {
      this.name = name;
   }

   /**
    * adds a new display name and attribute name. If the display name already exists, it is replaced
    * @param displayName   the display name (in report file)
    * @param attributeName the attribute name (in exposed jmx)
    */
   public final void add(String displayName, String attributeName) {
      statsName.put(displayName, attributeName);
   }

   /**
    * returns a set of entries where the key is the display name and the value the attribute name
    * @return  a set of entries where the key is the display name and the value the attribute name
    */
   public final Set<Map.Entry<String, String>> getStats() {
      return statsName.entrySet();
   }

   /**
    * returns the name of the jmx component
    * @return  the name of the jmx component
    */
   public final String getName() {
      return name;
   }

   @Override
   public final String toString() {
      return "StatisticComponent{" +
            "name='" + name + '\'' +
            ", statsName=" + statsName +
            '}';
   }
}
