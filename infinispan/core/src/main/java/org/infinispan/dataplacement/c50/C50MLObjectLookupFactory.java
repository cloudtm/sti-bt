/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.dataplacement.c50;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.dataplacement.SegmentMapping;
import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.c50.keyfeature.KeyFeatureManager;
import org.infinispan.dataplacement.c50.lookup.BloomFilter;
import org.infinispan.dataplacement.c50.tree.DecisionTree;
import org.infinispan.dataplacement.c50.tree.DecisionTreeBuilder;
import org.infinispan.dataplacement.c50.tree.DecisionTreeParser;
import org.infinispan.dataplacement.c50.tree.ParseTreeNode;
import org.infinispan.dataplacement.lookup.ObjectLookup;
import org.infinispan.dataplacement.lookup.ObjectLookupFactory;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Object Lookup Factory when Machine Learner (C5.0) and Bloom Filters technique is used
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class C50MLObjectLookupFactory implements ObjectLookupFactory {

   public static final String LOCATION = "location";
   public static final String KEY_FEATURE_MANAGER = "keyFeatureManager";
   public static final String BF_FALSE_POSITIVE = "bfFalsePositiveProb";
   private static final String INPUT_FORMAT = "%1$sinput-%2$s";
   private static final String INPUT_ML_DATA_FORMAT = INPUT_FORMAT + ".data";
   private static final String INPUT_ML_NAMES_FORMAT = INPUT_FORMAT + ".names";
   private static final String INPUT_ML_TREE_FORMAT = INPUT_FORMAT + ".tree";
   private static final String EXEC_FORMAT = "%1$sc5.0 -f " + INPUT_FORMAT;
   private static final Log log = LogFactory.getLog(C50MLObjectLookupFactory.class);
   private final Map<String, Feature> featureMap;
   private KeyFeatureManager keyFeatureManager;
   private String machineLearnerPath = System.getProperty("user.dir");
   private double bloomFilterFalsePositiveProbability = 0.001;

   public C50MLObjectLookupFactory() {
      featureMap = new HashMap<String, Feature>();
   }

   @Override
   public void setConfiguration(Configuration configuration) {
      TypedProperties typedProperties = configuration.dataPlacement().properties();

      machineLearnerPath = typedProperties.getProperty(LOCATION, machineLearnerPath);
      if (!machineLearnerPath.endsWith(File.separator)) {
         machineLearnerPath += File.separator;
      }

      String keyFeatureManagerClassName = typedProperties.getProperty(KEY_FEATURE_MANAGER, null);

      if (keyFeatureManagerClassName == null) {
         throw new IllegalStateException("Key Feature Manager cannot be null");
      }

      keyFeatureManager = Util.getInstance(keyFeatureManagerClassName, Thread.currentThread().getContextClassLoader());

      if (keyFeatureManager == null) {
         throw new IllegalStateException("Key Feature Manager cannot be null");
      }

      try {
         String tmp = typedProperties.getProperty(BF_FALSE_POSITIVE, "0.001");
         bloomFilterFalsePositiveProbability = Double.parseDouble(tmp);
      } catch (NumberFormatException nfe) {
         log.warnf("Error parsing bloom filter false positive probability. The value is %s. %s",
                   bloomFilterFalsePositiveProbability, nfe.getMessage());
      }

      for (Feature feature : keyFeatureManager.getAllKeyFeatures()) {
         featureMap.put(feature.getName(), feature);
      }
   }

   @Override
   public void init(Collection<ObjectLookup> objectLookupCollection) {
      for (ObjectLookup objectLookup : objectLookupCollection) {
         if (objectLookup instanceof C50MLObjectLookup) {
            ((C50MLObjectLookup) objectLookup).setKeyFeatureManager(keyFeatureManager);
         }
      }
   }

   @Override
   public ObjectLookup createObjectLookup(SegmentMapping segmentMapping, int numberOfOwners) {
      List<Object> keys = new LinkedList<Object>();
      for (Iterator<SegmentMapping.KeyOwners> iterator = segmentMapping.iterator(); iterator.hasNext(); ) {
         keys.add(iterator.next().getKey());
      }
      BloomFilter bloomFilter = createBloomFilter(keys);
      C50MLObjectLookup objectLookup = new C50MLObjectLookup(numberOfOwners, bloomFilter);
      objectLookup.setKeyFeatureManager(keyFeatureManager);
      deleteAll();

      for (int iteration = 0; iteration < numberOfOwners; ++iteration) {
         Set<Integer> ownersIndexes = new TreeSet<Integer>();
         boolean success = writeObjectsToInputData(segmentMapping.iterator(), ownersIndexes, iteration);

         if (!success) {
            log.errorf("Cannot create Object Lookup. Error writing input.data");
            return null;
         }

         success = writeInputNames(ownersIndexes, iteration);

         if (!success) {
            log.errorf("Cannot create Object Lookup. Error writing input.name");
            return null;
         }

         try {
            runMachineLearner(iteration);
         } catch (IOException e) {
            log.errorf(e, "Error while trying to executing the Machine Learner");
            return null;
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
         }

         ParseTreeNode root;
         try {
            root = DecisionTreeParser.parse(String.format(INPUT_ML_TREE_FORMAT, machineLearnerPath, iteration));
         } catch (Exception e) {
            log.errorf(e, "Error parsing Machine Learner tree");
            return null;
         }

         DecisionTree tree = DecisionTreeBuilder.build(root, featureMap);
         objectLookup.setDecisionTreeList(iteration, tree);
      }

      return objectLookup;
   }

   @Override
   public int getNumberOfQueryProfilingPhases() {
      return 3;
   }

   /**
    * returns the bloom filter with the objects to move encoding on it
    *
    * @param objectsToMove the objects to move
    * @return the bloom filter
    */
   private BloomFilter createBloomFilter(Collection<Object> objectsToMove) {
      return new BloomFilter(objectsToMove, bloomFilterFalsePositiveProbability);
   }

   /**
    * it starts the machine learner and blocks until the process ends
    *
    * @param iteration the iteration number
    * @throws java.io.IOException  if an error occurs when launch the process
    * @throws InterruptedException if interrupted while waiting
    */
   private void runMachineLearner(int iteration) throws IOException, InterruptedException {
      Process process = Runtime.getRuntime()
            .exec(String.format(EXEC_FORMAT, machineLearnerPath, iteration));
      if (process != null) {
         process.getOutputStream();
         //this is needed because the process can block if the input stream buffer gets full
         while (process.getInputStream().read() != -1) {}
         process.waitFor();
      }
   }

   /**
    * writes the input.name files needed to run the machine leaner
    *
    * @param possibleReturnValues the possible values of the decision
    * @param iteration            the iteration number
    * @return true if the file was correctly written, false otherwise
    */
   private boolean writeInputNames(Collection<Integer> possibleReturnValues, int iteration) {
      BufferedWriter writer = getBufferedWriter(String.format(INPUT_ML_NAMES_FORMAT, machineLearnerPath, iteration));

      if (writer == null) {
         log.errorf("Cannot create writer when tried to write the input.names");
         return false;
      }

      try {
         writer.write("home");
         writer.newLine();
         writer.newLine();

         for (Feature feature : keyFeatureManager.getAllKeyFeatures()) {
            writeInputNames(writer, feature);
         }

         if (possibleReturnValues.isEmpty()) {
            writer.write("home: -2,-1");
         } else if (possibleReturnValues.size() == 1) {
            writer.write("home: -1,");
         } else {
            writer.write("home: ");
         }

         Iterator<Integer> iterator = possibleReturnValues.iterator();

         if (iterator.hasNext()) {
            writer.write(Integer.toString(iterator.next()));
         }

         while (iterator.hasNext()) {
            writer.write(",");
            writer.write(Integer.toString(iterator.next()));
         }
         writer.write(".");
         writer.flush();

      } catch (IOException e) {
         log.errorf("Error writing input.names. %s", e.getMessage());
         return false;
      }
      close(writer);
      return true;
   }

   /**
    * writes a single feature in the input.names
    *
    * @param writer  the writer for the file
    * @param feature the feature instance (with type, etc...)
    * @throws IOException if it cannot write in the file
    */
   private void writeInputNames(BufferedWriter writer, Feature feature) throws IOException {
      writer.write(feature.getName());
      writer.write(": ");
      String[] listOfNames = feature.getMachineLearnerClasses();

      if (listOfNames.length == 1) {
         writer.write(listOfNames[0]);
      } else {
         writer.write(listOfNames[0]);
         for (int i = 1; i < listOfNames.length; ++i) {
            writer.write(",");
            writer.write(listOfNames[i]);
         }
      }

      writer.write(".");
      writer.newLine();
      writer.flush();
   }

   /**
    * writes the input.data with the objects to move and their new owner
    *
    * @param iterator      the objects to move and new location
    * @param ownersIndexes the new owners indexes. to write in the .names file
    * @param iteration     the iteration number
    * @return true if the file was correctly wrote, false otherwise
    */
   private boolean writeObjectsToInputData(Iterator<SegmentMapping.KeyOwners> iterator, Set<Integer> ownersIndexes, int iteration) {
      BufferedWriter writer = getBufferedWriter(String.format(INPUT_ML_DATA_FORMAT, machineLearnerPath, iteration));

      if (writer == null) {
         log.errorf("Cannot create writer when tried to write the input.data");
         return false;
      }

      try {
         while (iterator.hasNext()) {
            SegmentMapping.KeyOwners keyOwners = iterator.next();
            int owner = keyOwners.getOwnerIndexes().length < iteration ?
                  keyOwners.getOwnerIndexes()[iteration] : -1;
            if (owner >= 0) {
               writeInputData(keyOwners.getKey(), owner, writer);
               ownersIndexes.add(owner);
            }
         }
      } catch (IOException e) {
         log.errorf("Error writing input.data. %s", e.getMessage());
         return false;
      } finally {
         close(writer);
      }

      return true;
   }

   /**
    * writes a single key in the input.data
    *
    * @param key       the key
    * @param nodeIndex the new owner index
    * @param writer    the writer for input.data
    * @throws IOException if it cannot write on it
    */
   private void writeInputData(Object key, Integer nodeIndex, BufferedWriter writer) throws IOException {
      Map<Feature, FeatureValue> keyFeatures = keyFeatureManager.getFeatures(key);

      for (Feature feature : keyFeatureManager.getAllKeyFeatures()) {
         FeatureValue keyFeatureValue = keyFeatures.get(feature);
         String value;
         if (keyFeatureValue == null) {
            value = "N/A";
         } else {
            value = keyFeatureValue.getValueAsString();
         }
         writer.write(value);
         writer.write(",");
      }
      writer.write(nodeIndex.toString());
      writer.newLine();
      writer.flush();

   }

   /**
    * returns a buffered writer for the file in file path
    *
    * @param filePath the file path
    * @return the buffered writer or null if the file cannot be written
    */
   private BufferedWriter getBufferedWriter(String filePath) {
      try {
         return new BufferedWriter(new FileWriter(filePath));
      } catch (IOException e) {
         log.errorf("Cannot create writer for file %s. %s", filePath, e.getMessage());
      }
      return null;
   }

   private void deleteAll() {
      try {
         Runtime.getRuntime().exec("rm " + String.format(INPUT_FORMAT, machineLearnerPath, "*"));
      } catch (IOException e) {
         log.warnf("Error deleting old files");
      }
   }

   /**
    * close closeable instance
    *
    * @param closeable the object to close
    */
   private void close(Closeable closeable) {
      try {
         closeable.close();
      } catch (IOException e) {
         log.warnf("Error closing %s. %s", closeable, e.getMessage());
      }
   }


}
