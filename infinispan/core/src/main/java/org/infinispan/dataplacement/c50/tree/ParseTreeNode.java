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
package org.infinispan.dataplacement.c50.tree;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedReader;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a node in the decision tree
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ParseTreeNode {

   private static final Log log = LogFactory.getLog(ParseTreeNode.class);
   private final RuleType[] ruleTypes = new RuleType[]{
         new RuleType0V2(),
         new RuleType0(),
         new RuleType3(),
         new RuleType2(),
         new RuleType1()
   };
   private int type;
   private String clazz;
   private int[] frequency;
   private String attribute;
   private int numberOfForks;
   private String cut;
   private EltsValues[] elts;
   private ParseTreeNode[] forks;

   public ParseTreeNode() {
      type = -1;
   }

   public int getType() {
      return type;
   }

   public String getClazz() {
      return clazz;
   }

   public int[] getFrequency() {
      return frequency;
   }

   public String getAttribute() {
      return attribute;
   }

   public int getNumberOfForks() {
      return numberOfForks;
   }

   public String getCut() {
      return cut;
   }

   public EltsValues[] getElts() {
      return elts;
   }

   public ParseTreeNode[] getForks() {
      return forks;
   }

   /**
    * parses the current node from the file
    *
    * @param reader the reader for the *.tree file
    * @throws Exception if some error occurs during the parse
    */
   public void parse(BufferedReader reader) throws Exception {
      String line;
      while ((line = reader.readLine()) != null) {
         line = line.trim();

         if (line.isEmpty() || !line.startsWith("type")) {
            if (log.isTraceEnabled()) {
               log.tracef("Discarding line %s. it is not useful", line);
            }
         } else {
            break;
         }
      }
      if (line == null) {
         throw new IllegalStateException("EOF not expected!");
      }

      if (log.isTraceEnabled()) {
         log.tracef("Parsing line '%s'", line);
      }

      for (RuleType ruleType : ruleTypes) {
         if (ruleType.matches(line)) {
            ruleType.parse(reader);
            return;
         }
      }

      throw new IllegalStateException("The rule should match to one of the type");
   }

   /**
    * pretty print the tree
    *
    * @param level   the level of the node
    * @param builder the builder to put the output
    */
   public void toString(int level, StringBuilder builder) {
      if (level > 0) {
         for (int i = 0; i < level - 1; ++i) {
            builder.append("    ");
         }
         builder.append(":...");
      }
      builder.append(toString()).append("\n");
      if (forks != null) {
         for (ParseTreeNode node : forks) {
            node.toString(level + 1, builder);
         }
      }
   }

   @Override
   public String toString() {
      return "ParseTreeNode{" +
            "type=" + type +
            " class='" + clazz + '\'' +
            (frequency == null ? "" : " freq=" + freqToString()) +
            (attribute == null ? "" : " att='" + attribute + '\'') +
            (numberOfForks == 0 ? "" : " fork=" + numberOfForks) +
            (cut == null ? "" : " cut='" + cut + '\'') +
            (elts == null ? "" : " " + eltsToString()) +
            '}';
   }

   private String freqToString() {
      if (frequency == null) {
         return "";
      } else if (frequency.length == 1) {
         return Integer.toString(frequency[0]);
      }
      String s = Integer.toString(frequency[0]);
      for (int i = 1; i < frequency.length; ++i) {
         s += "," + Integer.toString(frequency[i]);
      }
      return s;
   }

   private String eltsToString() {
      if (elts == null) {
         return "";
      } else if (elts.length == 1) {
         return "elts='" + elts[0] + "'";
      }
      String s = "elts='" + elts[0] + "'";
      for (int i = 1; i < elts.length; ++i) {
         s += " elts='" + elts[i] + "'";
      }
      return s;
   }

   /**
    * parses the frequencies
    *
    * @param value the frequencies
    */
   private void parseFrequency(String value) {
      String[] freqValues = value.split(",");
      frequency = new int[freqValues.length];

      for (int i = 0; i < frequency.length; ++i) {
         frequency[i] = Integer.parseInt(freqValues[i]);
      }
   }

   /**
    * parses the elts list
    *
    * @param value the elts list
    */
   private void parseElts(String value) {
      int charIdx = 0;

      if (log.isTraceEnabled()) {
         log.tracef("Parsing elts values in '%s'", value);
      }

      for (int i = 0; i < elts.length; ++i) {
         boolean escape = false;
         boolean comma = false;
         boolean endString = false;
         elts[i] = new EltsValues();

         if (log.isTraceEnabled()) {
            log.tracef("Starting iterator %s. parsing from %s", i, value.substring(charIdx));
         }

         if (!value.startsWith("elts=", charIdx)) {
            throw new IllegalStateException("Expected 'elts=' in string");
         }
         charIdx += 5;
         if (value.charAt(charIdx) != '\"') {
            throw new IllegalStateException("Expected '\"' in string");
         }
         charIdx++;
         String etlsValue = "";
         inner:
         while (true) {
            if (log.isTraceEnabled()) {
               log.tracef("consuming... %s", value.substring(charIdx));
            }
            switch (value.charAt(charIdx)) {
               case '\\':
                  escape = true;
                  break;
               case ',':
                  if (escape) {
                     etlsValue += "\\,";
                     escape = false;
                  } else if (endString) {

                     elts[i].addValue(etlsValue);
                     comma = true;

                     if (log.isTraceEnabled()) {
                        log.tracef("adding '%s'. State: escape=%s,comma=%s,end=%s", etlsValue, escape, comma, endString);
                     }

                     etlsValue = "";
                  } else {
                     comma = true;
                  }
                  break;
               case '\"':
                  if (escape) {
                     etlsValue += "\\\"";
                     escape = false;
                  } else if (comma) {
                     comma = false; //ignore, starting a new value
                  } else {
                     endString = true;
                  }
                  break;
               case ' ':
                  if (endString) {

                     if (log.isTraceEnabled()) {
                        log.tracef("adding '%s' and ending. State: escape=%s,comma=%s,end=%s", etlsValue, escape, comma, endString);
                     }

                     elts[i].addValue(etlsValue);
                     etlsValue = "";
                     charIdx++;
                     break inner;
                  }
                  etlsValue += " ";
                  break;
               default:
                  if (escape) {
                     etlsValue += "\\";
                  }
                  escape = false;
                  comma = false;
                  endString = false;
                  etlsValue += value.charAt(charIdx);
            }
            charIdx++;
            if (charIdx >= value.length()) {
               break;
            }
         }
         if (endString && !etlsValue.isEmpty()) {
            if (log.isTraceEnabled()) {
               log.tracef("adding '%s' and ending. State: escape=%s,comma=%s,end=%s", etlsValue, escape, comma, endString);
            }
            elts[i].addValue(etlsValue);
         }
      }
   }

   public static class EltsValues {
      private final List<String> values;

      public EltsValues() {
         this.values = new LinkedList<String>();
      }

      public List<String> getValues() {
         return Collections.unmodifiableList(values);
      }

      @Override
      public String toString() {
         return "EltsValues{" +
               "values=" + values +
               '}';
      }

      private void addValue(String value) {
         values.add(value);
      }
   }

   private abstract class RuleType {

      private final Pattern pattern;
      protected Matcher matcher;

      protected RuleType(String rule) {
         this.pattern = Pattern.compile(rule);
      }

      /**
       * check if the rule matches with this pattern
       *
       * @param rule the rule
       * @return true if the rule matches to this pattern
       */
      public final boolean matches(String rule) {
         matcher = pattern.matcher(rule);

         if (log.isTraceEnabled()) {
            log.tracef("Trying to match '%s' with pattern '%s%", rule, pattern.pattern());
         }

         return matcher.matches();
      }

      /**
       * parses the rule
       *
       * @param reader the reader used to parse the child nodes (if any)
       * @throws Exception if some error occurs
       */
      abstract void parse(BufferedReader reader) throws Exception;
   }

   private class RuleType0 extends RuleType {

      public RuleType0() {
         super("type=\"0\" class=\"(.*)\"");
      }

      @Override
      public void parse(BufferedReader reader) throws Exception {
         if (log.isTraceEnabled()) {
            log.tracef("'%s' Matched with type 0: %s", matcher.group(0), matcher.pattern());
         }
         type = 0;
         clazz = matcher.group(1);
      }
   }

   private class RuleType0V2 extends RuleType {

      public RuleType0V2() {
         super("type=\"0\" class=\"(.*)\" freq=\"(.*)\"");
      }

      @Override
      public void parse(BufferedReader reader) throws Exception {
         if (log.isTraceEnabled()) {
            log.tracef("'%s' Matched with type 0: %s", matcher.group(0), matcher.pattern());
         }
         type = 0;
         clazz = matcher.group(1);
         parseFrequency(matcher.group(2));
      }
   }

   private class RuleType1 extends RuleType {

      public RuleType1() {
         super("type=\"1\" class=\"(.*)\" freq=\"(.*)\" att=\"(.*)\" forks=\"(\\d+)\"");
      }

      @Override
      public void parse(BufferedReader reader) throws Exception {
         if (log.isTraceEnabled()) {
            log.tracef("'%s' Matched with type 1: %s", matcher.group(0), matcher.pattern());
         }
         type = 1;
         clazz = matcher.group(1);
         parseFrequency(matcher.group(2));
         attribute = matcher.group(3);
         numberOfForks = Integer.parseInt(matcher.group(4));

         forks = new ParseTreeNode[numberOfForks];

         for (int i = 0; i < numberOfForks; ++i) {
            forks[i] = new ParseTreeNode();
            forks[i].parse(reader);
         }
      }
   }

   private class RuleType2 extends RuleType {

      public RuleType2() {
         super("type=\"2\" class=\"(.*)\" freq=\"(.*)\" att=\"(.*)\" forks=\"(\\d+)\" cut=\"(.*)\"");
      }

      @Override
      public void parse(BufferedReader reader) throws Exception {
         if (log.isTraceEnabled()) {
            log.tracef("'%s' Matched with type 2: %s", matcher.group(0), matcher.pattern());
         }
         type = 2;
         clazz = matcher.group(1);
         parseFrequency(matcher.group(2));
         attribute = matcher.group(3);
         numberOfForks = Integer.parseInt(matcher.group(4));
         cut = matcher.group(5);

         forks = new ParseTreeNode[numberOfForks];

         for (int i = 0; i < numberOfForks; ++i) {
            forks[i] = new ParseTreeNode();
            forks[i].parse(reader);
         }
      }
   }

   private class RuleType3 extends RuleType {

      public RuleType3() {
         super("type=\"3\" class=\"(.*)\" freq=\"(.*)\" att=\"(.*)\" forks=\"(\\d+)\" (elts=\"(.*)\")+");
      }

      @Override
      public void parse(BufferedReader reader) throws Exception {
         if (log.isTraceEnabled()) {
            log.tracef("'%s' Matched with type 3: %s", matcher.group(0), matcher.pattern());
         }
         type = 3;
         clazz = matcher.group(1);
         parseFrequency(matcher.group(2));
         attribute = matcher.group(3);
         numberOfForks = Integer.parseInt(matcher.group(4));

         elts = new EltsValues[numberOfForks];

         parseElts(matcher.group(5));

         forks = new ParseTreeNode[numberOfForks];

         for (int i = 0; i < numberOfForks; ++i) {
            forks[i] = new ParseTreeNode();
            forks[i].parse(reader);
         }
      }
   }
}
