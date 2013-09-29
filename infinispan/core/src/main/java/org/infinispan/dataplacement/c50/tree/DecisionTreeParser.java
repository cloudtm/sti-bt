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

import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Decision Tree parser for the output file *.tree
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DecisionTreeParser {

   private static final Log log = LogFactory.getLog(DecisionTreeParser.class);
   private static final String FILE_EXTENSION = ".tree";

   /**
    * parses the tree represented in this instance
    *
    * @param filePath the file path to the .tree file
    * @return the root of the decision tree
    * @throws Exception if some errors occurs during the parser
    */
   public static ParseTreeNode parse(String filePath) throws Exception {
      if (!filePath.endsWith(FILE_EXTENSION)) {
         filePath += FILE_EXTENSION;
      }

      if (log.isTraceEnabled()) {
         log.tracef("Starting to parse file %s", filePath);
      }

      InputStream inputStream = FileLookupFactory.newInstance().lookupFile(filePath,
                                                                           DecisionTreeParser.class.getClassLoader());

      if (inputStream == null) {
         throw new IllegalArgumentException("File '" + filePath + "' not found");
      }
      BufferedReader reader = null;
      try {
         reader = new BufferedReader(new InputStreamReader(inputStream));

         ParseTreeNode root = new ParseTreeNode();
         root.parse(reader);

         if (log.isTraceEnabled()) {
            StringBuilder stringBuilder = new StringBuilder("Tree parsed:\n");
            root.toString(0, stringBuilder);
            log.trace(stringBuilder);
         }

         return root;
      } finally {
         safeClose(reader);
      }
   }

   private static void safeClose(Closeable closeable) {
      try {
         if (closeable != null) {
            closeable.close();
         }
      } catch (IOException e) {
         //just ignore
      }
   }
}
