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
package org.infinispan.dataplacement.stats;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Task that saves the round statistics to a file
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class SaveStatsTask implements Runnable {

   private static final Log log = LogFactory.getLog(SaveStatsTask.class);
   private static final String PATH = "./stats.csv";
   private static final AtomicBoolean FIRST_TIME = new AtomicBoolean(true);
   private final Stats stats;

   public SaveStatsTask(Stats stats) {
      this.stats = stats;
   }

   @Override
   public void run() {
      try {
         log.errorf("Save stats to %s, first time %s", PATH, FIRST_TIME.get());
         BufferedWriter writer = new BufferedWriter(new FileWriter(PATH, !FIRST_TIME.get()));
         stats.saveTo(writer, FIRST_TIME.compareAndSet(true, false));
         writer.flush();
         writer.close();
      } catch (Exception e) {
         log.errorf(e, "Error saving stats to %s", PATH);
      }
   }
}
