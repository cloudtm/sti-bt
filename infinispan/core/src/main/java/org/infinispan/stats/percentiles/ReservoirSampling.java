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
package org.infinispan.stats.percentiles;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Date: 01/01/12
 * Time: 18:38
 * @author Roberto
 * @since 5.2
 */
public class ReservoirSampling implements PercentileStats {

   private static final int DEFAULT_NUM_SPOTS = 100;
   private final long SEED = System.nanoTime();
   private double[] reservoir;
   private final AtomicInteger index;
   private int NUM_SPOT;
   private final Random rand;

   public ReservoirSampling(){
      NUM_SPOT = DEFAULT_NUM_SPOTS;
      this.reservoir = createArray();
      this.index = new AtomicInteger(0);
      rand = new Random(SEED);
   }

   public ReservoirSampling(int numSpots){
      this.NUM_SPOT = numSpots;
      this.reservoir = createArray();
      this.index = new AtomicInteger(0);
      rand = new Random(SEED);

   }

   public synchronized final void insertSample(double sample){
      int i = index.getAndIncrement();
      if(i < NUM_SPOT)
         reservoir[i]=sample;
      else{
         int rand_generated = rand.nextInt(i+2);//should be nextInt(index+1) but nextInt is exclusive
         if(rand_generated < NUM_SPOT){
            reservoir[rand_generated]=sample;
         }
      }
   }

   public synchronized final double get95Percentile(){
      return getKPercentile(95);
   }

   public synchronized final double get90Percentile(){
      return getKPercentile(90);
   }

   public synchronized final double get99Percentile(){
      return getKPercentile(99);
   }

   public synchronized final double getKPercentile(int k){
      if (k < 0 || k > 100) {
         throw new RuntimeException("Wrong index in getKpercentile");
      }
      double[] copy = createArray();
      System.arraycopy(this.reservoir,0,copy,0,NUM_SPOT);
      Arrays.sort(copy);
      return copy[this.getIndex(k)];
   }

   private int getIndex(int k){
      //I solve the proportion k:100=x:NUM_SAMPLE
      //Every percentage is covered by NUM_SAMPLE / 100 buckets; I consider here only the first as representative
      //of a percentage
      return (int) (NUM_SPOT * (k-1) / 100);
   }

   public final void reset(){
      this.index.set(0);
      this.reservoir = createArray();
   }

   private double[] createArray() {
      return new double[NUM_SPOT];
   }
}


