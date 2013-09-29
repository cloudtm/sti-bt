package org.radargun.producer;

import java.util.Random;

/**
 * This class handles the sleep time for the producer rates
 *
 * @author Pedro Ruivo
 * @author Diego Didona
 * @since 1.1
 */
public class ProducerRate {

   private final double lambda;
   private final Random random;

   /**
    *
    * @param producerLambda   the lambda (arrival rate) in transaction per millisecond
    */
   public ProducerRate(double producerLambda) {
      this.lambda = producerLambda;
      this.random = new Random(System.currentTimeMillis());
   }

   /**
    * it sleeps to a determined rate, in order to achieve the lambda (arrival rate) desired
    */
   public final void sleep() {
      long sleepTime = (long) exp(random, lambda);
      try {
         Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
         //interrupted
      }
   }

   /**
    * NOTE: the public visibility is only for testing purpose!
    * @param random  random generator number
    * @param lambda  the lambda in milliseconds
    * @return        the sleeping time in milliseconds
    */
   public final double exp(Random random, double lambda) {
      double ret = -Math.log(1.0D - random.nextDouble()) / lambda;
      //I bound the value in the interval 1msec-30sec--> at least 1msec!
      return ret < 1 ? 1 : (ret > 30000 ? 30000 : ret);
   }

   /**
    * returns the lambda of this producer rate
    * @return  the lambda of this producer rate
    */
   public double getLambda() {
      return lambda;
   }
}
