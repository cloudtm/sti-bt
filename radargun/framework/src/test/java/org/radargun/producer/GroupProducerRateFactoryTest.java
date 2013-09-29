package org.radargun.producer;

import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Tests the Group Producer Rate Factory component
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
@Test
public class GroupProducerRateFactoryTest {

   public void testProducerRate() {
      //10000 tx/sec, 10 nodes
      int numberOfNodes = 10;
      for (int i = 0; i < numberOfNodes; ++i) {
         GroupProducerRateFactory groupProducerRateFactory = new GroupProducerRateFactory(10000, numberOfNodes, i, 10);
         ProducerRate[] pr = groupProducerRateFactory.create();

         assert pr.length == 10 : "Expected 10 producers";
         assertProducerRate(0.1D, pr);
      }
   }

   public void testSlowProducers() {
      //500 tx/sec, 2 nodes

      int numberOfNodes = 2;
      for (int i = 0; i < numberOfNodes; ++i) {
         GroupProducerRateFactory groupProducerRateFactory = new GroupProducerRateFactory(500, numberOfNodes, i, 10);
         ProducerRate[] pr = groupProducerRateFactory.create();

         assert pr.length == 3 : "Expected 3 producers";
         assertProducerRate(0.1D, pr[0], pr[1]);
         assertProducerRate(0.05D, pr[2]);
      }
   }

   public void testUnbalancedNodesAndSlowProducers() {
      //500 tx/sec, 3 nodes
      GroupProducerRateFactory groupProducerRateFactory = new GroupProducerRateFactory(500, 3, 0, 10);
      ProducerRate[] pr = groupProducerRateFactory.create();

      assert pr.length == 2 : "Expected 2 producers";
      assertProducerRate(0.1D, pr[0]);
      assertProducerRate(0.067D, pr[1]);

      groupProducerRateFactory = new GroupProducerRateFactory(500, 3, 1, 10);
      pr = groupProducerRateFactory.create();

      assert pr.length == 2 : "Expected 2 producers";
      assertProducerRate(0.1D, pr[0]);
      assertProducerRate(0.067D, pr[1]);

      groupProducerRateFactory = new GroupProducerRateFactory(500, 3, 2, 10);
      pr = groupProducerRateFactory.create();

      assert pr.length == 2 : "Expected 2 producers";
      assertProducerRate(0.1D, pr[0]);
      assertProducerRate(0.066D, pr[1]);
   }

   private void assertProducerRate(double lambda, ProducerRate... producers) {
      for (ProducerRate producerRate : producers) {
         BigDecimal bg = new BigDecimal(producerRate.getLambda());
         bg = bg.setScale(3, RoundingMode.HALF_UP);
         assert lambda == bg.doubleValue() : "Different Lambda detected! " + lambda + "!=" +
               producerRate.getLambda();
      }
   }

}
