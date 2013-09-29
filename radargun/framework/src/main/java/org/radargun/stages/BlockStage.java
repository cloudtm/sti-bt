package org.radargun.stages;

import org.radargun.jmx.annotations.MBean;
import org.radargun.jmx.annotations.ManagedOperation;

import java.util.concurrent.CountDownLatch;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
@MBean(objectName = "Block", description = "Blocks master execution until the unblock method is invoked")
public class BlockStage extends AbstractMasterStage {
   
   private final CountDownLatch block = new CountDownLatch(1);
   
   @Override
   public boolean execute() throws Exception {
      block.await();
      return true;
   }
   
   @ManagedOperation(description = "Unblocks master execution")
   public void unblock(){
      block.countDown();
   }
}
