package org.infinispan.remoting.responses;

import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.ConfigurationState;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ConfigurationFilter implements ResponseFilter {

   private int expectedResponses;

   public ConfigurationFilter(int expectedResponses) {
      this.expectedResponses = expectedResponses - 1; //remove self
   }

   @Override
   public boolean isAcceptable(Response response, Address sender) {
      if (response instanceof SuccessfulResponse) {
         Object value = ((SuccessfulResponse) response).getResponseValue();
         if (value != null && value instanceof ConfigurationState) {
            expectedResponses = 0;
         }
      }
      expectedResponses--;
      return true;
   }

   @Override
   public boolean needMoreResponses() {
      return expectedResponses > 0;
   }
}
