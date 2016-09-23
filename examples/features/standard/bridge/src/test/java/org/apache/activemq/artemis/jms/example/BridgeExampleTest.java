package org.apache.activemq.artemis.jms.example;

import org.apache.activemq.artemis.junit.EmbeddedJMSResource;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class BridgeExampleTest {

   @Rule
   public EmbeddedJMSResource server0 = new EmbeddedJMSResource("activemq/server0/broker.xml");

   @Rule
   public EmbeddedJMSResource server1 = new EmbeddedJMSResource("activemq/server1/broker.xml");

   @Test
   public void testMain() throws Exception {
      String[] args = new String[0];

      BridgeExample.main(args);
   }

}