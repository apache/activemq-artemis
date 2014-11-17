/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.activemq6.tests.integration.jms.jms2client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.Random;

import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.jms.HornetQJMSClient;
import org.apache.activemq6.api.jms.JMSFactoryType;
import org.apache.activemq6.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq6.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class NonExistentQueueTest extends JMSTestBase
{

   private JMSContext context;
   private final Random random = new Random();
   private Queue queue;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      context = createContext();
      queue = createQueue(JmsContextTest.class.getSimpleName() + "Queue1");
   }


   @Test
   public void sendToNonExistantDestination() throws Exception
   {
      Destination destination = HornetQJMSClient.createQueue("DoesNotExist");
      TransportConfiguration transportConfiguration = new TransportConfiguration(InVMConnectorFactory.class.getName());
      ConnectionFactory localConnectionFactory = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                                   transportConfiguration);
      // Using JMS 1 API
      Connection connection = localConnectionFactory.createConnection();
      Session session = connection.createSession();
      try
      {
         MessageProducer messageProducer = session.createProducer(null);
         messageProducer.send(destination, session.createMessage());
         Assert.fail("Succeeded in sending message to a non-existant destination using JMS 1 API!");
      }
      catch (JMSException e)
      { // Expected }

      }

      // Using JMS 2 API
      JMSContext context = localConnectionFactory.createContext();
      JMSProducer jmsProducer = context.createProducer().setDeliveryMode(DeliveryMode.PERSISTENT);

      try
      {
         jmsProducer.send(destination, context.createMessage());
         Assert.fail("Succeeded in sending message to a non-existant destination using JMS 2 API!");
      }
      catch (JMSRuntimeException e)
      { // Expected }
      }


   }
}
