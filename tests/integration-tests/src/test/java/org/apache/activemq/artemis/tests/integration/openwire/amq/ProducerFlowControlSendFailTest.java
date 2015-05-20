/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.openwire.amq;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * adapted from: org.apache.activemq.ProducerFlowControlSendFailTest
 */
public class ProducerFlowControlSendFailTest extends ProducerFlowControlTest
{

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   @Override
   protected void extraServerConfig(Configuration serverConfig)
   {
      String match = "jms.queue.#";
      Map<String, AddressSettings> asMap = serverConfig.getAddressesSettings();
      asMap.get(match)
              .setMaxSizeBytes(1)
              .setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL);
   }

   @Override
   public void test2ndPubisherWithStandardConnectionThatIsBlocked() throws Exception
   {
      // with sendFailIfNoSpace set, there is no blocking of the connection
   }

   @Override
   public void testAsyncPubisherRecoverAfterBlock() throws Exception
   {
      // sendFail means no flowControllwindow as there is no producer ack, just
      // an exception
   }

   @Override
   @Test
   public void testPubisherRecoverAfterBlock() throws Exception
   {
      ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory) getConnectionFactory();
      // with sendFail, there must be no flowControllwindow
      // sendFail is an alternative flow control mechanism that does not block
      factory.setUseAsyncSend(true);
      this.flowControlConnection = (ActiveMQConnection) factory.createConnection();
      this.flowControlConnection.start();

      final Session session = this.flowControlConnection.createSession(false,
            Session.CLIENT_ACKNOWLEDGE);
      final MessageProducer producer = session.createProducer(queueA);

      final AtomicBoolean keepGoing = new AtomicBoolean(true);

      Thread thread = new Thread("Filler")
      {
         @Override
         public void run()
         {
            while (keepGoing.get())
            {
               try
               {
                  producer.send(session.createTextMessage("Test message"));
                  if (gotResourceException.get())
                  {
                     System.out.println("got exception");
                     // do not flood the broker with requests when full as we
                     // are sending async and they
                     // will be limited by the network buffers
                     Thread.sleep(200);
                  }
               }
               catch (Exception e)
               {
                  // with async send, there will be no exceptions
                  e.printStackTrace();
               }
            }
         }
      };
      thread.start();
      waitForBlockedOrResourceLimit(new AtomicBoolean(false));

      // resourceException on second message, resumption if we
      // can receive 10
      MessageConsumer consumer = session.createConsumer(queueA);
      TextMessage msg;
      for (int idx = 0; idx < 10; ++idx)
      {
         msg = (TextMessage) consumer.receive(1000);
         if (msg != null)
         {
            msg.acknowledge();
         }
      }
      keepGoing.set(false);
      consumer.close();
   }

   @Test
   public void testPubisherRecoverAfterBlockWithSyncSend() throws Exception
   {
      ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory) getConnectionFactory();
      factory.setExceptionListener(null);
      factory.setUseAsyncSend(false);
      this.flowControlConnection = (ActiveMQConnection) factory.createConnection();
      this.flowControlConnection.start();

      final Session session = this.flowControlConnection.createSession(false,
            Session.CLIENT_ACKNOWLEDGE);
      final MessageProducer producer = session.createProducer(queueA);

      final AtomicBoolean keepGoing = new AtomicBoolean(true);
      final AtomicInteger exceptionCount = new AtomicInteger(0);
      Thread thread = new Thread("Filler")
      {
         @Override
         public void run()
         {
            while (keepGoing.get())
            {
               try
               {
                  producer.send(session.createTextMessage("Test message"));
               }
               catch (JMSException arg0)
               {
                  if (arg0 instanceof ResourceAllocationException)
                  {
                     gotResourceException.set(true);
                     exceptionCount.incrementAndGet();
                  }
               }
            }
         }
      };
      thread.start();
      waitForBlockedOrResourceLimit(new AtomicBoolean(false));

      // resourceException on second message, resumption if we
      // can receive 10
      MessageConsumer consumer = session.createConsumer(queueA);
      TextMessage msg;
      for (int idx = 0; idx < 10; ++idx)
      {
         msg = (TextMessage) consumer.receive(1000);
         if (msg != null)
         {
            msg.acknowledge();
         }
      }
      assertTrue("we were blocked at least 5 times", 5 < exceptionCount.get());
      keepGoing.set(false);
   }

   protected ConnectionFactory getConnectionFactory() throws Exception
   {
      factory.setExceptionListener(new ExceptionListener()
      {
         public void onException(JMSException arg0)
         {
            if (arg0 instanceof ResourceAllocationException)
            {
               gotResourceException.set(true);
            }
         }
      });
      return factory;
   }

}
