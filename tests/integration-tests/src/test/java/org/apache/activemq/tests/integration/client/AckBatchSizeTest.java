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
package org.apache.activemq.tests.integration.client;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class AckBatchSizeTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");

   /*ackbatchSize tests*/

   /*
   * tests that wed don't acknowledge until the correct ackBatchSize is reached
   * */

   private int getMessageEncodeSize(final SimpleString address) throws Exception
   {
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession(false, true, true);
      ClientMessage message = session.createMessage(false);
      // we need to set the destination so we can calculate the encodesize correctly
      message.setAddress(address);
      int encodeSize = message.getEncodeSize();
      session.close();
      cf.close();
      return encodeSize;
   }

   @Test
   public void testAckBatchSize() throws Exception
   {
      ActiveMQServer server = createServer(false);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      int numMessages = 100;
      locator.setAckBatchSize(numMessages * getMessageEncodeSize(addressA));
      locator.setBlockOnAcknowledge(true);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);

      ClientSession session = cf.createSession(false, true, true);
      session.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      for (int i = 0; i < numMessages; i++)
      {
         cp.send(sendSession.createMessage(false));
      }

      ClientConsumer consumer = session.createConsumer(queueA);
      session.start();
      for (int i = 0; i < numMessages - 1; i++)
      {
         ClientMessage m = consumer.receive(5000);

         m.acknowledge();
      }

      ClientMessage m = consumer.receive(5000);
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      Assert.assertEquals(100, q.getDeliveringCount());
      m.acknowledge();
      Assert.assertEquals(0, q.getDeliveringCount());
      sendSession.close();
      session.close();
   }

   /*
   * tests that when the ackBatchSize is 0 we ack every message directly
   * */
   @Test
   public void testAckBatchSizeZero() throws Exception
   {
      ActiveMQServer server = createServer(false);

      server.start();
      ServerLocator locator = createInVMNonHALocator();
      locator.setAckBatchSize(0);
      locator.setBlockOnAcknowledge(true);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      int numMessages = 100;

      ClientSession session = cf.createSession(false, true, true);
      session.createQueue(addressA, queueA, false);
      ClientProducer cp = sendSession.createProducer(addressA);
      for (int i = 0; i < numMessages; i++)
      {
         cp.send(sendSession.createMessage(false));
      }

      ClientConsumer consumer = session.createConsumer(queueA);
      session.start();
      Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
      ClientMessage[] messages = new ClientMessage[numMessages];
      for (int i = 0; i < numMessages; i++)
      {
         messages[i] = consumer.receive(5000);
         Assert.assertNotNull(messages[i]);
      }
      for (int i = 0; i < numMessages; i++)
      {
         messages[i].acknowledge();
         Assert.assertEquals(numMessages - i - 1, q.getDeliveringCount());
      }
      sendSession.close();
      session.close();
   }
}
