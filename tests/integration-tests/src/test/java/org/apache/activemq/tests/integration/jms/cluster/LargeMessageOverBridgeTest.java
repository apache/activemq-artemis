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
package org.apache.activemq.tests.integration.jms.cluster;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.jms.HornetQJMSClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.jms.client.HornetQConnectionFactory;
import org.apache.activemq.tests.util.JMSClusteredTestBase;
import org.junit.Test;

/**
 * A TextMessageOverBridgeTest
 *
 * @author clebertsuconic
 */
public class LargeMessageOverBridgeTest extends JMSClusteredTestBase
{
   /**
    * This was causing a text message to ber eventually converted into large message when sent over the bridge
    *
    * @throws Exception
    */
   @Test
   public void testSendHalfLargeTextMessage() throws Exception
   {
      createQueue("Q1");

      Queue queue = (Queue) context1.lookup("queue/Q1");
      Connection conn1 = cf1.createConnection();
      Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod1 = session1.createProducer(queue);

      Connection conn2 = cf2.createConnection();
      Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons2 = session2.createConsumer(queue);
      conn2.start();

      StringBuffer buffer = new StringBuffer();

      for (int i = 0; i < 51180; i++)
      {
         buffer.append('a');
      }

      for (int i = 0; i < 10; i++)
      {
         TextMessage msg = session1.createTextMessage(buffer.toString());
         prod1.send(msg);
      }

      TextMessage msg2 = (TextMessage) cons2.receive(5000);
      assertNotNull(msg2);

      assertEquals(buffer.toString(), msg2.getText());

      conn1.close();
      conn2.close();

   }


   protected Configuration createConfigServer2()
   {
      Configuration config = super.createConfigServer2();

      installHack(config);

      return config;
   }


   /**
    * the hack to create the failing condition in certain tests
    *
    * @param config
    */
   private void installHack(Configuration config)
   {
      if (this.getName().equals("testSendBytesAsLargeOnBridgeOnly"))
      {
         for (ClusterConnectionConfiguration conn : config.getClusterConfigurations())
         {
            conn.setMinLargeMessageSize(1000);
         }
      }
   }

   protected Configuration createConfigServer1()
   {
      Configuration config = super.createConfigServer1();

      installHack(config);


      return config;
   }


   /**
    * This was causing a text message to ber eventually converted into large message when sent over the bridge
    *
    * @throws Exception
    */
   @Test
   public void testSendBytesAsLargeOnBridgeOnly() throws Exception
   {
      createQueue("Q1");


      Queue queue = (Queue) context1.lookup("queue/Q1");
      Connection conn1 = cf1.createConnection();
      Session session1 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer prod1 = session1.createProducer(queue);

      Connection conn2 = cf2.createConnection();
      Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons2 = session2.createConsumer(queue);
      conn2.start();

      byte[] bytes = new byte[10 * 1024];

      for (int i = 0; i < bytes.length; i++)
      {
         bytes[i] = getSamplebyte(i);
      }

      for (int i = 0; i < 10; i++)
      {
         BytesMessage msg = session1.createBytesMessage();
         msg.writeBytes(bytes);
         prod1.send(msg);
      }

      session1.commit();


      for (int i = 0; i < 5; i++)
      {
         BytesMessage msg2 = (BytesMessage) cons2.receive(5000);
         assertNotNull(msg2);
         msg2.acknowledge();

         for (int j = 0; j < bytes.length; j++)
         {
            assertEquals("Position " + i, msg2.readByte(), bytes[j]);
         }
      }

      conn1.close();
      conn2.close();
   }

   /**
    * The message won't be large to the client while it will be considered large through the bridge
    *
    * @throws Exception
    */
   @Test
   public void testSendLargeForBridge() throws Exception
   {
      createQueue("Q1");


      Queue queue = (Queue) context1.lookup("queue/Q1");


      HornetQConnectionFactory cf1 = HornetQJMSClient.createConnectionFactoryWithHA(JMSFactoryType.CF, new TransportConfiguration(INVM_CONNECTOR_FACTORY, generateInVMParams(0)));
      cf1.setMinLargeMessageSize(200 * 1024);

      Connection conn1 = cf1.createConnection();
      Session session1 = conn1.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer prod1 = session1.createProducer(queue);

      Connection conn2 = cf2.createConnection();
      Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons2 = session2.createConsumer(queue);
      conn2.start();

      byte[] bytes = new byte[150 * 1024];

      for (int i = 0; i < bytes.length; i++)
      {
         bytes[i] = getSamplebyte(i);
      }

      for (int i = 0; i < 10; i++)
      {
         BytesMessage msg = session1.createBytesMessage();
         msg.writeBytes(bytes);
         prod1.send(msg);
      }

      session1.commit();


      for (int i = 0; i < 5; i++)
      {
         BytesMessage msg2 = (BytesMessage) cons2.receive(5000);
         assertNotNull(msg2);
         msg2.acknowledge();

         for (int j = 0; j < bytes.length; j++)
         {
            assertEquals("Position " + i, msg2.readByte(), bytes[j]);
         }
      }

      conn1.close();
      conn2.close();
   }

}
