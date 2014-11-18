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
package org.apache.activemq.tests.integration.jms.connection;
import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.ActiveMQExceptionType;
import org.apache.activemq.api.core.ActiveMQInternalErrorException;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Assert;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.jms.ActiveMQJMSClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.client.impl.ClientSessionInternal;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.jms.client.ActiveMQSession;
import org.apache.activemq.jms.client.ActiveMQTemporaryTopic;
import org.apache.activemq.spi.core.protocol.RemotingConnection;
import org.apache.activemq.tests.util.JMSTestBase;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class CloseDestroyedConnectionTest extends JMSTestBase
{
   private ActiveMQConnectionFactory cf;
   private ActiveMQSession session1;
   private Connection conn2;
   private ActiveMQSession session2;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      cf =
               ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                  new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      cf.setBlockOnDurableSend(true);
      cf.setPreAcknowledge(true);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (session1 != null)
         session1.close();
      if (session2 != null)
         session2.close();
      if (conn != null)
         conn.close();
      if (conn2 != null)
         conn2.close();
      cf = null;

      super.tearDown();
   }

   @Test
   public void testClosingTemporaryTopicDeletesQueue() throws JMSException, ActiveMQException
   {
      conn = cf.createConnection();

      Assert.assertEquals(1, server.getRemotingService().getConnections().size());

      session1 = (ActiveMQSession)conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQTemporaryTopic topic = (ActiveMQTemporaryTopic)session1.createTemporaryTopic();
      String address = topic.getAddress();
      session1.close();
      conn.close();
      conn2 = cf.createConnection();
      session2 = (ActiveMQSession)conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession cs = session2.getCoreSession();
      try
      {
         cs.createConsumer(address);
         fail("the address from the TemporaryTopic still exists!");
      }
      catch (ActiveMQException e)
      {
         assertEquals("expecting 'queue does not exist'", ActiveMQExceptionType.QUEUE_DOES_NOT_EXIST, e.getType());
      }
   }

   /*
    * Closing a connection that is destroyed should cleanly close everything without throwing exceptions
    */
   @Test
   public void testCloseDestroyedConnection() throws Exception
   {
      long connectionTTL = 500;
      cf.setClientFailureCheckPeriod(connectionTTL / 2);
      // Need to set connection ttl to a low figure so connections get removed quickly on the server
      cf.setConnectionTTL(connectionTTL);

      conn = cf.createConnection();

      Assert.assertEquals(1, server.getRemotingService().getConnections().size());

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // Give time for the initial ping to reach the server before we fail (it has connection TTL in it)
      Thread.sleep(500);

      String queueName = "myqueue";

      Queue queue = ActiveMQJMSClient.createQueue(queueName);

      super.createQueue(queueName);

      sess.createConsumer(queue);

      sess.createProducer(queue);

      sess.createBrowser(queue);

      // Now fail the underlying connection

      ClientSessionInternal sessi = (ClientSessionInternal)((ActiveMQSession)sess).getCoreSession();

      RemotingConnection rc = sessi.getConnection();

      rc.fail(new ActiveMQInternalErrorException());

      // Now close the connection

      conn.close();

      long start = System.currentTimeMillis();

      while (true)
      {
         int cons = server.getRemotingService().getConnections().size();

         if (cons == 0)
         {
            break;
         }

         long now = System.currentTimeMillis();

         if (now - start > 10000)
         {
            throw new Exception("Timed out waiting for connections to close");
         }

         Thread.sleep(50);
      }
   }
}
