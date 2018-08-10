/*
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
package org.apache.activemq.artemis.tests.integration.jms.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.InvalidClientIDException;
import javax.jms.JMSContext;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.XAConnection;
import javax.jms.XASession;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class ConnectionTest extends JMSTestBase {

   private Connection conn2;

   @Test
   public void testThroughNewConnectionFactory() throws Exception {
      ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://0");
      testThroughNewConnectionFactory(connectionFactory);

      // Run it again with a cloned through serialization CF, simulating JNDI lookups
      connectionFactory = serialClone(connectionFactory);
      testThroughNewConnectionFactory(connectionFactory);

      connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616?&blockOnNonDurableSend=true&" +
                                                           "retryIntervalMultiplier=1.0&maxRetryInterval=2000&producerMaxRate=-1&" +
                                                           "blockOnDurableSend=true&connectionTTL=60000&compressLargeMessage=false&reconnectAttempts=0&" +
                                                           "cacheLargeMessagesClient=false&scheduledThreadPoolMaxSize=5&useGlobalPools=true&" +
                                                           "callFailoverTimeout=-1&initialConnectAttempts=1&clientFailureCheckPeriod=30000&" +
                                                           "blockOnAcknowledge=true&consumerWindowSize=1048576&minLargeMessageSize=102400&" +
                                                           "autoGroup=false&threadPoolMaxSize=-1&confirmationWindowSize=-1&" +
                                                           "transactionBatchSize=1048576&callTimeout=30000&preAcknowledge=false&" +
                                                           "connectionLoadBalancingPolicyClassName=org.apache.activemq.artemis.api.core.client.loadbalance." +
                                                           "RoundRobinConnectionLoadBalancingPolicy&dupsOKBatchSize=1048576&initialMessagePacketSize=1500&" +
                                                           "consumerMaxRate=-1&retryInterval=2000&failoverOnInitialConnection=false&producerWindowSize=65536&" +
                                                           "port=61616&host=localhost#");

      testThroughNewConnectionFactory(connectionFactory);

      // Run it again with a cloned through serialization CF, simulating JNDI lookups
      connectionFactory = serialClone(connectionFactory);
      testThroughNewConnectionFactory(connectionFactory);
   }

   private void testThroughNewConnectionFactory(ActiveMQConnectionFactory factory) throws Exception {
      Connection conn = factory.createConnection();
      conn.close();

      try (JMSContext ctx = factory.createContext()) {
         ctx.createProducer().send(ctx.createQueue("queue"), "Test");
      }

      try (JMSContext ctx = factory.createContext()) {
         Assert.assertNotNull(ctx.createConsumer(ctx.createQueue("queue")).receiveNoWait());
         Assert.assertNull(ctx.createConsumer(ctx.createQueue("queue")).receiveNoWait());
      }

      factory.close();
   }

   @Test
   public void testSetSameIdToDifferentConnections() throws Exception {
      String id = "somethingElse" + name.getMethodName();
      conn = cf.createConnection();
      conn2 = cf.createConnection();
      conn.getClientID();
      conn.setClientID(id);
      try {
         conn2.setClientID(id);
         Assert.fail("should not happen.");
      } catch (InvalidClientIDException expected) {
         // expected
      }


      Session session1 = conn.createSession();
      Session session2 = conn.createSession();

      session1.close();
      session2.close();

   }


   @Test
   public void testTwoConnectionsSameIDThroughCF() throws Exception {
      ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616?clientID=myid");

      conn = connectionFactory.createConnection();
      try {
         conn2 = connectionFactory.createConnection();
         Assert.fail("Exception expected");
      } catch (InvalidClientIDException expected) {
         // expected
      }


      Session session1 = conn.createSession();
      Session session2 = conn.createSession();

      session1.close();
      session2.close();
   }

   @Test
   public void testTwoConnectionsSameIDThroughCFWithShareClientIDEnabeld() throws Exception {
      ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616?clientID=myid;enableSharedClientID=true");

      conn = connectionFactory.createConnection();
      try {
         conn2 = connectionFactory.createConnection();
      } catch (InvalidClientIDException expected) {
         Assert.fail("Should allow sharing of client IDs among the same CF");
      }

      Session session1 = conn.createSession();
      Session session2 = conn.createSession();
      Session session3 = conn2.createSession();
      Session session4 = conn2.createSession();

      session1.close();
      session2.close();
      session3.close();
      session4.close();
   }

   @Test
   public void testGetSetConnectionFactory() throws Exception {
      conn = cf.createConnection();

      conn.getClientID();

      conn.setClientID("somethingElse");
   }

   @Test
   public void testTXTypeInvalid() throws Exception {
      conn = cf.createConnection();

      Session sess = conn.createSession(false, Session.SESSION_TRANSACTED);

      assertEquals(Session.AUTO_ACKNOWLEDGE, sess.getAcknowledgeMode());

      sess.close();

      TopicSession tpSess = ((TopicConnection) conn).createTopicSession(false, Session.SESSION_TRANSACTED);

      assertEquals(Session.AUTO_ACKNOWLEDGE, tpSess.getAcknowledgeMode());

      tpSess.close();

      QueueSession qSess = ((QueueConnection) conn).createQueueSession(false, Session.SESSION_TRANSACTED);

      assertEquals(Session.AUTO_ACKNOWLEDGE, qSess.getAcknowledgeMode());

      qSess.close();

   }

   @Test
   public void testXAInstanceof() throws Exception {
      conn = cf.createConnection();

      assertFalse(conn instanceof XAConnection);
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      assertFalse(sess instanceof XASession);
   }

   @Test
   public void testConnectionFactorySerialization() throws Exception {
      //first try cf without any connection being created
      ConnectionFactory newCF = getCFThruSerialization(cf);
      testCreateConnection(newCF);

      //now serialize a cf after a connection has been created
      //https://issues.jboss.org/browse/WFLY-327
      Connection aConn = null;
      try {
         aConn = cf.createConnection();
         newCF = getCFThruSerialization(cf);
         testCreateConnection(newCF);
      } finally {
         if (aConn != null) {
            aConn.close();
         }
      }

   }

   private ConnectionFactory getCFThruSerialization(ConnectionFactory fact) throws Exception {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);

      oos.writeObject(cf);
      ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(bis);
      ConnectionFactory newCF = (ConnectionFactory) ois.readObject();
      oos.close();
      ois.close();

      return newCF;
   }

   private void testCreateConnection(ConnectionFactory fact) throws Exception {
      Connection newConn = null;
      try {
         newConn = fact.createConnection();
         newConn.start();
         newConn.stop();
         Session session1 = newConn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         session1.close();
         Session session2 = newConn.createSession(true, Session.SESSION_TRANSACTED);
         session2.close();
      } finally {
         if (newConn != null) {
            newConn.close();
         }
      }
   }

   @Override
   @After
   public void tearDown() throws Exception {
      if (conn2 != null) {
         conn2.close();
      }
      super.tearDown();
   }
}
