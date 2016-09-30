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
package org.apache.activemq.artemis.jms.tests;

import javax.jms.Connection;
import javax.jms.ConnectionMetaData;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TopicConnection;

import org.apache.activemq.artemis.jms.tests.util.ProxyAssertSupport;
import org.junit.Assert;
import org.junit.Test;

/**
 * Connection tests. Contains all connection tests, except tests relating to closing a connection,
 * which go to ConnectionClosedTest.
 */
public class ConnectionTest extends JMSTestCase {
   // Constants -----------------------------------------------------

   private static final JmsTestLogger log = JmsTestLogger.LOGGER;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testManyConnections() throws Exception {
      for (int i = 0; i < 100; i++) {
         Connection conn = createConnection();
         conn.close();
      }
   }

   //
   // Note: All tests related to closing a Connection should go to ConnectionClosedTest
   //

   @Test
   public void testGetClientID() throws Exception {
      Connection connection = createConnection();
      String clientID = connection.getClientID();

      // We don't currently set client ids on the server, so this should be null.
      // In the future we may provide connection factories that set a specific client id
      // so this may change
      ProxyAssertSupport.assertNull(clientID);

      connection.close();
   }

   @Test
   public void testSetClientID() throws Exception {
      Connection connection = createConnection();

      final String clientID = "my-test-client-id";

      connection.setClientID(clientID);

      ProxyAssertSupport.assertEquals(clientID, connection.getClientID());

      Connection connection2 = createConnection();
      try {
         connection2.setClientID(clientID);
         Assert.fail("setClientID was expected to throw an exception");
      } catch (JMSException e) {
         // expected
      }

      connection.close();

      connection2.setClientID(clientID);
   }

   @Test
   public void testSetClientAfterStart() throws Exception {
      Connection connection = null;
      try {
         connection = createConnection();

         // we startthe connection
         connection.start();

         // an attempt to set the client ID now should throw an IllegalStateException
         connection.setClientID("testSetClientID_2");
         ProxyAssertSupport.fail("Should throw a javax.jms.IllegalStateException");
      } catch (javax.jms.IllegalStateException e) {
      } catch (JMSException e) {
         ProxyAssertSupport.fail("Should raise a javax.jms.IllegalStateException, not a " + e);
      } catch (java.lang.IllegalStateException e) {
         ProxyAssertSupport.fail("Should raise a javax.jms.IllegalStateException, not a java.lang.IllegalStateException");
      } finally {
         if (connection != null) {
            connection.close();
         }
      }

   }

   @Test
   public void testSetClientIDFail() throws Exception {
      final String clientID = "my-test-client-id";

      // Setting a client id must be the first thing done to the connection
      // otherwise a javax.jms.IllegalStateException must be thrown

      Connection connection = createConnection();
      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try {
         connection.setClientID(clientID);
         ProxyAssertSupport.fail();
      } catch (javax.jms.IllegalStateException e) {
         ConnectionTest.log.trace("Caught exception ok");
      }

      connection.close();

      // TODO: This will probably go away, remove it enterily after we
      //       make sure this rule can go away
      //      connection = createConnection();
      //      connection.getClientID();
      //      try
      //      {
      //         connection.setClientID(clientID);
      //         ProxyAssertSupport.fail();
      //      }
      //      catch (javax.jms.IllegalStateException e)
      //      {
      //      }
      //      connection.close();

      connection = createConnection();
      ExceptionListener listener = connection.getExceptionListener();
      try {
         connection.setClientID(clientID);
         ProxyAssertSupport.fail();
      } catch (javax.jms.IllegalStateException e) {
      }
      connection.close();

      connection = createConnection();
      connection.setExceptionListener(listener);
      try {
         connection.setClientID(clientID);
         ProxyAssertSupport.fail();
      } catch (javax.jms.IllegalStateException e) {
      }
      connection.close();
   }

   @Test
   public void testGetMetadata() throws Exception {
      Connection connection = createConnection();

      ConnectionMetaData metaData = connection.getMetaData();

      // TODO - need to check whether these are same as current version
      metaData.getJMSMajorVersion();
      metaData.getJMSMinorVersion();
      metaData.getJMSProviderName();
      metaData.getJMSVersion();
      metaData.getJMSXPropertyNames();
      metaData.getProviderMajorVersion();
      metaData.getProviderMinorVersion();
      metaData.getProviderVersion();

      connection.close();
   }

   /**
    * Test creation of QueueSession
    */
   @Test
   public void testQueueConnection1() throws Exception {
      QueueConnection qc = queueCf.createQueueConnection();

      qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      qc.close();
   }

   /**
    * Test creation of TopicSession
    */
   @Test
   public void testTopicConnection() throws Exception {
      TopicConnection tc = topicCf.createTopicConnection();

      tc.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
   }

   /**
    * Test ExceptionListener stuff
    */
   @Test
   public void testExceptionListener() throws Exception {
      Connection conn = createConnection();

      ExceptionListener listener1 = new MyExceptionListener();

      conn.setExceptionListener(listener1);

      ExceptionListener listener2 = conn.getExceptionListener();

      ProxyAssertSupport.assertNotNull(listener2);

      ProxyAssertSupport.assertEquals(listener1, listener2);

      conn.close();

   }

   // This test is to check netty issue in https://jira.jboss.org/jira/browse/JBMESSAGING-1618

   @Test
   public void testConnectionListenerBug() throws Exception {
      for (int i = 0; i < 1000; i++) {
         Connection conn = createConnection();

         MyExceptionListener listener = new MyExceptionListener();

         conn.setExceptionListener(listener);

         conn.close();
      }
   }

   /**
    * This test is similar to a JORAM Test...
    * (UnifiedTest::testCreateDurableConnectionConsumerOnQueueConnection)
    *
    * @throws Exception
    */
   @Test
   public void testDurableSubscriberOnQueueConnection() throws Exception {
      QueueConnection queueConnection = ((QueueConnectionFactory) queueCf).createQueueConnection();

      try {
         queueConnection.createDurableConnectionConsumer(ActiveMQServerTestCase.topic1, "subscriptionName", "", (ServerSessionPool) null, 1);
         ProxyAssertSupport.fail("Should throw a javax.jms.IllegalStateException");
      } catch (javax.jms.IllegalStateException e) {
      } catch (java.lang.IllegalStateException e) {
         ProxyAssertSupport.fail("Should throw a javax.jms.IllegalStateException");
      } catch (JMSException e) {
         ProxyAssertSupport.fail("Should throw a javax.jms.IllegalStateException, not a " + e);
      } finally {
         queueConnection.close();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   static class MyExceptionListener implements ExceptionListener {

      JMSException exceptionReceived;

      @Override
      public void onException(final JMSException exception) {
         exceptionReceived = exception;
         ConnectionTest.log.trace("Received exception");
      }
   }
}
