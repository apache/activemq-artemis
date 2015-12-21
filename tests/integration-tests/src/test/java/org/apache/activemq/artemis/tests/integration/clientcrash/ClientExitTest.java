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
package org.apache.activemq.artemis.tests.integration.clientcrash;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.SpawnedVMSupport;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Before;
import org.junit.Test;

/**
 * A test that makes sure that an ActiveMQ Artemis client gracefully exists after the last session is
 * closed. Test for http://jira.jboss.org/jira/browse/JBMESSAGING-417.
 *
 * This is not technically a crash test, but it uses the same type of topology as the crash tests
 * (local server, remote VM client).
 */
public class ClientExitTest extends ClientTestBase {
   // Constants ------------------------------------------------------------------------------------

   private static final String MESSAGE_TEXT = RandomUtil.randomString();

   private static final SimpleString QUEUE = new SimpleString("ClientExitTestQueue");

   // Static ---------------------------------------------------------------------------------------

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Attributes -----------------------------------------------------------------------------------

   private ClientSession session;

   private ClientConsumer consumer;

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   @Test
   public void testGracefulClientExit() throws Exception {
      // spawn a JVM that creates a JMS client, which sends a test message
      Process p = SpawnedVMSupport.spawnVM(GracefulClient.class.getName(), ClientExitTest.QUEUE.toString(), ClientExitTest.MESSAGE_TEXT);

      // read the message from the queue

      Message message = consumer.receive(15000);

      assertNotNull(message);
      assertEquals(ClientExitTest.MESSAGE_TEXT, message.getBodyBuffer().readString());

      // the client VM should exit by itself. If it doesn't, that means we have a problem
      // and the test will timeout
      ClientExitTest.log.debug("waiting for the client VM to exit ...");
      p.waitFor();

      assertEquals(0, p.exitValue());

      // FIXME https://jira.jboss.org/jira/browse/JBMESSAGING-1421
      // Thread.sleep(1000);
      //
      // // the local session
      // assertActiveConnections(1);
      // // assertActiveSession(1);

      session.close();

      // FIXME https://jira.jboss.org/jira/browse/JBMESSAGING-1421
      // Thread.sleep(1000);
      // assertActiveConnections(0);
      // // assertActiveSession(0);
   }

   // Package protected ----------------------------------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      ServerLocator locator = createNettyNonHALocator();
      addServerLocator(locator);
      ClientSessionFactory sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true);
      session.createQueue(ClientExitTest.QUEUE, ClientExitTest.QUEUE, null, false);
      consumer = session.createConsumer(ClientExitTest.QUEUE);
      session.start();
   }
}
