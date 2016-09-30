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
package org.apache.activemq.artemis.tests.integration.largemessage;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;

public class ServerLargeMessageTest extends ActiveMQTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // The ClientConsumer should be able to also send ServerLargeMessages as that's done by the CoreBridge
   @Test
   public void testSendServerMessage() throws Exception {
      ActiveMQServer server = createServer(true);

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false);

      try {
         LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) server.getStorageManager());

         fileMessage.setMessageID(1005);

         for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
            fileMessage.addBytes(new byte[]{ActiveMQTestBase.getSamplebyte(i)});
         }
         // The server would be doing this
         fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         fileMessage.releaseResources();

         session.createQueue("A", "A");

         ClientProducer prod = session.createProducer("A");

         prod.send(fileMessage);

         fileMessage.deleteFile();

         session.commit();

         session.start();

         ClientConsumer cons = session.createConsumer("A");

         ClientMessage msg = cons.receive(5000);

         Assert.assertNotNull(msg);

         Assert.assertEquals(msg.getBodySize(), 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         for (int i = 0; i < 2 * ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++) {
            Assert.assertEquals(ActiveMQTestBase.getSamplebyte(i), msg.getBodyBuffer().readByte());
         }

         msg.acknowledge();

         session.commit();

      } finally {
         sf.close();
         locator.close();
         server.stop();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
