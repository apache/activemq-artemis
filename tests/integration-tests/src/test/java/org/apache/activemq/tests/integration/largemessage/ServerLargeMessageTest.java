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
package org.apache.activemq.tests.integration.largemessage;

import org.apache.activemq.api.core.Message;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.HornetQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * A ServerLargeMessageTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ServerLargeMessageTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // The ClientConsumer should be able to also send ServerLargeMessages as that's done by the CoreBridge
   @Test
   public void testSendServerMessage() throws Exception
   {
      HornetQServer server = createServer(true);

      server.start();

      ServerLocator locator = createFactory(false);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false);

      try
      {
         LargeServerMessageImpl fileMessage = new LargeServerMessageImpl((JournalStorageManager) server.getStorageManager());

         fileMessage.setMessageID(1005);

         for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
         {
            fileMessage.addBytes(new byte[]{UnitTestCase.getSamplebyte(i)});
         }
         // The server would be doing this
         fileMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);


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

         Assert.assertEquals(msg.getBodySize(), 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);

         for (int i = 0; i < 2 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE; i++)
         {
            Assert.assertEquals(UnitTestCase.getSamplebyte(i), msg.getBodyBuffer().readByte());
         }

         msg.acknowledge();

         session.commit();

      }
      finally
      {
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
