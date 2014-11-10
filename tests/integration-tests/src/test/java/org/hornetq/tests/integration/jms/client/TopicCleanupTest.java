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
package org.hornetq.tests.integration.jms.client;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.tests.util.JMSTestBase;
import org.junit.Test;

/**
 * This test will simulate a situation where the Topics used to have an extra queue on startup.
 * The server was then written to perform a cleanup, and that cleanup should always work.
 * This test will create the dirty situation where the test should recover from
 *
 * @author clebertsuconic
 */
public class TopicCleanupTest extends JMSTestBase
{

   protected boolean usePersistence()
   {
      return true;
   }

   @Test
   public void testSendTopic() throws Exception
   {
      Topic topic = createTopic("topic");
      Connection conn = cf.createConnection();


      try
      {
         conn.setClientID("someID");

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = sess.createDurableSubscriber(topic, "someSub");

         conn.start();

         MessageProducer prod = sess.createProducer(topic);

         TextMessage msg1 = sess.createTextMessage("text");

         prod.send(msg1);

         assertNotNull(cons.receive(5000));

         conn.close();

         StorageManager storage = server.getStorageManager();


         for (int i = 0; i < 100; i++)
         {
            long txid = storage.generateID();

            final Queue queue = new QueueImpl(storage.generateID(), SimpleString.toSimpleString("jms.topic.topic"), SimpleString.toSimpleString("jms.topic.topic"), FilterImpl.createFilter(HornetQServerImpl.GENERIC_IGNORED_FILTER), true, false, server.getScheduledPool(), server.getPostOffice(),
                                              storage, server.getAddressSettingsRepository(), server.getExecutorFactory().getExecutor());

            LocalQueueBinding binding = new LocalQueueBinding(queue.getAddress(), queue, server.getNodeID());

            storage.addQueueBinding(txid, binding);

            storage.commitBindings(txid);
         }

         jmsServer.stop();

         jmsServer.start();

      }
      finally
      {
         try
         {
            conn.close();
         }
         catch (Throwable igonred)
         {
         }
      }

   }

}
