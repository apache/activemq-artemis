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

package org.hornetq.tests.integration.tools;

import java.util.ArrayList;
import java.util.Map;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.postoffice.impl.PostOfficeImpl;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.cluster.impl.ClusterConnectionBridge;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;
import org.hornetq.tools.Main;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class TransferMessageTest extends ClusterTestBase
{

   public static final int NUM_MESSAGES = 2000;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      setupServers();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      stopServers();

      super.tearDown();
   }

   protected boolean isNetty()
   {
      return true;
   }


   public void setupServers() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());
      setupServer(3, isFileStorage(), isNetty());
      setupServer(4, isFileStorage(), isNetty());
   }


   @Test
   public void testFreezeMessages() throws Throwable
   {
      try
      {
         setupCluster();

         startServers();

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());
         setupSessionFactory(3, isNetty());
         setupSessionFactory(4, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, true);
         createQueue(1, "queues.testaddress", "queue0", null, true);
         createQueue(2, "queues.testaddress", "queue0", null, true);
         createQueue(3, "queues.testaddress", "queue0", null, true);
         createQueue(4, "queues.testaddress", "queue0", null, true);

         addConsumer(0, 0, "queue0", null);
         addConsumer(1, 1, "queue0", null);
         addConsumer(2, 2, "queue0", null);
         addConsumer(3, 3, "queue0", null);
         addConsumer(4, 4, "queue0", null);

         waitForBindings(0, "queues.testaddress", 1, 1, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 1, true);
         waitForBindings(3, "queues.testaddress", 1, 1, true);
         waitForBindings(4, "queues.testaddress", 1, 1, true);

         waitForBindings(0, "queues.testaddress", 4, 4, false);
         waitForBindings(1, "queues.testaddress", 4, 4, false);
         waitForBindings(2, "queues.testaddress", 4, 4, false);
         waitForBindings(3, "queues.testaddress", 4, 4, false);
         waitForBindings(4, "queues.testaddress", 4, 4, false);


         PostOfficeImpl postOffice = (PostOfficeImpl) servers[0].getPostOffice();

         ArrayList<String> queuesToTransfer = new ArrayList<String>();

//         System.out.println("bindings = " + postOffice.getAddressManager().getBindings().size());
         for (Map.Entry<SimpleString, Binding> entry : postOffice.getAddressManager().getBindings().entrySet())
         {
//            System.out.println("entry: " + entry + " / " + entry.getValue() + " class = " + entry.getValue().getClass());

            if (entry.getValue() instanceof LocalQueueBinding)
            {
               LocalQueueBinding localQueueBinding = (LocalQueueBinding) entry.getValue();

               if (localQueueBinding.getBindable() instanceof QueueImpl)
               {
                  QueueImpl queue = (QueueImpl) localQueueBinding.getBindable();
                  for (Consumer consumer : queue.getConsumers())
                  {

                     if (consumer instanceof ClusterConnectionBridge)
                     {
                        queuesToTransfer.add(entry.getKey().toString());
//                        System.out.println("Removing bridge from consumers, so messages should get stuck");
                        queue.removeConsumer(consumer);
                     }
                  }
               }
            }
         }

         consumers[0].getConsumer().close();

         send(0, "queues.testaddress", NUM_MESSAGES, true, null);


         createQueue(0, "output-result", "output-result", null, true);

         queuesToTransfer.add("queue0");

         closeAllConsumers();

         for (String str : queuesToTransfer)
         {
            callTransfer("transfer-queue",
                         "127.0.0.1", "" + TransportConstants.DEFAULT_PORT, "guest", "guest",
                         str,
                         "127.0.0.1", "" + TransportConstants.DEFAULT_PORT, "guest", "guest",
                         "output-result",
                         "500", "100");
         }

         ClientSession session = sfs[0].createSession(false, false);
         ClientConsumer consumer = session.createConsumer("output-result");

         session.start();

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            ClientMessage msg = consumer.receive(5000);
            assertNotNull(msg);
            msg.acknowledge();

            if (i % 100 == 0)
            {
               session.commit();
            }
         }

         assertNull(consumer.receiveImmediate());

         session.commit();

         session.close();

      }
      catch (Throwable e)
      {
         throw e;
      }

   }


   public static void callTransfer(String... args) throws Exception
   {
      Main.main(args);
   }


   @Test
   public void testFreezeMessagesWithFilter() throws Throwable
   {
      try
      {
         setupCluster();

         startServers();

         setupSessionFactory(0, isNetty());
         setupSessionFactory(1, isNetty());
         setupSessionFactory(2, isNetty());
         setupSessionFactory(3, isNetty());
         setupSessionFactory(4, isNetty());

         createQueue(0, "queues.testaddress", "queue0", null, true);
         createQueue(1, "queues.testaddress", "queue0", null, true);
         createQueue(2, "queues.testaddress", "queue0", null, true);
         createQueue(3, "queues.testaddress", "queue0", null, true);
         createQueue(4, "queues.testaddress", "queue0", null, true);

         createQueue(0, "queues2.testaddress", "queue2", null, true);
         createQueue(1, "queues2.testaddress", "queue2", null, true);
         createQueue(2, "queues2.testaddress", "queue2", null, true);
         createQueue(3, "queues2.testaddress", "queue2", null, true);
         createQueue(4, "queues2.testaddress", "queue2", null, true);

         addConsumer(0, 0, "queue0", null);
         addConsumer(1, 1, "queue0", null);
         addConsumer(2, 2, "queue0", null);
         addConsumer(3, 3, "queue0", null);
         addConsumer(4, 4, "queue0", null);

         addConsumer(5, 0, "queue2", null);
         addConsumer(6, 1, "queue2", null);
         addConsumer(7, 2, "queue2", null);
         addConsumer(8, 3, "queue2", null);
         addConsumer(9, 4, "queue2", null);

         waitForBindings(0, "queues.testaddress", 1, 1, true);
         waitForBindings(1, "queues.testaddress", 1, 1, true);
         waitForBindings(2, "queues.testaddress", 1, 1, true);
         waitForBindings(3, "queues.testaddress", 1, 1, true);
         waitForBindings(4, "queues.testaddress", 1, 1, true);

         waitForBindings(0, "queues.testaddress", 4, 4, false);
         waitForBindings(1, "queues.testaddress", 4, 4, false);
         waitForBindings(2, "queues.testaddress", 4, 4, false);
         waitForBindings(3, "queues.testaddress", 4, 4, false);
         waitForBindings(4, "queues.testaddress", 4, 4, false);


         waitForBindings(0, "queues2.testaddress", 1, 1, true);
         waitForBindings(1, "queues2.testaddress", 1, 1, true);
         waitForBindings(2, "queues2.testaddress", 1, 1, true);
         waitForBindings(3, "queues2.testaddress", 1, 1, true);
         waitForBindings(4, "queues2.testaddress", 1, 1, true);

         waitForBindings(0, "queues2.testaddress", 4, 4, false);
         waitForBindings(1, "queues2.testaddress", 4, 4, false);
         waitForBindings(2, "queues2.testaddress", 4, 4, false);
         waitForBindings(3, "queues2.testaddress", 4, 4, false);
         waitForBindings(4, "queues2.testaddress", 4, 4, false);


         PostOfficeImpl postOffice = (PostOfficeImpl) servers[0].getPostOffice();

         ArrayList<String> queuesToTransfer = new ArrayList<String>();

//         System.out.println("bindings = " + postOffice.getAddressManager().getBindings().size());
         for (Map.Entry<SimpleString, Binding> entry : postOffice.getAddressManager().getBindings().entrySet())
         {
//            System.out.println("entry: " + entry + " / " + entry.getValue() + " class = " + entry.getValue().getClass());

            if (entry.getValue() instanceof LocalQueueBinding)
            {
               LocalQueueBinding localQueueBinding = (LocalQueueBinding) entry.getValue();

               if (localQueueBinding.getBindable() instanceof QueueImpl)
               {
                  QueueImpl queue = (QueueImpl) localQueueBinding.getBindable();
                  for (Consumer consumer : queue.getConsumers())
                  {

                     if (consumer instanceof ClusterConnectionBridge)
                     {
                        queuesToTransfer.add(entry.getKey().toString());
//                        System.out.println("Removing bridge from consumers, so messages should get stuck");
                        queue.removeConsumer(consumer);
                     }
                  }
               }
            }
         }

         closeAllConsumers();

         send(0, "queues.testaddress", NUM_MESSAGES, true, null);

         send(0, "queues2.testaddress", 1000, true, null);


         createQueue(0, "tmp-queue", "tmp-queue", null, true);

         queuesToTransfer.add("queue0");

         queuesToTransfer.add("queue2");

         for (String str : queuesToTransfer)
         {
            callTransfer("transfer-queue",
                         "127.0.0.1", "" + TransportConstants.DEFAULT_PORT, "guest", "guest",
                         str,
                         "127.0.0.1", "" + TransportConstants.DEFAULT_PORT, "guest", "guest",
                         "tmp-queue",
                         "500", "100");
         }


         createQueue(0, "output-result", "output-result", null, true);


         System.out.println("Transferring the main output-queue now");


         callTransfer("transfer-queue",
                      "127.0.0.1", "" + TransportConstants.DEFAULT_PORT, "guest", "guest",
                      "tmp-queue",
                      "127.0.0.1", "" + TransportConstants.DEFAULT_PORT, "guest", "guest",
                      "output-result",
                      "500", "100",
                      "_HQ_TOOL_original_address='queues.testaddress'");


         ClientSession session = sfs[0].createSession(false, false);
         ClientConsumer consumer = session.createConsumer("output-result");

         session.start();

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            ClientMessage msg = consumer.receive(5000);
            assertNotNull(msg);
            msg.acknowledge();

            if (i % 100 == 0)
            {
               session.commit();
            }
         }


         assertNull(consumer.receiveImmediate());

         session.commit();

         stopServers(1, 2, 3, 4);


         System.out.println("Last transfer!!!");

         callTransfer("transfer-queue",
                      "127.0.0.1", "" + TransportConstants.DEFAULT_PORT, "guest", "guest",
                      "tmp-queue",
                      "127.0.0.1", "" + TransportConstants.DEFAULT_PORT, "guest", "guest",
                      "output-result",
                      "500", "100",
                      "_HQ_TOOL_original_address='queues2.testaddress'");

         session.start();


         for (int i = 0; i < 1000; i++)
         {
            ClientMessage msg = consumer.receive(5000);
            assertNotNull(msg);
            msg.acknowledge();

            if (i % 100 == 0)
            {
               session.commit();
            }
         }

         assertNull(consumer.receiveImmediate());

         session.commit();

         session.close();

      }
      catch (Throwable e)
      {
         throw e;
      }

   }


   protected void setupCluster() throws Exception
   {
      setupCluster(false);
   }


   protected void startServers() throws Exception
   {
      startServers(0, 1, 2, 3, 4);
   }


   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      setupClusterConnection("cluster0", "queues", forwardWhenNoConsumers, 1, isNetty(), 0, 1, 2, 3, 4);

      setupClusterConnection("cluster1", "queues", forwardWhenNoConsumers, 1, isNetty(), 1, 0, 2, 3, 4);

      setupClusterConnection("cluster2", "queues", forwardWhenNoConsumers, 1, isNetty(), 2, 0, 1, 3, 4);

      setupClusterConnection("cluster3", "queues", forwardWhenNoConsumers, 1, isNetty(), 3, 0, 1, 2, 4);

      setupClusterConnection("cluster4", "queues", forwardWhenNoConsumers, 1, isNetty(), 4, 0, 1, 2, 3);
   }


}
