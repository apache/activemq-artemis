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
package org.apache.activemq.core.example;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory;

/**
 * A simple example of using twitter connector service.
 *
 * @author <a href="tm.igarashi@gmail.com">Tomohisa Igarashi</a>
 */
public class TwitterConnectorExample
{
   private static final String INCOMING_QUEUE = "queue.incomingQueue";
   private static final String OUTGOING_QUEUE = "queue.outgoingQueue";

   public static void main(final String[] args) throws Exception
   {
      ServerLocator locator = null;
      ClientSessionFactory csf = null;
      ClientSession session = null;
      try
      {
         String testMessage = System.currentTimeMillis() + ": " + System.getProperty("twitter.example.alternativeMessage");
         if(testMessage == null || testMessage.trim().equals("")) {
            testMessage = System.currentTimeMillis() + ": ### Hello, ActiveMQ fans!! We are now experiencing so fast, so reliable and so exciting messaging never seen before ;-) ###";
         }

         // Step 1. Create a ClientSessionFactory


         locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName()));

         csf = locator.createSessionFactory();

         // Step 2. Create a core session.
         session = csf.createSession(true,true);

         // Step 3. Create a core producer for queue.outgoingQueue.
         ClientProducer cp = session.createProducer(OUTGOING_QUEUE);

         // Step 4. Create a core consumer for queue.incomingQueue.
         ClientConsumer cc = session.createConsumer(INCOMING_QUEUE);

         // Step 5. Create a core message.
         ClientMessage cm = session.createMessage(org.apache.activemq.api.core.Message.TEXT_TYPE,true);
         cm.getBodyBuffer().writeString(testMessage);

         // Step 6. Send a message to queue.outgoingQueue.
         cp.send(cm);
         System.out.println("#### Sent a message to " + OUTGOING_QUEUE + ": " + testMessage);

         // Step 7. Start the session.
         session.start();

         // Step 8. Receive a message from queue.incomingQueue.
         //         Outgoing connector forwards a message(sent at Step 6.) to twitter immediately.
         //         Since incoming connector consumes from twitter and forwards to queue.incomingQueue
         //         every 60 seconds, It will be received in 60+x seconds.
         System.out.println("#### A message will be received in 60 seconds. Please wait...");
         ClientMessage received = cc.receive(70 * 1000);
         received.acknowledge();
         String receivedText = received.getBodyBuffer().readString();

         while(!receivedText.equals(testMessage))
         {
            // ignoring other tweets
            received = cc.receiveImmediate();
            if(received == null) {
               // no other tweets. test message has gone...
               return;
            }

            received.acknowledge();
            receivedText = received.getBodyBuffer().readString();
         }

         System.out.println("#### Received a message from " + INCOMING_QUEUE + ": " + receivedText);
      }
      finally
      {
         // Step 9. Be sure to close some resources.
         if(session != null)
         {
            session.close();
         }
         if(csf != null)
         {
            csf.close();
         }

         if (locator != null)
         {
            locator.close();
         }
      }
   }

}
