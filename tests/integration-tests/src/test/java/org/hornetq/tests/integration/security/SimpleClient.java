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
package org.hornetq.tests.integration.security;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.RandomUtil;

/**
 * Code to be run in an external VM, via main()
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
final class SimpleClient
{

   public static void main(final String[] args) throws Exception
   {
      try
      {
         if (args.length != 1)
         {
            throw new Exception("require 1 argument: connector factory class name");
         }
         String connectorFactoryClassName = args[0];

         String queueName = RandomUtil.randomString();
         String messageText = RandomUtil.randomString();

         ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(connectorFactoryClassName));
         try
         {
            ClientSessionFactory sf = locator.createSessionFactory();
            ClientSession session = sf.createSession(false, true, true);

            session.createQueue(queueName, queueName, null, false);
            ClientProducer producer = session.createProducer(queueName);
            ClientConsumer consumer = session.createConsumer(queueName);

            ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                          false,
                                                          0,
                                                          System.currentTimeMillis(),
                                                          (byte) 1);
            message.getBodyBuffer().writeString(messageText);
            producer.send(message);

            session.start();

            ClientMessage receivedMsg = consumer.receive(5000);
            if (receivedMsg == null)
            {
               throw new Exception("did not receive the message");
            }

            String text = receivedMsg.getBodyBuffer().readString();
            if (text == null || !text.equals(messageText))
            {
               throw new Exception("received " + text + ", was expecting " + messageText);
            }

            // clean all resources to exit cleanly
            consumer.close();
            session.deleteQueue(queueName);
            session.close();
            sf.close();
            System.out.println("OK");
         }
         finally
         {
            locator.close();
         }
      }
      catch (Throwable t)
      {

         String allStack = t.getMessage() + "|";
         StackTraceElement[] stackTrace = t.getStackTrace();
         for (StackTraceElement stackTraceElement : stackTrace)
         {
            allStack += stackTraceElement.toString() + "|";
         }
         // System.out.println(t.getClass().getName());
         // System.out.println(t.getMessage());
         System.out.println(allStack);
         System.exit(1);
      }
   }
}
