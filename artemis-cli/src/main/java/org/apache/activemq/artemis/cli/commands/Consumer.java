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

package org.apache.activemq.artemis.cli.commands;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Session;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.util.ConsumerThread;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;

@Command(name = "consume", description = "It will send consume messages from an instance")
public class Consumer extends DestAbstract
{


   @Option(name = "--durable", description = "It will use durable subscription in case of client")
   boolean durable = false;

   @Option(name = "--breakOnNull", description = "It will break on null messages")
   boolean breakOnNull = false;

   @Option(name = "--receiveTimeout", description = "Time used on receive(timeout)")
   int receiveTimeout;

   @Override
   public Object execute(ActionContext context) throws Exception
   {
      super.execute(context);

      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL, user, password);

      Destination dest = ActiveMQDestination.createDestination(this.destination, ActiveMQDestination.QUEUE_TYPE);
      try (Connection connection = factory.createConnection())
      {
         ConsumerThread[] threadsArray = new ConsumerThread[threads];
         for (int i = 0; i < threads; i++)
         {
            Session session;
            if (txBatchSize > 0)
            {
               session = connection.createSession(true, Session.SESSION_TRANSACTED);
            }
            else
            {
               session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            }
            threadsArray[i] = new ConsumerThread(session, dest, i);

            threadsArray[i].setVerbose(verbose).setSleep(sleep).setDurable(durable).setBatchSize(txBatchSize).setBreakOnNull(breakOnNull)
                          .setMessageCount(messageCount).setReceiveTimeOut(receiveTimeout);
         }

         for (ConsumerThread thread : threadsArray)
         {
            thread.start();
         }

         connection.start();

         for (ConsumerThread thread : threadsArray)
         {
            thread.join();
         }
      }

      return null;
   }

}
