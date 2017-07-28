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

package org.apache.activemq.artemis.cli.commands.messages;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Session;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;

@Command(name = "producer", description = "It will send messages to an instance")
public class Producer extends DestAbstract {

   @Option(name = "--non-persistent", description = "It will send messages non persistently")
   boolean nonpersistent = false;

   @Option(name = "--message-size", description = "Size of each byteMessage (The producer will use byte message on this case)")
   int messageSize = 0;

   @Option(name = "--text-size", description = "Size of each textMessage (The producer will use text message on this case)")
   int textMessageSize;

   @Option(name = "--msgttl", description = "TTL for each message")
   long msgTTL = 0L;

   @Option(name = "--group", description = "Message Group to be used")
   String msgGroupID = null;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      ActiveMQConnectionFactory factory = createConnectionFactory();

      Destination dest = ActiveMQDestination.createDestination(this.destination, ActiveMQDestination.TYPE.QUEUE);
      try (Connection connection = factory.createConnection()) {
         ProducerThread[] threadsArray = new ProducerThread[threads];
         for (int i = 0; i < threads; i++) {
            Session session;
            if (txBatchSize > 0) {
               session = connection.createSession(true, Session.SESSION_TRANSACTED);
            } else {
               session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            }
            threadsArray[i] = new ProducerThread(session, dest, i);

            threadsArray[i].setVerbose(verbose).setSleep(sleep).setPersistent(!nonpersistent).
               setMessageSize(messageSize).setTextMessageSize(textMessageSize).
               setMsgTTL(msgTTL).setMsgGroupID(msgGroupID).setTransactionBatchSize(txBatchSize).
               setMessageCount(messageCount);
         }

         for (ProducerThread thread : threadsArray) {
            thread.start();
         }

         int messagesProduced = 0;
         for (ProducerThread thread : threadsArray) {
            thread.join();
            messagesProduced += thread.getSentCount();
         }

         return messagesProduced;
      }
   }

}
