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
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.FileInputStream;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.factory.serialize.MessageSerializer;
import org.apache.activemq.artemis.jms.client.ActiveMQMessage;

@Command(name = "producer", description = "It will send messages to an instance")
public class Producer extends DestAbstract {

   public static final String DEMO_TEXT = "demo.txt";

   @Option(name = "--non-persistent", description = "It will send messages non persistently")
   boolean nonpersistent = false;

   @Option(name = "--message-size", description = "Size of each byteMessage (The producer will use byte message on this case)")
   int messageSize = 0;

   @Option(name = "--text-size", description = "Size of each textMessage (The producer will use text message on this case)")
   int textMessageSize;

   @Option(name = "--object-size", description = "Size of each ObjectMessage (The producer will use object mesasge on this case)")
   int objectSize;

   @Option(name = "--msgttl", description = "TTL for each message")
   long msgTTL = 0L;

   @Option(name = "--group", description = "Message Group to be used")
   String msgGroupID = null;

   @Option(name = "--data", description = "Messages will be read form the specified file, other message options will be ignored.")
   String fileName = null;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      ConnectionFactory factory = createConnectionFactory();

      try (Connection connection = factory.createConnection()) {

         byte[] queueId = null;
         boolean isFQQN = isFQQN();
         if (isFQQN) {
            queueId = getQueueIdFromName(getQueueFromFQQN(destination));
         }

         // If we are reading from file, we process messages sequentially to guarantee ordering.  i.e. no thread creation.
         if (fileName != null) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination dest = lookupDestination(session, isFQQN);

            MessageProducer producer = session.createProducer(dest);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            int messageCount = 0;
            try {
               MessageSerializer serializer = getMessageSerializer();
               serializer.setInput(new FileInputStream(fileName), session);
               serializer.start();

               Message message = serializer.read();

               while (message != null) {
                  if (queueId != null) ((ActiveMQMessage) message).getCoreMessage().putBytesProperty(org.apache.activemq.artemis.api.core.Message.HDR_ROUTE_TO_IDS, queueId);
                  producer.send(message);
                  message = serializer.read();
                  messageCount++;
               }

               session.commit();
               serializer.stop();
            } catch (Exception e) {
               System.err.println("Error occurred during import.  Rolling back.");
               session.rollback();
               e.printStackTrace();
               return 0;
            }
            System.out.println("Sent " + messageCount + " Messages.");
            return messageCount;
         } else {
            ProducerThread[] threadsArray = new ProducerThread[threads];
            for (int i = 0; i < threads; i++) {
               Session session;
               if (txBatchSize > 0) {
                  session = connection.createSession(true, Session.SESSION_TRANSACTED);
               } else {
                  session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               }
               Destination dest = lookupDestination(session, isFQQN);
               threadsArray[i] = new ProducerThread(session, dest, i);

               threadsArray[i].setVerbose(verbose).setSleep(sleep).setPersistent(!nonpersistent).
                  setMessageSize(messageSize).setTextMessageSize(textMessageSize).setObjectSize(objectSize).
                  setMsgTTL(msgTTL).setMsgGroupID(msgGroupID).setTransactionBatchSize(txBatchSize).
                  setMessageCount(messageCount).setQueueId(queueId);
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

   public Destination lookupDestination(Session session, boolean isFQQN) throws Exception {
      Destination dest;
      if (!isFQQN) {
         dest = lookupDestination(session);
      } else {
         String address = getAddressFromFQQN(destination);
         if (isFQQNAnycast(getQueueFromFQQN(destination))) {
            String queue = getQueueFromFQQN(destination);
            if (!queue.equals(address)) {
               throw new ActiveMQException("FQQN support is limited to Anycast queues where the queue name equals the address.");
            }
            dest = session.createQueue(address);
         } else {
            dest = session.createTopic(address);
         }
      }
      return dest;
   }

   protected boolean isFQQNAnycast(String queueName) throws Exception {
      ClientMessage message = getQueueAttribute(queueName, "RoutingType");
      String routingType = (String) ManagementHelper.getResult(message);
      return routingType.equalsIgnoreCase("anycast");
   }
}
