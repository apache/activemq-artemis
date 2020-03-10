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
import java.io.InputStream;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.factory.serialize.MessageSerializer;

@Command(name = "producer", description = "It will send messages to an instance")
public class Producer extends DestAbstract {

   public static final String DEMO_TEXT = "demo.txt";

   @Option(name = "--non-persistent", description = "It will send messages non persistently")
   boolean nonpersistent = false;

   @Option(name = "--message-size", description = "Size of each byteMessage (The producer will use byte message on this case)")
   int messageSize = 0;

   @Option(name = "--message", description = "Content of each textMessage (The producer will use text message on this case)")
   String message = null;

   @Option(name = "--text-size", description = "Size of each textMessage (The producer will use text message on this case)")
   int textMessageSize;

   @Option(name = "--object-size", description = "Size of each ObjectMessage (The producer will use object message on this case)")
   int objectSize;

   @Option(name = "--msgttl", description = "TTL for each message")
   long msgTTL = 0L;

   @Option(name = "--group", description = "Message Group to be used")
   String msgGroupID = null;

   @Option(name = "--data", description = "Messages will be read form the specified file, other message options will be ignored.")
   String file = null;

   public boolean isNonpersistent() {
      return nonpersistent;
   }

   public Producer setNonpersistent(boolean nonpersistent) {
      this.nonpersistent = nonpersistent;
      return this;
   }

   public int getMessageSize() {
      return messageSize;
   }

   public Producer setMessageSize(int messageSize) {
      this.messageSize = messageSize;
      return this;
   }

   public String getMessage() {
      return message;
   }

   public Producer setMessage(String message) {
      this.message = message;
      return this;
   }

   public int getTextMessageSize() {
      return textMessageSize;
   }

   public Producer setTextMessageSize(int textMessageSize) {
      this.textMessageSize = textMessageSize;
      return this;
   }

   public int getObjectSize() {
      return objectSize;
   }

   public Producer setObjectSize(int objectSize) {
      this.objectSize = objectSize;
      return this;
   }

   public long getMsgTTL() {
      return msgTTL;
   }

   public Producer setMsgTTL(long msgTTL) {
      this.msgTTL = msgTTL;
      return this;
   }

   public String getMsgGroupID() {
      return msgGroupID;
   }

   public Producer setMsgGroupID(String msgGroupID) {
      this.msgGroupID = msgGroupID;
      return this;
   }

   public String getFile() {
      return file;
   }

   public Producer setFile(String file) {
      this.file = file;
      return this;
   }

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      ConnectionFactory factory = createConnectionFactory();

      try (Connection connection = factory.createConnection()) {

         // If we are reading from file, we process messages sequentially to guarantee ordering.  i.e. no thread creation.
         if (file != null) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination dest = getDestination(session);

            MessageProducer producer = session.createProducer(dest);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            int messageCount = 0;
            try {
               MessageSerializer serializer = getMessageSerializer();
               if (serializer == null) {
                  System.err.println("Error. Unable to instantiate serializer class: " + serializer);
                  return null;
               }

               InputStream in;
               try {
                  in = new FileInputStream(file);
               } catch (Exception e) {
                  System.err.println("Error: Unable to open file for reading\n" + e.getMessage());
                  return null;
               }

               serializer.setInput(in, session);
               serializer.start();

               Message message = serializer.read();

               while (message != null) {
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
               Destination dest = getDestination(session);
               threadsArray[i] = new ProducerThread(session, dest, i);

               threadsArray[i]
                  .setVerbose(verbose)
                  .setSleep(sleep)
                  .setPersistent(!nonpersistent)
                  .setMessageSize(messageSize)
                  .setTextMessageSize(textMessageSize)
                  .setMessage(message)
                  .setObjectSize(objectSize)
                  .setMsgTTL(msgTTL)
                  .setMsgGroupID(msgGroupID)
                  .setTransactionBatchSize(txBatchSize)
                  .setMessageCount(messageCount);
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
}
