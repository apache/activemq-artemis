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
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import java.io.FileOutputStream;
import java.io.OutputStream;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.factory.serialize.MessageSerializer;

@Command(name = "consumer", description = "It will consume messages from an instance")
public class Consumer extends DestAbstract {

   @Option(name = "--durable", description = "It will use durable subscription in case of client")
   boolean durable = false;

   @Option(name = "--break-on-null", description = "It will break on null messages")
   boolean breakOnNull = false;

   @Option(name = "--receive-timeout", description = "Time used on receive(timeout)")
   int receiveTimeout = 3000;

   @Option(name = "--filter", description = "filter to be used with the consumer")
   String filter;

   @Option(name = "--data", description = "serialize the messages to the specified file as they are consumed")
   String file;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      System.out.println("Consumer:: filter = " + filter);

      SerialiserMessageListener listener = null;
      MessageSerializer serializer = null;
      if (file != null) {
         serializer = getMessageSerializer();
         if (serializer == null) {
            System.err.println("Error. Unable to instantiate serializer class: " + this.serializer);
            return null;
         }

         OutputStream out;
         try {
            out = new FileOutputStream(file);
         } catch (Exception e) {
            System.err.println("Error: Unable to open file for writing\n" + e.getMessage());
            return null;
         }

         listener = new SerialiserMessageListener(serializer, out);
         serializer.start();
      }

      ConnectionFactory factory = createConnectionFactory();

      try (Connection connection = factory.createConnection()) {
         // We read messages in a single thread when persisting to file.
         ConsumerThread[] threadsArray = new ConsumerThread[threads];
         for (int i = 0; i < threads; i++) {
            Session session;
            if (txBatchSize > 0) {
               session = connection.createSession(true, Session.SESSION_TRANSACTED);
            } else {
               session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            }

            Destination dest = getDestination(session);
            threadsArray[i] = new ConsumerThread(session, dest, i);

            threadsArray[i]
               .setVerbose(verbose)
               .setSleep(sleep)
               .setDurable(durable)
               .setBatchSize(txBatchSize)
               .setBreakOnNull(breakOnNull)
               .setMessageCount(messageCount)
               .setReceiveTimeOut(receiveTimeout)
               .setFilter(filter)
               .setBrowse(false)
               .setListener(listener);
         }

         for (ConsumerThread thread : threadsArray) {
            thread.start();
         }

         connection.start();

         int received = 0;

         for (ConsumerThread thread : threadsArray) {
            thread.join();
            received += thread.getReceived();
         }

         if (serializer != null) {
            serializer.stop();
         }

         return received;
      }
   }

   private class SerialiserMessageListener implements MessageListener {

      private MessageSerializer messageSerializer;

      SerialiserMessageListener(MessageSerializer messageSerializer, OutputStream outputStream) throws Exception {
         this.messageSerializer = messageSerializer;
         this.messageSerializer.setOutput(outputStream);
      }

      @Override
      public void onMessage(Message message) {
         messageSerializer.write(message);
      }
   }

   public boolean isDurable() {
      return durable;
   }

   public Consumer setDurable(boolean durable) {
      this.durable = durable;
      return this;
   }

   public boolean isBreakOnNull() {
      return breakOnNull;
   }

   public Consumer setBreakOnNull(boolean breakOnNull) {
      this.breakOnNull = breakOnNull;
      return this;
   }

   public int getReceiveTimeout() {
      return receiveTimeout;
   }

   public Consumer setReceiveTimeout(int receiveTimeout) {
      this.receiveTimeout = receiveTimeout;
      return this;
   }

   public String getFilter() {
      return filter;
   }

   public Consumer setFilter(String filter) {
      this.filter = filter;
      return this;
   }

   public String getFile() {
      return file;
   }

   public Consumer setFile(String file) {
      this.file = file;
      return this;
   }
}
