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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.factory.serialize.MessageSerializer;
import org.apache.activemq.artemis.cli.factory.serialize.XMLMessageSerializer;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;

public class DestAbstract extends ConnectionAbstract {

   @Option(name = "--destination", description = "Destination to be used. It can be prefixed with queue:// or topic:// and can be an FQQN in the form of <address>::<queue>. (Default: queue://TEST)")
   String destination = "queue://TEST";

   @Option(name = "--message-count", description = "Number of messages to act on (Default: 1000)")
   int messageCount = 1000;

   @Option(name = "--sleep", description = "Time wait between each message")
   int sleep = 0;

   @Option(name = "--txt-size", description = "TX Batch Size")
   int txBatchSize;

   @Option(name = "--threads", description = "Number of Threads to be used (Default: 1)")
   int threads = 1;

   @Option(name = "--serializer", description = "Override the default serializer with a custom implementation")
   String serializer;

   protected MessageSerializer getMessageSerializer() {
      if (serializer != null) {
         try {
            return (MessageSerializer) Class.forName(serializer).getConstructor().newInstance();
         } catch (Exception e) {
            System.err.println("Error: unable to instantiate serializer class: " + serializer);
            System.err.println("Defaulting to: " + XMLMessageSerializer.class.getName());
         }
      }

      if (!protocol.equalsIgnoreCase("CORE")) {
         System.err.println("Default Serializer does not support: " + protocol + " protocol");
         return null;
      }

      return new XMLMessageSerializer();
   }

   protected Destination getDestination(Session session) throws JMSException {
      if (destination.startsWith(ActiveMQDestination.TOPIC_QUALIFIED_PREFIX)) {
         return session.createTopic(stripPrefix(destination));
      }
      return session.createQueue(stripPrefix(destination));
   }

   private String stripPrefix(String destination) {
      int index = destination.indexOf("://");
      if (index != -1) {
         return destination.substring(index + 3);
      } else {
         return destination;
      }
   }

   public String getDestination() {
      return destination;
   }

   public DestAbstract setDestination(String destination) {
      this.destination = destination;
      return this;
   }

   public int getMessageCount() {
      return messageCount;
   }

   public DestAbstract setMessageCount(int messageCount) {
      this.messageCount = messageCount;
      return this;
   }

   public int getSleep() {
      return sleep;
   }

   public DestAbstract setSleep(int sleep) {
      this.sleep = sleep;
      return this;
   }

   public int getTxBatchSize() {
      return txBatchSize;
   }

   public DestAbstract setTxBatchSize(int txBatchSize) {
      this.txBatchSize = txBatchSize;
      return this;
   }

   public int getThreads() {
      return threads;
   }

   public DestAbstract setThreads(int threads) {
      this.threads = threads;
      return this;
   }

   public String getSerializer() {
      return serializer;
   }

   public DestAbstract setSerializer(String serializer) {
      this.serializer = serializer;
      return this;
   }
}
