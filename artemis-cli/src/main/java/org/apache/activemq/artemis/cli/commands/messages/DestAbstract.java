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
import javax.jms.Session;
import java.nio.ByteBuffer;

import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientRequestor;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.cli.factory.serialize.MessageSerializer;
import org.apache.activemq.artemis.cli.factory.serialize.XMLMessageSerializer;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;

public class DestAbstract extends ConnectionAbstract {

   public static final String DEFAULT_MESSAGE_SERIALIZER = "org.apache.activemq.artemis.cli.factory.serialize.XMLMessageSerializer";

   private static final String FQQN_PREFIX = "fqqn://";

   private static final String FQQN_SEPERATOR = "::";

   @Option(name = "--destination", description = "Destination to be used. It can be prefixed with queue:// or topic:// or fqqn:// (Default: queue://TEST)")
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

   protected boolean isFQQN() throws ActiveMQException {
      boolean fqqn = destination.contains("::");
      if (fqqn) {
         if (!destination.startsWith("fqqn://")) {
            throw new ActiveMQException("FQQN destinations must start with the fqqn:// prefix");
         }

         if (protocol.equalsIgnoreCase("AMQP")) {
            throw new ActiveMQException("Sending to FQQN destinations is not support via AMQP protocol");
         }
         return true;
      } else {
         return false;
      }
   }

   protected Destination lookupDestination(Session session) throws Exception {
      if (protocol.equals("AMQP")) {
         return session.createQueue(destination);
      } else {
         return ActiveMQDestination.createDestination(this.destination, ActiveMQDestination.TYPE.QUEUE);
      }
   }

   protected MessageSerializer getMessageSerializer() {
      if (serializer == null) return new XMLMessageSerializer();
      try {
         return (MessageSerializer) Class.forName(serializer).getConstructor().newInstance();
      } catch (Exception e) {
         System.out.println("Error: unable to instantiate serializer class: " + serializer);
         System.out.println("Defaulting to: " + DEFAULT_MESSAGE_SERIALIZER);
      }
      return new XMLMessageSerializer();
   }

   // FIXME We currently do not support producing to FQQN.  This is a work around.
   private ClientSession getManagementSession() throws Exception {
      ServerLocator serverLocator = ActiveMQClient.createServerLocator(brokerURL);
      ClientSessionFactory sf = serverLocator.createSessionFactory();

      ClientSession managementSession;
      if (user != null || password != null) {
         managementSession = sf.createSession(user, password, false, true, true, false, 0);
      } else {
         managementSession = sf.createSession(false, true, true);
      }
      return managementSession;
   }

   public byte[] getQueueIdFromName(String queueName) throws Exception {
      try {
         ClientMessage message = getQueueAttribute(queueName, "ID");
         Number idObject = (Number) ManagementHelper.getResult(message);
         ByteBuffer byteBuffer = ByteBuffer.allocate(8);
         byteBuffer.putLong(idObject.longValue());
         return byteBuffer.array();
      } catch (Exception e) {
         throw new ActiveMQException("Error occured when looking up FQQN.  Please ensure the FQQN exists.", e, ActiveMQExceptionType.ILLEGAL_STATE);
      }
   }

   protected ClientMessage getQueueAttribute(String queueName, String attribute) throws Exception {
      ClientSession managementSession = getManagementSession();
      managementSession.start();

      try (ClientRequestor requestor = new ClientRequestor(managementSession, "activemq.management")) {
         ClientMessage managementMessage = managementSession.createMessage(false);
         ManagementHelper.putAttribute(managementMessage, ResourceNames.QUEUE + queueName, attribute);
         managementSession.start();
         ClientMessage reply = requestor.request(managementMessage);
         return reply;
      } finally {
         managementSession.stop();
      }
   }

   protected String getQueueFromFQQN(String fqqn) {
      return fqqn.substring(fqqn.indexOf(FQQN_SEPERATOR) + FQQN_SEPERATOR.length());
   }

   protected String getAddressFromFQQN(String fqqn) {
      return fqqn.substring(fqqn.indexOf(FQQN_PREFIX) + FQQN_PREFIX.length(), fqqn.indexOf(FQQN_SEPERATOR));
   }

   protected String getFQQNFromDestination(String destination) {
      return destination.substring(destination.indexOf(FQQN_PREFIX) + FQQN_PREFIX.length());
   }
}
