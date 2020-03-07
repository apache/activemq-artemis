/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.mqtt;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;

public class MQTTSessionCallback implements SessionCallback {

   private final MQTTSession session;
   private final MQTTConnection connection;

   private MQTTLogger log = MQTTLogger.LOGGER;

   public MQTTSessionCallback(MQTTSession session, MQTTConnection connection) throws Exception {
      this.session = session;
      this.connection = connection;
   }

   @Override
   public boolean supportsDirectDelivery() {
      return false;
   }

   @Override
   public boolean isWritable(ReadyListener callback, Object protocolContext) {
      return connection.isWritable(callback);
   }

   @Override
   public int sendMessage(MessageReference reference,
                          Message message,
                          ServerConsumer consumer,
                          int deliveryCount) {
      try {
         session.getMqttPublishManager().sendMessage(message.toCore(), consumer, deliveryCount);
      } catch (Exception e) {
         log.warn("Unable to send message: " + message.getMessageID() + " Cause: " + e.getMessage(), e);
      }
      return 1;
   }

   @Override
   public boolean updateDeliveryCountAfterCancel(ServerConsumer consumer, MessageReference ref, boolean failed) {
      return false;
   }

   @Override
   public int sendLargeMessageContinuation(ServerConsumer consumerID,
                                           byte[] body,
                                           boolean continues,
                                           boolean requiresResponse) {
      return 1;
   }

   @Override
   public int sendLargeMessage(MessageReference reference,
                               Message message,
                               ServerConsumer consumer,
                               long bodySize,
                               int deliveryCount) {
      return sendMessage(reference, message, consumer, deliveryCount);
   }

   @Override
   public void disconnect(ServerConsumer consumer, SimpleString queueName) {
      try {
         consumer.removeItself();
      } catch (Exception e) {
         log.error(e.getMessage());
      }
   }

   @Override
   public void afterDelivery() throws Exception {

   }

   @Override
   public void browserFinished(ServerConsumer consumer) {

   }

   @Override
   public boolean hasCredits(ServerConsumer consumerID) {
      return true;
   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address) {
   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address) {
   }

   @Override
   public void closed() {
   }

}
