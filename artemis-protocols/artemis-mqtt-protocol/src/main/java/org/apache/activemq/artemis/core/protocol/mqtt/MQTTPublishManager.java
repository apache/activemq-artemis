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

import java.io.UnsupportedEncodingException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.EmptyByteBuf;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.core.transaction.Transaction;

/**
 * Handles MQTT Exactly Once (QoS level 2) Protocol.
 */
public class MQTTPublishManager {

   private static final String MANAGEMENT_QUEUE_PREFIX = "$sys.mqtt.queue.qos2.";

   private SimpleString managementAddress;

   private ServerConsumer managementConsumer;

   private MQTTSession session;

   private MQTTLogger log = MQTTLogger.LOGGER;

   private final Object lock = new Object();

   private MQTTSessionState state;

   private MQTTSessionState.OutboundStore outboundStore;

   public MQTTPublishManager(MQTTSession session) {
      this.session = session;
   }

   synchronized void start() throws Exception {
      this.state = session.getSessionState();
      this.outboundStore = state.getOutboundStore();

      createManagementAddress();
      createManagementQueue();
      createManagementConsumer();
   }

   synchronized void stop() throws Exception {
      if (managementConsumer != null) {
         managementConsumer.removeItself();
         managementConsumer.setStarted(false);
         managementConsumer.close(false);
      }
   }

   void clean() throws Exception {
      createManagementAddress();
      Queue queue = session.getServer().locateQueue(managementAddress);
      if (queue != null) {
         queue.deleteQueue();
      }
   }

   private void createManagementConsumer() throws Exception {
      long consumerId = session.getServer().getStorageManager().generateID();
      managementConsumer = session.getServerSession().createConsumer(consumerId, managementAddress, null, false, false, -1);
      managementConsumer.setStarted(true);
   }

   private void createManagementAddress() {
      managementAddress = new SimpleString(MANAGEMENT_QUEUE_PREFIX + session.getSessionState().getClientId());
   }

   private void createManagementQueue() throws Exception {
      Queue q = session.getServer().locateQueue(managementAddress);
      if (q == null) {
         session.getServerSession().createQueue(managementAddress, managementAddress, null, false, MQTTUtil.DURABLE_MESSAGES);
      }
   }

   boolean isManagementConsumer(ServerConsumer consumer) {
      return consumer == managementConsumer;
   }

   /**
    * Since MQTT Subscriptions can over lap; a client may receive the same message twice.  When this happens the client
    * returns a PubRec or PubAck with ID.  But we need to know which consumer to ack, since we only have the ID to go on we
    * are not able to decide which consumer to ack.  Instead we send MQTT messages with different IDs and store a reference
    * to original ID and consumer in the Session state.  This way we can look up the consumer Id and the message Id from
    * the PubAck or PubRec message id. *
    */
   protected void sendMessage(ServerMessage message, ServerConsumer consumer, int deliveryCount) throws Exception {
      // This is to allow retries of PubRel.
      if (isManagementConsumer(consumer)) {
         sendPubRelMessage(message);
      } else {
         int qos = decideQoS(message, consumer);
         if (qos == 0) {
            sendServerMessage((int) message.getMessageID(), (ServerMessageImpl) message, deliveryCount, qos);
            session.getServerSession().acknowledge(consumer.getID(), message.getMessageID());
         } else if (qos == 1 || qos == 2) {
            int mqttid = outboundStore.generateMqttId(message.getMessageID(), consumer.getID());
            outboundStore.publish(mqttid, message.getMessageID(), consumer.getID());
            sendServerMessage(mqttid, (ServerMessageImpl) message, deliveryCount, qos);
         } else {
            // Client must have disconnected and it's Subscription QoS cleared
            consumer.individualCancel(message.getMessageID(), false);
         }
      }
   }

   // INBOUND
   void handleMessage(int messageId, String topic, int qos, ByteBuf payload, boolean retain) throws Exception {
      sendInternal(messageId, topic, qos, payload, retain, false);
   }

   /**
    * Sends a message either on behalf of the client or on behalf of the broker (Will Messages)
    * @param messageId
    * @param topic
    * @param qos
    * @param payload
    * @param retain
    * @param internal if true means on behalf of the broker (skips authorisation) and does not return ack.
    * @throws Exception
    */
   void sendInternal(int messageId, String topic, int qos, ByteBuf payload, boolean retain, boolean internal) throws Exception {
      synchronized (lock) {
         ServerMessage serverMessage = MQTTUtil.createServerMessageFromByteBuf(session, topic, retain, qos, payload);

         if (qos > 0) {
            serverMessage.setDurable(MQTTUtil.DURABLE_MESSAGES);
         }

         if (qos < 2 || !state.getPubRec().contains(messageId)) {
            if (qos == 2 && !internal)
               state.getPubRec().add(messageId);

            Transaction tx = session.getServerSession().newTransaction();
            try {
               session.getServerSession().send(tx, serverMessage, true, false);
               if (retain) {
                  boolean reset = payload instanceof EmptyByteBuf || payload.capacity() == 0;
                  session.getRetainMessageManager().handleRetainedMessage(serverMessage, topic, reset, tx);
               }
               tx.commit();
            } catch (Throwable t) {
               tx.rollback();
               throw t;
            }
            createMessageAck(messageId, qos, internal);
         }
      }
   }

   void sendPubRelMessage(ServerMessage message) {
      int messageId = message.getIntProperty(MQTTUtil.MQTT_MESSAGE_ID_KEY);
      session.getProtocolHandler().sendPubRel(messageId);
   }

   void handlePubRec(int messageId) throws Exception {
      try {
         Pair<Long, Long> ref = outboundStore.publishReceived(messageId);
         if (ref != null) {
            ServerMessage m = MQTTUtil.createPubRelMessage(session, managementAddress, messageId);
            session.getServerSession().send(m, true);
            session.getServerSession().acknowledge(ref.getB(), ref.getA());
         } else {
            session.getProtocolHandler().sendPubRel(messageId);
         }
      } catch (ActiveMQIllegalStateException e) {
         log.warn("MQTT Client(" + session.getSessionState().getClientId() + ") attempted to Ack already Ack'd message");
      }
   }

   void handlePubComp(int messageId) throws Exception {
      Pair<Long, Long> ref = session.getState().getOutboundStore().publishComplete(messageId);
      if (ref != null) {
         session.getServerSession().acknowledge(ref.getB(), ref.getA());
      }
   }

   private void createMessageAck(final int messageId, final int qos, final boolean internal) {
      session.getServer().getStorageManager().afterCompleteOperations(new IOCallback() {
         @Override
         public void done() {
            if (!internal) {
               if (qos == 1) {
                  session.getProtocolHandler().sendPubAck(messageId);
               } else if (qos == 2) {
                  session.getProtocolHandler().sendPubRec(messageId);
               }
            }
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
            log.error("Pub Sync Failed");
         }
      });
   }

   void handlePubRel(int messageId) {
      // We don't check to see if a PubRel existed for this message.  We assume it did and so send PubComp.
      state.getPubRec().remove(messageId);
      session.getProtocolHandler().sendPubComp(messageId);
      state.removeMessageRef(messageId);
   }

   void handlePubAck(int messageId) throws Exception {
      try {
         Pair<Long, Long> ref = outboundStore.publishAckd(messageId);
         if (ref != null) {
            session.getServerSession().acknowledge(ref.getB(), ref.getA());
         }
      } catch (ActiveMQIllegalStateException e) {
         log.warn("MQTT Client(" + session.getSessionState().getClientId() + ") attempted to Ack already Ack'd message");
      }
   }

   private void sendServerMessage(int messageId, ServerMessageImpl message, int deliveryCount, int qos) {
      String address = MQTTUtil.convertCoreAddressFilterToMQTT(message.getAddress().toString(), session.getWildcardConfiguration());

      ByteBuf payload;
      switch (message.getType()) {
         case Message.TEXT_TYPE:
            try {
               SimpleString text = message.getBodyBuffer().readNullableSimpleString();
               byte[] stringPayload = text.toString().getBytes("UTF-8");
               payload = ByteBufAllocator.DEFAULT.buffer(stringPayload.length);
               payload.writeBytes(stringPayload);
               break;
            } catch (UnsupportedEncodingException e) {
               log.warn("Unable to send message: " + message.getMessageID() + " Cause: " + e.getMessage());
            }
         default:
            ActiveMQBuffer bufferDup = message.getBodyBufferDuplicate();
            payload = bufferDup.readBytes(message.getEndOfBodyPosition() - bufferDup.readerIndex()).byteBuf();
            break;
      }
      session.getProtocolHandler().send(messageId, address, qos, payload, deliveryCount);
   }

   private int decideQoS(ServerMessage message, ServerConsumer consumer) {

      int subscriptionQoS = -1;
      try {
         subscriptionQoS = session.getSubscriptionManager().getConsumerQoSLevels().get(consumer.getID());
      } catch (NullPointerException e) {
         // This can happen if the client disconnected during a server send.
         return subscriptionQoS;
      }

      int qos = 2;
      if (message.containsProperty(MQTTUtil.MQTT_QOS_LEVEL_KEY)) {
         qos = message.getIntProperty(MQTTUtil.MQTT_QOS_LEVEL_KEY);
      }

      /* Subscription QoS is the maximum QoS the client is willing to receive for this subscription.  If the message QoS
      is less than the subscription QoS then use it, otherwise use the subscription qos). */
      return subscriptionQoS < qos ? subscriptionQoS : qos;
   }
}
