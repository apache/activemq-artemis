/**
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

package org.apache.activemq.artemis.core.protocol.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;

/**
 * Handles MQTT Exactly Once (QoS level 2) Protocol.
 */
public class MQTTPublishManager
{
   private static final String MANAGEMENT_QUEUE_PREFIX = "$sys.mqtt.queue.qos2.";

   private SimpleString managementAddress;

   private ServerConsumer managementConsumer;

   private MQTTSession session;

   private MQTTLogger log = MQTTLogger.LOGGER;

   private final Object lock = new Object();

   public MQTTPublishManager(MQTTSession session)
   {
      this.session = session;
   }

   synchronized void start() throws Exception
   {
      createManagementAddress();
      createManagementQueue();
      createManagementConsumer();
   }

   synchronized void stop(boolean clean) throws Exception
   {
      if (managementConsumer != null)
      {
         managementConsumer.removeItself();
         managementConsumer.setStarted(false);
         managementConsumer.close(false);
         if (clean) session.getServer().destroyQueue(managementAddress);
      }
   }

   private void createManagementConsumer() throws Exception
   {
      long consumerId = session.getServer().getStorageManager().generateID();
      managementConsumer = session.getServerSession().createConsumer(consumerId, managementAddress, null, false, false, -1);
      managementConsumer.setStarted(true);
   }

   private void createManagementAddress()
   {
      String clientId = session.getSessionState().getClientId();
      managementAddress = new SimpleString(MANAGEMENT_QUEUE_PREFIX + clientId);
   }

   private void createManagementQueue() throws Exception
   {
      if (session.getServer().locateQueue(managementAddress) == null)
      {
         session.getServerSession().createQueue(managementAddress, managementAddress, null, false, MQTTUtil.DURABLE_MESSAGES);
      }
   }

   boolean isManagementConsumer(ServerConsumer consumer)
   {
      return consumer == managementConsumer;
   }

   private int generateMqttId(int qos)
   {
      if (qos == 1)
      {
         return session.getSessionState().generateId();
      }
      else
      {
         Integer mqttid = session.getSessionState().generateId();
         if (mqttid == null)
         {
            mqttid = (int) session.getServer().getStorageManager().generateID();
         }
         return mqttid;
      }
   }

   /** Since MQTT Subscriptions can over lap; a client may receive the same message twice.  When this happens the client
    * returns a PubRec or PubAck with ID.  But we need to know which consumer to ack, since we only have the ID to go on we
    * are not able to decide which consumer to ack.  Instead we send MQTT messages with different IDs and store a reference
    * to original ID and consumer in the Session state.  This way we can look up the consumer Id and the message Id from
    * the PubAck or PubRec message id. **/
   protected void sendMessage(ServerMessage message, ServerConsumer consumer, int deliveryCount) throws Exception
   {
      // This is to allow retries of PubRel.
      if (isManagementConsumer(consumer))
      {
         sendPubRelMessage(message);
      }
      else
      {
         int qos = decideQoS(message, consumer);
         if (qos == 0)
         {
            sendServerMessage((int) message.getMessageID(), (ServerMessageImpl) message, deliveryCount, qos);
            session.getServerSession().acknowledge(consumer.getID(), message.getMessageID());
         }
         else
         {
            String consumerAddress = consumer.getQueue().getAddress().toString();
            Integer mqttid = generateMqttId(qos);

            session.getSessionState().addOutbandMessageRef(mqttid, consumerAddress, message.getMessageID(), qos);
            sendServerMessage(mqttid, (ServerMessageImpl) message, deliveryCount, qos);
         }
      }
   }

   // INBOUND
   void handleMessage(int messageId, String topic, int qos, ByteBuf payload, boolean retain) throws Exception
   {
      synchronized (lock)
      {
         ServerMessage serverMessage = MQTTUtil.createServerMessageFromByteBuf(session, topic, retain, qos, payload);

         if (qos > 0)
         {
            serverMessage.setDurable(MQTTUtil.DURABLE_MESSAGES);
         }

         if (qos < 2 || !session.getSessionState().getPubRec().contains(messageId))
         {
            if (qos == 2) session.getSessionState().getPubRec().add(messageId);
            session.getServerSession().send(serverMessage, true);
         }


         if (retain)
         {
            boolean reset = payload instanceof EmptyByteBuf || payload.capacity() == 0;
            session.getRetainMessageManager().handleRetainedMessage(serverMessage, topic, reset);
         }

         createMessageAck(messageId, qos);
      }
   }

   void sendPubRelMessage(ServerMessage message)
   {
      if (message.getIntProperty(MQTTUtil.MQTT_MESSAGE_TYPE_KEY) == MqttMessageType.PUBREL.value())
      {
         int messageId = message.getIntProperty(MQTTUtil.MQTT_MESSAGE_ID_KEY);
         MQTTMessageInfo messageInfo = new MQTTMessageInfo(message.getMessageID(), managementConsumer.getID(), message.getAddress().toString());
         session.getSessionState().storeMessageRef(messageId, messageInfo, false);
         session.getProtocolHandler().sendPubRel(messageId);
      }
   }

   private void createMessageAck(final int messageId, final int qos)
   {
      session.getServer().getStorageManager().afterCompleteOperations(new IOCallback()
      {
         @Override
         public void done()
         {
            if (qos == 1)
            {
               session.getProtocolHandler().sendPubAck(messageId);
            }
            else if (qos == 2)
            {
               session.getProtocolHandler().sendPubRec(messageId);
            }
         }

         @Override
         public void onError(int errorCode, String errorMessage)
         {
            log.error("Pub Sync Failed");
         }
      });
   }

   void handlePubRec(int messageId) throws Exception
   {
      MQTTMessageInfo messageRef = session.getSessionState().getMessageInfo(messageId);
      if (messageRef != null)
      {
         ServerMessage pubRel = MQTTUtil.createPubRelMessage(session, managementAddress, messageId);
         session.getServerSession().send(pubRel, true);
         session.getServerSession().acknowledge(messageRef.getConsumerId(), messageRef.getServerMessageId());
         session.getProtocolHandler().sendPubRel(messageId);
      }
   }

   void handlePubComp(int messageId) throws Exception
   {
      MQTTMessageInfo messageInfo = session.getSessionState().getMessageInfo(messageId);

      // Check to see if this message is stored if not just drop the packet.
      if (messageInfo != null)
      {
         session.getServerSession().acknowledge(managementConsumer.getID(), messageInfo.getServerMessageId());
      }
   }

   void handlePubRel(int messageId)
   {
      // We don't check to see if a PubRel existed for this message.  We assume it did and so send PubComp.
      session.getSessionState().getPubRec().remove(messageId);
      session.getProtocolHandler().sendPubComp(messageId);
      session.getSessionState().removeMessageRef(messageId);
   }


   void handlePubAck(int messageId) throws Exception
   {
      Pair<String, Long> pub1MessageInfo = session.getSessionState().removeOutbandMessageRef(messageId, 1);
      if (pub1MessageInfo != null)
      {
         String mqttAddress = MQTTUtil.convertCoreAddressFilterToMQTT(pub1MessageInfo.getA());
         ServerConsumer consumer = session.getSubscriptionManager().getConsumerForAddress(mqttAddress);
         session.getServerSession().acknowledge(consumer.getID(), pub1MessageInfo.getB());
      }
   }

   private void sendServerMessage(int messageId, ServerMessageImpl message, int deliveryCount, int qos)
   {
      String address = MQTTUtil.convertCoreAddressFilterToMQTT(message.getAddress().toString()).toString();

      //FIXME should we be copying the body buffer here?
      ByteBuf payload = message.getBodyBufferCopy().byteBuf();
      session.getProtocolHandler().send(messageId, address, qos, payload, deliveryCount);
   }

   private int decideQoS(ServerMessage message, ServerConsumer consumer)
   {
      int subscriptionQoS = session.getSubscriptionManager().getConsumerQoSLevels().get(consumer.getID());
      int qos = message.getIntProperty(MQTTUtil.MQTT_QOS_LEVEL_KEY);

      /* Subscription QoS is the maximum QoS the client is willing to receive for this subscription.  If the message QoS
      is less than the subscription QoS then use it, otherwise use the subscription qos). */
      return subscriptionQoS < qos ? subscriptionQoS : qos;
   }
}