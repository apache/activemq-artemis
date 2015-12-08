/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.mqtt;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.server.ServerMessage;

public class MQTTSessionState {

   private String clientId;

   private ServerMessage willMessage;

   private final ConcurrentMap<String, MqttTopicSubscription> subscriptions = new ConcurrentHashMap<>();

   // Used to store Packet ID of Publish QoS1 and QoS2 message.  See spec: 4.3.3 QoS 2: Exactly once delivery.  Method B.
   private Map<Integer, MQTTMessageInfo> messageRefStore;

   private ConcurrentMap<String, Map<Long, Integer>> addressMessageMap;

   private Set<Integer> pubRec;

   private Set<Integer> pub;

   private boolean attached = false;

   private MQTTLogger log = MQTTLogger.LOGGER;

   // Objects track the Outbound message references
   private Map<Integer, Pair<String, Long>> outboundMessageReferenceStore;

   private ConcurrentMap<String, ConcurrentMap<Long, Integer>> reverseOutboundReferenceStore;

   private final Object outboundLock = new Object();

   // FIXME We should use a better mechanism for creating packet IDs.
   private AtomicInteger lastId = new AtomicInteger(0);

   public MQTTSessionState(String clientId) {
      this.clientId = clientId;

      pubRec = new HashSet<>();
      pub = new HashSet<>();

      outboundMessageReferenceStore = new ConcurrentHashMap<>();
      reverseOutboundReferenceStore = new ConcurrentHashMap<>();

      messageRefStore = new ConcurrentHashMap<>();
      addressMessageMap = new ConcurrentHashMap<>();
   }

   int generateId() {
      lastId.compareAndSet(Short.MAX_VALUE, 1);
      return lastId.addAndGet(1);
   }

   void addOutbandMessageRef(int mqttId, String address, long serverMessageId, int qos) {
      synchronized (outboundLock) {
         outboundMessageReferenceStore.put(mqttId, new Pair<>(address, serverMessageId));
         if (qos == 2) {
            if (reverseOutboundReferenceStore.containsKey(address)) {
               reverseOutboundReferenceStore.get(address).put(serverMessageId, mqttId);
            }
            else {
               ConcurrentHashMap<Long, Integer> serverToMqttId = new ConcurrentHashMap<>();
               serverToMqttId.put(serverMessageId, mqttId);
               reverseOutboundReferenceStore.put(address, serverToMqttId);
            }
         }
      }
   }

   Pair<String, Long> removeOutbandMessageRef(int mqttId, int qos) {
      synchronized (outboundLock) {
         Pair<String, Long> messageInfo = outboundMessageReferenceStore.remove(mqttId);
         if (qos == 1) {
            return messageInfo;
         }

         Map<Long, Integer> map = reverseOutboundReferenceStore.get(messageInfo.getA());
         if (map != null) {
            map.remove(messageInfo.getB());
            if (map.isEmpty()) {
               reverseOutboundReferenceStore.remove(messageInfo.getA());
            }
            return messageInfo;
         }
         return null;
      }
   }

   Set<Integer> getPubRec() {
      return pubRec;
   }

   Set<Integer> getPub() {
      return pub;
   }

   boolean getAttached() {
      return attached;
   }

   void setAttached(boolean attached) {
      this.attached = attached;
   }

   boolean isWill() {
      return willMessage != null;
   }

   ServerMessage getWillMessage() {
      return willMessage;
   }

   void setWillMessage(ServerMessage willMessage) {
      this.willMessage = willMessage;
   }

   void deleteWillMessage() {
      willMessage = null;
   }

   Collection<MqttTopicSubscription> getSubscriptions() {
      return subscriptions.values();
   }

   boolean addSubscription(MqttTopicSubscription subscription) {
      synchronized (subscriptions) {
         addressMessageMap.putIfAbsent(MQTTUtil.convertMQTTAddressFilterToCore(subscription.topicName()), new ConcurrentHashMap<Long, Integer>());

         MqttTopicSubscription existingSubscription = subscriptions.get(subscription.topicName());
         if (existingSubscription != null) {
            if (subscription.qualityOfService().value() > existingSubscription.qualityOfService().value()) {
               subscriptions.put(subscription.topicName(), subscription);
               return true;
            }
         }
         else {
            subscriptions.put(subscription.topicName(), subscription);
            return true;
         }
      }
      return false;
   }

   void removeSubscription(String address) {
      synchronized (subscriptions) {
         subscriptions.remove(address);
         addressMessageMap.remove(address);
      }
   }

   MqttTopicSubscription getSubscription(String address) {
      return subscriptions.get(address);
   }

   String getClientId() {
      return clientId;
   }

   void setClientId(String clientId) {
      this.clientId = clientId;
   }

   void storeMessageRef(Integer mqttId, MQTTMessageInfo messageInfo, boolean storeAddress) {
      messageRefStore.put(mqttId, messageInfo);
      if (storeAddress) {
         Map<Long, Integer> addressMap = addressMessageMap.get(messageInfo.getAddress());
         if (addressMap != null) {
            addressMap.put(messageInfo.getServerMessageId(), mqttId);
         }
      }
   }

   void removeMessageRef(Integer mqttId) {
      MQTTMessageInfo info = messageRefStore.remove(mqttId);
      if (info != null) {
         Map<Long, Integer> addressMap = addressMessageMap.get(info.getAddress());
         if (addressMap != null) {
            addressMap.remove(info.getServerMessageId());
         }
      }
   }

   MQTTMessageInfo getMessageInfo(Integer mqttId) {
      return messageRefStore.get(mqttId);
   }
}
