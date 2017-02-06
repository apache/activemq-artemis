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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.jctools.maps.NonBlockingHashMap;
import org.jctools.maps.NonBlockingHashMapLong;

public class MQTTSessionState {

   private String clientId;

   private ServerMessage willMessage;

   private final ConcurrentMap<String, MqttTopicSubscription> subscriptions = new NonBlockingHashMap<>();

   // Used to store Packet ID of Publish QoS1 and QoS2 message.  See spec: 4.3.3 QoS 2: Exactly once delivery.  Method B.
   private NonBlockingHashMapLong<MQTTMessageInfo> messageRefStore;

   private ConcurrentMap<String, NonBlockingHashMapLong<Integer>> addressMessageMap;

   private Set<Integer> pubRec;

   private Set<Integer> pub;

   private boolean attached = false;

   // Objects track the Outbound message references
   private NonBlockingHashMapLong<Pair<String, Long>> outboundMessageReferenceStore;

   private ConcurrentMap<String, ConcurrentMap<Long, Integer>> reverseOutboundReferenceStore;

   private final Object outboundLock = new Object();

   // FIXME We should use a better mechanism for creating packet IDs.
   private AtomicInteger lastId = new AtomicInteger(0);

   private final OutboundStore outboundStore = new OutboundStore();

   public MQTTSessionState(String clientId) {
      this.clientId = clientId;

      pubRec = new HashSet<>();
      pub = new HashSet<>();

      outboundMessageReferenceStore = new NonBlockingHashMapLong<>();
      reverseOutboundReferenceStore = new NonBlockingHashMap<>();

      messageRefStore = new NonBlockingHashMapLong<>();
      addressMessageMap = new NonBlockingHashMap<>();
   }

   OutboundStore getOutboundStore() {
      return outboundStore;
   }

   Set<Integer> getPubRec() {
      return pubRec;
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
         addressMessageMap.putIfAbsent(MQTTUtil.convertMQTTAddressFilterToCore(subscription.topicName()), new NonBlockingHashMapLong<>());

         MqttTopicSubscription existingSubscription = subscriptions.get(subscription.topicName());
         if (existingSubscription != null) {
            if (subscription.qualityOfService().value() > existingSubscription.qualityOfService().value()) {
               subscriptions.put(subscription.topicName(), subscription);
               return true;
            }
         } else {
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

   void removeMessageRef(Integer mqttId) {
      MQTTMessageInfo info = messageRefStore.remove(mqttId);
      if (info != null) {
         Map<Long, Integer> addressMap = addressMessageMap.get(info.getAddress());
         if (addressMap != null) {
            addressMap.remove(info.getServerMessageId());
         }
      }
   }

   public class OutboundStore {

      private final HashMap<String, Integer> artemisToMqttMessageMap = new HashMap<>();

      private final HashMap<Integer, Pair<Long, Long>> mqttToServerIds = new HashMap<>();

      private final Object dataStoreLock = new Object();

      private final AtomicInteger ids = new AtomicInteger(0);

      public int generateMqttId(long serverId, long consumerId) {
         synchronized (dataStoreLock) {
            Integer id = artemisToMqttMessageMap.get(consumerId + ":" + serverId);
            if (id == null) {
               ids.compareAndSet(Short.MAX_VALUE, 1);
               id = ids.addAndGet(1);
            }
            return id;
         }
      }

      public void publish(int mqtt, long serverId, long consumerId) {
         synchronized (dataStoreLock) {
            artemisToMqttMessageMap.put(consumerId + ":" + serverId, mqtt);
            mqttToServerIds.put(mqtt, new Pair(serverId, consumerId));
         }
      }

      public Pair<Long, Long> publishAckd(int mqtt) {
         synchronized (dataStoreLock) {
            Pair p =  mqttToServerIds.remove(mqtt);
            if (p != null) {
               mqttToServerIds.remove(p.getA());
            }
            return p;
         }
      }

      public Pair<Long, Long> publishReceived(int mqtt) {
         return publishAckd(mqtt);
      }

      public Pair<Long, Long> publishComplete(int mqtt) {
         return publishAckd(mqtt);
      }
   }
}
