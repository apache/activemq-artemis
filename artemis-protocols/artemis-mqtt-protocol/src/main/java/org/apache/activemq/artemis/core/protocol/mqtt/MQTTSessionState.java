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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;

public class MQTTSessionState {

   public static final MQTTSessionState DEFAULT = new MQTTSessionState(null);

   private String clientId;

   private final ConcurrentMap<String, MqttTopicSubscription> subscriptions = new ConcurrentHashMap<>();

   // Used to store Packet ID of Publish QoS1 and QoS2 message.  See spec: 4.3.3 QoS 2: Exactly once delivery.  Method B.
   private final Map<Integer, MQTTMessageInfo> messageRefStore = new ConcurrentHashMap<>();

   private final ConcurrentMap<String, Map<Long, Integer>> addressMessageMap = new ConcurrentHashMap<>();

   private final Set<Integer> pubRec = new HashSet<>();

   private boolean attached = false;

   private final OutboundStore outboundStore = new OutboundStore();

   public MQTTSessionState(String clientId) {
      this.clientId = clientId;
   }

   public synchronized void clear() {
      subscriptions.clear();
      messageRefStore.clear();
      addressMessageMap.clear();
      pubRec.clear();
      outboundStore.clear();
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

   Collection<MqttTopicSubscription> getSubscriptions() {
      return subscriptions.values();
   }

   boolean addSubscription(MqttTopicSubscription subscription, WildcardConfiguration wildcardConfiguration) {
      // synchronized to prevent race with removeSubscription
      synchronized (subscriptions) {
         addressMessageMap.putIfAbsent(MQTTUtil.convertMQTTAddressFilterToCore(subscription.topicName(), wildcardConfiguration), new ConcurrentHashMap<Long, Integer>());

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
      // synchronized to prevent race with addSubscription
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

      private HashMap<Pair<Long, Long>, Integer> artemisToMqttMessageMap = new HashMap<>();

      private HashMap<Integer, Pair<Long, Long>> mqttToServerIds = new HashMap<>();

      private final Object dataStoreLock = new Object();

      private final AtomicInteger ids = new AtomicInteger(0);

      private Pair<Long, Long> generateKey(long messageId, long consumerID) {
         return new Pair<>(messageId, consumerID);
      }

      public int generateMqttId(long messageId, long consumerId) {
         synchronized (dataStoreLock) {
            Integer id = artemisToMqttMessageMap.get(generateKey(messageId, consumerId));
            if (id == null) {
               ids.compareAndSet(Short.MAX_VALUE, 1);
               id = ids.addAndGet(1);
            }
            return id;
         }
      }

      public void publish(int mqtt, long messageId, long consumerId) {
         synchronized (dataStoreLock) {
            Pair<Long, Long> key = generateKey(messageId, consumerId);
            artemisToMqttMessageMap.put(key, mqtt);
            mqttToServerIds.put(mqtt, key);
         }
      }

      public Pair<Long, Long> publishAckd(int mqtt) {
         synchronized (dataStoreLock) {
            Pair p = mqttToServerIds.remove(mqtt);
            if (p != null) {
               mqttToServerIds.remove(p.getA());
               artemisToMqttMessageMap.remove(p);
            }
            return p;
         }
      }

      public Pair<Long, Long> publishReceived(int mqtt) {
         return publishAckd(mqtt);
      }

      public void publishReleasedSent(int mqttId, long serverMessageId) {
         synchronized (dataStoreLock) {
            mqttToServerIds.put(mqttId, new Pair<>(serverMessageId, 0L));
         }
      }

      public Pair<Long, Long> publishComplete(int mqtt) {
         return publishAckd(mqtt);
      }

      public void clear() {
         synchronized (dataStoreLock) {
            artemisToMqttMessageMap.clear();
            mqttToServerIds.clear();
            ids.set(0);
         }
      }
   }
}
