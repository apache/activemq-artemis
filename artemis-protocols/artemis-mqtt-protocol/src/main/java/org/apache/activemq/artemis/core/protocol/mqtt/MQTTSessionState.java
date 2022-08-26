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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.settings.impl.Match;
import org.jboss.logging.Logger;

public class MQTTSessionState {

   private static final Logger logger = Logger.getLogger(MQTTSessionState.class);

   public static final MQTTSessionState DEFAULT = new MQTTSessionState(null);

   private MQTTSession session;

   private String clientId;

   private final ConcurrentMap<String, Pair<MqttTopicSubscription, Integer>> subscriptions = new ConcurrentHashMap<>();

   // Used to store Packet ID of Publish QoS1 and QoS2 message.  See spec: 4.3.3 QoS 2: Exactly once delivery.  Method B.
   private final Map<Integer, MQTTMessageInfo> messageRefStore = new ConcurrentHashMap<>();

   private final ConcurrentMap<String, Map<Long, Integer>> addressMessageMap = new ConcurrentHashMap<>();

   private final Set<Integer> pubRec = new HashSet<>();

   private boolean attached = false;

   private long disconnectedTime = 0;

   private final OutboundStore outboundStore = new OutboundStore();

   private int clientSessionExpiryInterval;

   private boolean isWill = false;

   private ByteBuf willMessage;

   private String willTopic;

   private int willQoSLevel;

   private boolean willRetain = false;

   private long willDelayInterval = 0;

   private List<? extends MqttProperties.MqttProperty> willUserProperties;

   private WillStatus willStatus = WillStatus.NOT_SENT;

   private boolean failed = false;

   private int clientMaxPacketSize = 0;

   private Map<Integer, String> clientTopicAliases;

   private Integer clientTopicAliasMaximum;

   private Map<String, Integer> serverTopicAliases;

   public MQTTSessionState(String clientId) {
      this.clientId = clientId;
   }

   public MQTTSession getSession() {
      return session;
   }

   public void setSession(MQTTSession session) {
      this.session = session;
   }

   public synchronized void clear() {
      subscriptions.clear();
      messageRefStore.clear();
      addressMessageMap.clear();
      pubRec.clear();
      outboundStore.clear();
      disconnectedTime = 0;
      if (willMessage != null) {
         willMessage.clear();
         willMessage = null;
      }
      willStatus = WillStatus.NOT_SENT;
      failed = false;
      willDelayInterval = 0;
      willRetain = false;
      willTopic = null;
      clientMaxPacketSize = 0;
      clearTopicAliases();
      clientTopicAliasMaximum = 0;
   }

   public OutboundStore getOutboundStore() {
      return outboundStore;
   }

   public Set<Integer> getPubRec() {
      return pubRec;
   }

   public boolean isAttached() {
      return attached;
   }

   public void setAttached(boolean attached) {
      this.attached = attached;
   }

   public Collection<MqttTopicSubscription> getSubscriptions() {
      Collection<MqttTopicSubscription> result = new HashSet<>();
      for (Pair<MqttTopicSubscription, Integer> pair : subscriptions.values()) {
         result.add(pair.getA());
      }
      return result;
   }

   public boolean addSubscription(MqttTopicSubscription subscription, WildcardConfiguration wildcardConfiguration, Integer subscriptionIdentifier) {
      // synchronized to prevent race with removeSubscription
      synchronized (subscriptions) {
         addressMessageMap.putIfAbsent(MQTTUtil.convertMqttTopicFilterToCoreAddress(subscription.topicName(), wildcardConfiguration), new ConcurrentHashMap<>());

         Pair<MqttTopicSubscription, Integer> existingSubscription = subscriptions.get(subscription.topicName());
         if (existingSubscription != null) {
            boolean updated = false;
            if (subscription.qualityOfService().value() > existingSubscription.getA().qualityOfService().value()) {
               existingSubscription.setA(subscription);
               updated = true;
            }
            if (subscriptionIdentifier != null && !subscriptionIdentifier.equals(existingSubscription.getB())) {
               existingSubscription.setB(subscriptionIdentifier);
               updated = true;
            }
            return updated;
         } else {
            subscriptions.put(subscription.topicName(), new Pair<>(subscription, subscriptionIdentifier));
            return true;
         }
      }
   }

   public void removeSubscription(String address) {
      // synchronized to prevent race with addSubscription
      synchronized (subscriptions) {
         subscriptions.remove(address);
         addressMessageMap.remove(address);
      }
   }

   public MqttTopicSubscription getSubscription(String address) {
      return subscriptions.get(address) != null ? subscriptions.get(address).getA() : null;
   }

   public List<Integer> getMatchingSubscriptionIdentifiers(String address) {
      address = MQTTUtil.convertCoreAddressToMqttTopicFilter(address, session.getServer().getConfiguration().getWildcardConfiguration());
      List<Integer> result = null;
      for (Pair<MqttTopicSubscription, Integer> pair : subscriptions.values()) {
         Pattern pattern = Match.createPattern(pair.getA().topicName(), MQTTUtil.MQTT_WILDCARD, true);
         boolean matches = pattern.matcher(address).matches();
         logger.debugf("Matching %s with %s: %s", address, pattern, matches);
         if (matches) {
            if (result == null) {
               result = new ArrayList<>();
            }
            if (pair.getB() != null) {
               result.add(pair.getB());
            }
         }
      }
      return result;
   }

   public String getClientId() {
      return clientId;
   }

   public void setClientId(String clientId) {
      this.clientId = clientId;
   }

   public long getDisconnectedTime() {
      return disconnectedTime;
   }

   public void setDisconnectedTime(long disconnectedTime) {
      this.disconnectedTime = disconnectedTime;
   }

   public int getClientSessionExpiryInterval() {
      return clientSessionExpiryInterval;
   }

   public void setClientSessionExpiryInterval(int sessionExpiryInterval) {
      this.clientSessionExpiryInterval = sessionExpiryInterval;
   }

   public boolean isWill() {
      return isWill;
   }

   public void setWill(boolean will) {
      isWill = will;
   }

   public ByteBuf getWillMessage() {
      return willMessage;
   }

   public void setWillMessage(ByteBuf willMessage) {
      this.willMessage = willMessage;
   }

   public String getWillTopic() {
      return willTopic;
   }

   public void setWillTopic(String willTopic) {
      this.willTopic = willTopic;
   }

   public int getWillQoSLevel() {
      return willQoSLevel;
   }

   public void setWillQoSLevel(int willQoSLevel) {
      this.willQoSLevel = willQoSLevel;
   }

   public boolean isWillRetain() {
      return willRetain;
   }

   public void setWillRetain(boolean willRetain) {
      this.willRetain = willRetain;
   }

   public long getWillDelayInterval() {
      return willDelayInterval;
   }

   public void setWillDelayInterval(long willDelayInterval) {
      this.willDelayInterval = willDelayInterval;
   }

   public void setWillUserProperties(List<? extends MqttProperties.MqttProperty> userProperties) {
      this.willUserProperties = userProperties;
   }

   public List<? extends MqttProperties.MqttProperty> getWillUserProperties() {
      return willUserProperties;
   }

   public WillStatus getWillStatus() {
      return willStatus;
   }

   public void setWillStatus(WillStatus willStatus) {
      this.willStatus = willStatus;
   }

   public boolean isFailed() {
      return failed;
   }

   public void setFailed(boolean failed) {
      this.failed = failed;
   }

   public int getClientMaxPacketSize() {
      return clientMaxPacketSize;
   }

   public void setClientMaxPacketSize(int clientMaxPacketSize) {
      this.clientMaxPacketSize = clientMaxPacketSize;
   }

   public void addClientTopicAlias(Integer alias, String topicName) {
      if (clientTopicAliases == null) {
         clientTopicAliases = new HashMap<>();
      }
      clientTopicAliases.put(alias, topicName);
   }

   public String getClientTopicAlias(Integer alias) {
      String result;

      if (clientTopicAliases == null) {
         result = null;
      } else {
         result = clientTopicAliases.get(alias);
      }

      return result;
   }

   public Integer getClientTopicAliasMaximum() {
      return clientTopicAliasMaximum;
   }

   public void setClientTopicAliasMaximum(Integer clientTopicAliasMaximum) {
      this.clientTopicAliasMaximum = clientTopicAliasMaximum;
   }

   public Integer addServerTopicAlias(String topicName) {
      if (serverTopicAliases == null) {
         serverTopicAliases = new ConcurrentHashMap<>();
      }
      Integer alias = serverTopicAliases.size() + 1;
      if (alias <= clientTopicAliasMaximum) {
         serverTopicAliases.put(topicName, alias);
         return alias;
      } else {
         return null;
      }
   }

   public Integer getServerTopicAlias(String topicName) {
      return serverTopicAliases == null ? null : serverTopicAliases.get(topicName);
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

   public void clearTopicAliases() {
      if (clientTopicAliases != null) {
         clientTopicAliases.clear();
         clientTopicAliases = null;
      }
      if (serverTopicAliases != null) {
         serverTopicAliases.clear();
         serverTopicAliases = null;
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

      public int getPendingMessages() {
         synchronized (dataStoreLock) {
            return mqttToServerIds.size();
         }
      }

      public void clear() {
         synchronized (dataStoreLock) {
            artemisToMqttMessageMap.clear();
            mqttToServerIds.clear();
            ids.set(0);
         }
      }
   }

   @Override
   public String toString() {
      return "MQTTSessionState[" + "session=" + session + ", clientId='" + clientId + "', subscriptions=" + subscriptions + ", messageRefStore=" + messageRefStore + ", addressMessageMap=" + addressMessageMap + ", pubRec=" + pubRec + ", attached=" + attached + ", outboundStore=" + outboundStore + ", disconnectedTime=" + disconnectedTime + ", sessionExpiryInterval=" + clientSessionExpiryInterval + ", isWill=" + isWill + ", willMessage=" + willMessage + ", willTopic='" + willTopic + "', willQoSLevel=" + willQoSLevel + ", willRetain=" + willRetain + ", willDelayInterval=" + willDelayInterval + ", failed=" + failed + ", maxPacketSize=" + clientMaxPacketSize + ']';
   }

   public enum WillStatus {
      NOT_SENT, SENT, SENDING;

      public byte getStatus() {
         switch (this) {
            case NOT_SENT:
               return 0;
            case SENT:
               return 1;
            case SENDING:
               return 2;
            default:
               return -1;
         }
      }

      public static WillStatus getStatus(byte status) {
         switch (status) {
            case 0:
               return NOT_SENT;
            case 1:
               return SENT;
            case 2:
               return SENDING;
            default:
               return null;
         }
      }
   }
}
