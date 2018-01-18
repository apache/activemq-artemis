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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.FilterConstants;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.BindingQueryResult;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.utils.CompositeAddress;

import io.netty.handler.codec.mqtt.MqttTopicSubscription;

public class MQTTSubscriptionManager {

   private final MQTTSession session;

   private final ConcurrentMap<Long, Integer> consumerQoSLevels;

   private final ConcurrentMap<String, ServerConsumer> consumers;

   // We filter out Artemis management messages and notifications
   private final SimpleString managementFilter;

   public MQTTSubscriptionManager(MQTTSession session) {
      this.session = session;

      consumers = new ConcurrentHashMap<>();
      consumerQoSLevels = new ConcurrentHashMap<>();

      // Create filter string to ignore management messages
      StringBuilder builder = new StringBuilder();
      builder.append("NOT ((");
      builder.append(FilterConstants.ACTIVEMQ_ADDRESS);
      builder.append(" = '");
      builder.append(session.getServer().getConfiguration().getManagementAddress());
      builder.append("') OR (");
      builder.append(FilterConstants.ACTIVEMQ_ADDRESS);
      builder.append(" = '");
      builder.append(session.getServer().getConfiguration().getManagementNotificationAddress());
      builder.append("'))");
      managementFilter = new SimpleString(builder.toString());
   }

   synchronized void start() throws Exception {
      for (MqttTopicSubscription subscription : session.getSessionState().getSubscriptions()) {
         String coreAddress = MQTTUtil.convertMQTTAddressFilterToCore(subscription.topicName(), session.getWildcardConfiguration());
         Queue q = createQueueForSubscription(coreAddress, subscription.qualityOfService().value());
         createConsumerForSubscriptionQueue(q, subscription.topicName(), subscription.qualityOfService().value());
      }
   }

   synchronized void stop() throws Exception {
      for (ServerConsumer consumer : consumers.values()) {
         consumer.setStarted(false);
         consumer.disconnect();
         consumer.getQueue().removeConsumer(consumer);
         consumer.close(false);
      }
   }

   /**
    * Creates a Queue if it doesn't already exist, based on a topic and address.  Returning the queue name.
    */
   private Queue createQueueForSubscription(String address, int qos) throws Exception {
      // Check to see if a subscription queue already exists.
      SimpleString queue = getQueueNameForTopic(address);
      Queue q = session.getServer().locateQueue(queue);

      // The queue does not exist so we need to create it.
      if (q == null) {
         SimpleString sAddress = SimpleString.toSimpleString(address);

         // Check we can auto create queues.
         BindingQueryResult bindingQueryResult = session.getServerSession().executeBindingQuery(sAddress);
         if (!bindingQueryResult.isAutoCreateQueues()) {
            throw ActiveMQMessageBundle.BUNDLE.noSuchQueue(sAddress);
         }

         // Check that the address exists, if not we try to auto create it.
         AddressInfo addressInfo = session.getServerSession().getAddress(sAddress);
         if (addressInfo == null) {
            if (!bindingQueryResult.isAutoCreateAddresses()) {
               throw ActiveMQMessageBundle.BUNDLE.addressDoesNotExist(SimpleString.toSimpleString(address));
            }
            addressInfo = session.getServerSession().createAddress(SimpleString.toSimpleString(address),
                                                                   RoutingType.MULTICAST, true);
         }
         return findOrCreateQueue(bindingQueryResult, addressInfo, queue, qos);
      }
      return q;
   }

   private Queue findOrCreateQueue(BindingQueryResult bindingQueryResult, AddressInfo addressInfo, SimpleString queue, int qos) throws Exception {

      if (addressInfo.getRoutingTypes().contains(RoutingType.MULTICAST)) {
         return session.getServerSession().createQueue(addressInfo.getName(), queue, RoutingType.MULTICAST, managementFilter, false, MQTTUtil.DURABLE_MESSAGES && qos >= 0, false);
      }

      if (addressInfo.getRoutingTypes().contains(RoutingType.ANYCAST)) {
         if (!bindingQueryResult.getQueueNames().isEmpty()) {
            SimpleString name = null;
            for (SimpleString qName : bindingQueryResult.getQueueNames()) {
               if (name == null) {
                  name = qName;
               } else if (qName.equals(addressInfo.getName())) {
                  name = qName;
               }
            }
            return session.getServer().locateQueue(name);
         } else {
            try {
               return session.getServerSession().createQueue(addressInfo.getName(), addressInfo.getName(), RoutingType.ANYCAST, managementFilter, false, MQTTUtil.DURABLE_MESSAGES && qos >= 0, false);
            } catch (ActiveMQQueueExistsException e) {
               return session.getServer().locateQueue(addressInfo.getName());
            }
         }
      }

      Set<RoutingType> routingTypeSet = new HashSet();
      routingTypeSet.add(RoutingType.MULTICAST);
      routingTypeSet.add(RoutingType.ANYCAST);
      throw ActiveMQMessageBundle.BUNDLE.invalidRoutingTypeForAddress(addressInfo.getRoutingType(), addressInfo.getName().toString(), routingTypeSet);
   }

   /**
    * Creates a new consumer for the queue associated with a subscription
    */
   private void createConsumerForSubscriptionQueue(Queue queue, String topic, int qos) throws Exception {
      long cid = session.getServer().getStorageManager().generateID();
      ServerConsumer consumer = session.getServerSession().createConsumer(cid, queue.getName(), null, false, true, -1);
      consumer.setStarted(true);

      consumers.put(topic, consumer);
      consumerQoSLevels.put(cid, qos);
   }

   private void addSubscription(MqttTopicSubscription subscription) throws Exception {
      String topicName = CompositeAddress.extractAddressName(subscription.topicName());
      MqttTopicSubscription s = session.getSessionState().getSubscription(topicName);

      int qos = subscription.qualityOfService().value();

      String coreAddress = MQTTUtil.convertMQTTAddressFilterToCore(topicName, session.getWildcardConfiguration());

      session.getSessionState().addSubscription(subscription, session.getWildcardConfiguration());

      Queue q = createQueueForSubscription(coreAddress, qos);

      if (s == null) {
         createConsumerForSubscriptionQueue(q, topicName, qos);
      } else {
         consumerQoSLevels.put(consumers.get(topicName).getID(), qos);
      }
      session.getRetainMessageManager().addRetainedMessagesToQueue(q, topicName);
   }

   void removeSubscriptions(List<String> topics) throws Exception {
      for (String topic : topics) {
         removeSubscription(topic);
      }
   }

   // FIXME: Do we need this synchronzied?
   private synchronized void removeSubscription(String address) throws Exception {
      String internalAddress = MQTTUtil.convertMQTTAddressFilterToCore(address, session.getWildcardConfiguration());

      SimpleString internalQueueName = getQueueNameForTopic(internalAddress);
      session.getSessionState().removeSubscription(address);


      ServerConsumer consumer = consumers.get(address);
      consumers.remove(address);
      if (consumer != null) {
         consumer.close(false);
         consumerQoSLevels.remove(consumer.getID());
      }

      if (session.getServerSession().executeQueueQuery(internalQueueName).isExists()) {
         session.getServerSession().deleteQueue(internalQueueName);
      }
   }

   private SimpleString getQueueNameForTopic(String topic) {
      return new SimpleString(session.getSessionState().getClientId() + "." + topic);
   }

   /**
    * As per MQTT Spec.  Subscribes this client to a number of MQTT topics.
    *
    * @param subscriptions
    * @return An array of integers representing the list of accepted QoS for each topic.
    * @throws Exception
    */
   int[] addSubscriptions(List<MqttTopicSubscription> subscriptions) throws Exception {
      int[] qos = new int[subscriptions.size()];

      for (int i = 0; i < subscriptions.size(); i++) {
         addSubscription(subscriptions.get(i));
         qos[i] = subscriptions.get(i).qualityOfService().value();
      }
      return qos;
   }

   Map<Long, Integer> getConsumerQoSLevels() {
      return consumerQoSLevels;
   }

   void clean() throws Exception {
      for (MqttTopicSubscription mqttTopicSubscription : session.getSessionState().getSubscriptions()) {
         removeSubscription(mqttTopicSubscription.topicName());
      }
   }
}
