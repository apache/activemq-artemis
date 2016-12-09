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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.apache.activemq.artemis.api.core.FilterConstants;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;

public class MQTTSubscriptionManager {

   private MQTTSession session;

   private ConcurrentMap<Long, Integer> consumerQoSLevels;

   private ConcurrentMap<String, ServerConsumer> consumers;

   // We filter out Artemis management messages and notifications
   private SimpleString managementFilter;

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
         String coreAddress = MQTTUtil.convertMQTTAddressFilterToCore(subscription.topicName());
         Queue q = createQueueForSubscription(coreAddress, subscription.qualityOfService().value());
         createConsumerForSubscriptionQueue(q, subscription.topicName(), subscription.qualityOfService().value());
      }
   }

   synchronized void stop(boolean clean) throws Exception {
      for (ServerConsumer consumer : consumers.values()) {
         consumer.setStarted(false);
         consumer.disconnect();
         consumer.getQueue().removeConsumer(consumer);
         consumer.close(false);
      }

      if (clean) {
         for (ServerConsumer consumer : consumers.values()) {
            session.getServer().destroyQueue(consumer.getQueue().getName());
         }
      }
   }

   /**
    * Creates a Queue if it doesn't already exist, based on a topic and address.  Returning the queue name.
    */
   private Queue createQueueForSubscription(String address, int qos) throws Exception {

      SimpleString queue = getQueueNameForTopic(address);

      Queue q = session.getServer().locateQueue(queue);
      if (q == null) {
         q = session.getServerSession().createQueue(new SimpleString(address), queue, managementFilter, false, MQTTUtil.DURABLE_MESSAGES && qos >= 0, -1, false, true);
      } else {
         if (q.isDeleteOnNoConsumers()) {
            throw ActiveMQMessageBundle.BUNDLE.invalidQueueConfiguration(q.getAddress(), q.getName(), "deleteOnNoConsumers", false, true);
         }
      }
      return q;
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
      MqttTopicSubscription s = session.getSessionState().getSubscription(subscription.topicName());

      int qos = subscription.qualityOfService().value();
      String topic = subscription.topicName();

      String coreAddress = MQTTUtil.convertMQTTAddressFilterToCore(topic);
      AddressInfo addressInfo = session.getServer().getAddressInfo(new SimpleString(coreAddress));
      if (addressInfo != null && addressInfo.getRoutingType() != AddressInfo.RoutingType.MULTICAST) {
         throw ActiveMQMessageBundle.BUNDLE.unexpectedRoutingTypeForAddress(new SimpleString(coreAddress), AddressInfo.RoutingType.MULTICAST, addressInfo.getRoutingType());
      }

      session.getSessionState().addSubscription(subscription);

      Queue q = createQueueForSubscription(coreAddress, qos);

      if (s == null) {
         createConsumerForSubscriptionQueue(q, topic, qos);
      } else {
         consumerQoSLevels.put(consumers.get(topic).getID(), qos);
      }
      session.getRetainMessageManager().addRetainedMessagesToQueue(q, topic);
   }

   void removeSubscriptions(List<String> topics) throws Exception {
      for (String topic : topics) {
         removeSubscription(topic);
      }
   }

   // FIXME: Do we need this synchronzied?
   private synchronized void removeSubscription(String address) throws Exception {
      ServerConsumer consumer = consumers.get(address);
      String internalAddress = MQTTUtil.convertMQTTAddressFilterToCore(address);
      SimpleString internalQueueName = getQueueNameForTopic(internalAddress);

      Queue queue = session.getServer().locateQueue(internalQueueName);
      queue.deleteQueue(true);
      session.getSessionState().removeSubscription(address);
      consumers.remove(address);
      consumerQoSLevels.remove(consumer.getID());
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

}
