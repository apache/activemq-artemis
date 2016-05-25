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
package org.apache.activemq.artemis.jms.management.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.management.MBeanInfo;
import javax.management.StandardMBean;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.management.TopicControl;
import org.apache.activemq.artemis.core.management.impl.MBeanInfoHelper;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQMessage;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.utils.SelectorTranslator;
import org.apache.activemq.artemis.utils.json.JSONArray;
import org.apache.activemq.artemis.utils.json.JSONObject;

public class JMSTopicControlImpl extends StandardMBean implements TopicControl {

   private final ActiveMQDestination managedTopic;

   private final AddressControl addressControl;

   private final ManagementService managementService;

   private final JMSServerManager jmsServerManager;

   // Static --------------------------------------------------------

   public static String createFilterFromJMSSelector(final String selectorStr) throws ActiveMQException {
      return selectorStr == null || selectorStr.trim().length() == 0 ? null : SelectorTranslator.convertToActiveMQFilterString(selectorStr);
   }

   // Constructors --------------------------------------------------

   public JMSTopicControlImpl(final ActiveMQDestination topic,
                              final JMSServerManager jmsServerManager,
                              final AddressControl addressControl,
                              final ManagementService managementService) throws Exception {
      super(TopicControl.class);
      this.jmsServerManager = jmsServerManager;
      managedTopic = topic;
      this.addressControl = addressControl;
      this.managementService = managementService;
   }

   // TopicControlMBean implementation ------------------------------

   @Override
   public void addBinding(String binding) throws Exception {
      jmsServerManager.addTopicToBindingRegistry(managedTopic.getName(), binding);
   }

   @Override
   public String[] getRegistryBindings() {
      return jmsServerManager.getBindingsOnTopic(managedTopic.getName());
   }

   @Override
   public String getName() {
      return managedTopic.getName();
   }

   @Override
   public boolean isTemporary() {
      return managedTopic.isTemporary();
   }

   @Override
   public String getAddress() {
      return managedTopic.getAddress();
   }

   @Override
   public long getMessageCount() {
      return getMessageCount(DurabilityType.ALL);
   }

   @Override
   public int getDeliveringCount() {
      List<QueueControl> queues = getQueues(DurabilityType.ALL);
      int count = 0;
      for (QueueControl queue : queues) {
         count += queue.getDeliveringCount();
      }
      return count;
   }

   @Override
   public long getMessagesAdded() {
      List<QueueControl> queues = getQueues(DurabilityType.ALL);
      int count = 0;
      for (QueueControl queue : queues) {
         count += queue.getMessagesAdded();
      }
      return count;
   }

   @Override
   public int getDurableMessageCount() {
      return getMessageCount(DurabilityType.DURABLE);
   }

   @Override
   public int getNonDurableMessageCount() {
      return getMessageCount(DurabilityType.NON_DURABLE);
   }

   @Override
   public int getSubscriptionCount() {
      return getQueues(DurabilityType.ALL).size();
   }

   @Override
   public int getDurableSubscriptionCount() {
      return getQueues(DurabilityType.DURABLE).size();
   }

   @Override
   public int getNonDurableSubscriptionCount() {
      return getQueues(DurabilityType.NON_DURABLE).size();
   }

   @Override
   public Object[] listAllSubscriptions() {
      return listSubscribersInfos(DurabilityType.ALL);
   }

   @Override
   public String listAllSubscriptionsAsJSON() throws Exception {
      return listSubscribersInfosAsJSON(DurabilityType.ALL);
   }

   @Override
   public Object[] listDurableSubscriptions() {
      return listSubscribersInfos(DurabilityType.DURABLE);
   }

   @Override
   public String listDurableSubscriptionsAsJSON() throws Exception {
      return listSubscribersInfosAsJSON(DurabilityType.DURABLE);
   }

   @Override
   public Object[] listNonDurableSubscriptions() {
      return listSubscribersInfos(DurabilityType.NON_DURABLE);
   }

   @Override
   public String listNonDurableSubscriptionsAsJSON() throws Exception {
      return listSubscribersInfosAsJSON(DurabilityType.NON_DURABLE);
   }

   @Override
   public Map<String, Object>[] listMessagesForSubscription(final String queueName) throws Exception {
      QueueControl coreQueueControl = (QueueControl) managementService.getResource(ResourceNames.CORE_QUEUE + queueName);
      if (coreQueueControl == null) {
         throw new IllegalArgumentException("No subscriptions with name " + queueName);
      }

      Map<String, Object>[] coreMessages = coreQueueControl.listMessages(null);

      Map<String, Object>[] jmsMessages = new Map[coreMessages.length];

      int i = 0;

      for (Map<String, Object> coreMessage : coreMessages) {
         jmsMessages[i++] = ActiveMQMessage.coreMaptoJMSMap(coreMessage);
      }
      return jmsMessages;
   }

   @Override
   public String listMessagesForSubscriptionAsJSON(final String queueName) throws Exception {
      return JMSQueueControlImpl.toJSON(listMessagesForSubscription(queueName));
   }

   @Override
   public int countMessagesForSubscription(final String clientID,
                                           final String subscriptionName,
                                           final String filterStr) throws Exception {
      String queueName = ActiveMQDestination.createQueueNameForDurableSubscription(true, clientID, subscriptionName);
      QueueControl coreQueueControl = (QueueControl) managementService.getResource(ResourceNames.CORE_QUEUE + queueName);
      if (coreQueueControl == null) {
         throw new IllegalArgumentException("No subscriptions with name " + queueName + " for clientID " + clientID);
      }
      String filter = JMSTopicControlImpl.createFilterFromJMSSelector(filterStr);
      return coreQueueControl.listMessages(filter).length;
   }

   @Override
   public int removeMessages(final String filterStr) throws Exception {
      String filter = JMSTopicControlImpl.createFilterFromJMSSelector(filterStr);
      int count = 0;
      String[] queues = addressControl.getQueueNames();
      for (String queue : queues) {
         QueueControl coreQueueControl = (QueueControl) managementService.getResource(ResourceNames.CORE_QUEUE + queue);
         if (coreQueueControl != null) {
            count += coreQueueControl.removeMessages(filter);
         }
      }

      return count;
   }

   @Override
   public void dropDurableSubscription(final String clientID, final String subscriptionName) throws Exception {
      String queueName = ActiveMQDestination.createQueueNameForDurableSubscription(true, clientID, subscriptionName);
      QueueControl coreQueueControl = (QueueControl) managementService.getResource(ResourceNames.CORE_QUEUE + queueName);
      if (coreQueueControl == null) {
         throw new IllegalArgumentException("No subscriptions with name " + queueName + " for clientID " + clientID);
      }
      ActiveMQServerControl serverControl = (ActiveMQServerControl) managementService.getResource(ResourceNames.CORE_SERVER);
      serverControl.destroyQueue(queueName);
   }

   @Override
   public void dropAllSubscriptions() throws Exception {
      ActiveMQServerControl serverControl = (ActiveMQServerControl) managementService.getResource(ResourceNames.CORE_SERVER);
      String[] queues = addressControl.getQueueNames();
      for (String queue : queues) {
         // Drop all subscription shouldn't delete the dummy queue used to identify if the topic exists on the core queues.
         // we will just ignore this queue
         if (!queue.equals(managedTopic.getAddress())) {
            serverControl.destroyQueue(queue);
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private Object[] listSubscribersInfos(final DurabilityType durability) {
      List<QueueControl> queues = getQueues(durability);
      List<Object[]> subInfos = new ArrayList<>(queues.size());

      for (QueueControl queue : queues) {
         String clientID = null;
         String subName = null;

         if (queue.isDurable()) {
            Pair<String, String> pair = ActiveMQDestination.decomposeQueueNameForDurableSubscription(queue.getName());
            clientID = pair.getA();
            subName = pair.getB();
         }

         String filter = queue.getFilter() != null ? queue.getFilter() : null;

         Object[] subscriptionInfo = new Object[6];
         subscriptionInfo[0] = queue.getName();
         subscriptionInfo[1] = clientID;
         subscriptionInfo[2] = subName;
         subscriptionInfo[3] = queue.isDurable();
         subscriptionInfo[4] = queue.getMessageCount();
         subscriptionInfo[5] = filter;
         subInfos.add(subscriptionInfo);
      }
      return subInfos.toArray(new Object[subInfos.size()]);
   }

   private String listSubscribersInfosAsJSON(final DurabilityType durability) throws Exception {
      try {
         List<QueueControl> queues = getQueues(durability);
         JSONArray array = new JSONArray();

         for (QueueControl queue : queues) {
            String clientID = null;
            String subName = null;

            if (queue.isDurable() && !queue.getName().startsWith(ResourceNames.JMS_TOPIC)) {
               Pair<String, String> pair = ActiveMQDestination.decomposeQueueNameForDurableSubscription(queue.getName());
               clientID = pair.getA();
               subName = pair.getB();
            }
            else if (queue.getName().startsWith(ResourceNames.JMS_TOPIC)) {
               // in the case of heirarchical topics the queue name will not follow the <part>.<part> pattern of normal
               // durable subscribers so skip decomposing the name for the client ID and subscription name and just
               // hard-code it
               clientID = "ActiveMQ";
               subName = "ActiveMQ";
            }

            String filter = queue.getFilter() != null ? queue.getFilter() : null;

            JSONObject info = new JSONObject();

            info.put("queueName", queue.getName());
            info.put("clientID", clientID);
            info.put("selector", filter);
            info.put("name", subName);
            info.put("durable", queue.isDurable());
            info.put("messageCount", queue.getMessageCount());
            info.put("deliveringCount", queue.getDeliveringCount());
            info.put("consumers", new JSONArray(queue.listConsumersAsJSON()));
            array.put(info);
         }

         return array.toString();
      }
      catch (Exception e) {
         e.printStackTrace();
         return e.toString();
      }
   }

   private int getMessageCount(final DurabilityType durability) {
      List<QueueControl> queues = getQueues(durability);
      int count = 0;
      for (QueueControl queue : queues) {
         count += queue.getMessageCount();
      }
      return count;
   }

   private List<QueueControl> getQueues(final DurabilityType durability) {
      try {
         List<QueueControl> matchingQueues = new ArrayList<>();
         String[] queues = addressControl.getQueueNames();
         for (String queue : queues) {
            QueueControl coreQueueControl = (QueueControl) managementService.getResource(ResourceNames.CORE_QUEUE + queue);

            // Ignore the "special" subscription
            if (coreQueueControl != null && !coreQueueControl.getName().equals(addressControl.getAddress())) {
               if (durability == DurabilityType.ALL || durability == DurabilityType.DURABLE && coreQueueControl.isDurable() ||
                  durability == DurabilityType.NON_DURABLE && !coreQueueControl.isDurable()) {
                  matchingQueues.add(coreQueueControl);
               }
            }
         }
         return matchingQueues;
      }
      catch (Exception e) {
         return Collections.emptyList();
      }
   }

   @Override
   public MBeanInfo getMBeanInfo() {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(), info.getDescription(), info.getAttributes(), info.getConstructors(), MBeanInfoHelper.getMBeanOperationsInfo(TopicControl.class), info.getNotifications());
   }

   // Inner classes -------------------------------------------------

   private enum DurabilityType {
      ALL, DURABLE, NON_DURABLE
   }
}
