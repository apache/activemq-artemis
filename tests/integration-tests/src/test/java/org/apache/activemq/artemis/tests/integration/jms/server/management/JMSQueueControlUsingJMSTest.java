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
package org.apache.activemq.artemis.tests.integration.jms.server.management;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.api.jms.management.JMSQueueControl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import java.util.Map;

/**
 * A JMSQueueControlUsingJMSTest
 */
public class JMSQueueControlUsingJMSTest extends JMSQueueControlTest {
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private QueueConnection connection;

   private QueueSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(InVMConnectorFactory.class.getName()));
      connection = cf.createQueueConnection();
      session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();
   }

   @Ignore
   @Override
   @Test
   public void testListDeliveringMessages() throws Exception {
      // I'm not implementing the required proxy for this test on this JMS test
   }

   @Override
   protected JMSQueueControl createManagementControl() throws Exception {
      ActiveMQQueue managementQueue = (ActiveMQQueue) ActiveMQJMSClient.createQueue("activemq.management");

      final JMSMessagingProxy proxy = new JMSMessagingProxy(session, managementQueue, ResourceNames.JMS_QUEUE + queue.getQueueName());

      return new JMSQueueControl() {

         @Override
         public void flushExecutor() {
            try {
               proxy.invokeOperation("flushExecutor");
            }
            catch (Exception e) {
               throw new RuntimeException(e.getMessage(), e);
            }
         }

         public boolean changeMessagePriority(final String messageID, final int newPriority) throws Exception {
            return (Boolean) proxy.invokeOperation("changeMessagePriority", messageID, newPriority);
         }

         public int changeMessagesPriority(final String filter, final int newPriority) throws Exception {
            return (Integer) proxy.invokeOperation("changeMessagesPriority", filter, newPriority);
         }

         public long countMessages(final String filter) throws Exception {
            return ((Number) proxy.invokeOperation("countMessages", filter)).intValue();
         }

         public boolean expireMessage(final String messageID) throws Exception {
            return (Boolean) proxy.invokeOperation("expireMessage", messageID);
         }

         public int expireMessages(final String filter) throws Exception {
            return (Integer) proxy.invokeOperation("expireMessages", filter);
         }

         public int getConsumerCount() {
            return (Integer) proxy.retrieveAttributeValue("consumerCount");
         }

         public String getDeadLetterAddress() {
            return (String) proxy.retrieveAttributeValue("deadLetterAddress");
         }

         public int getDeliveringCount() {
            return (Integer) proxy.retrieveAttributeValue("deliveringCount");
         }

         public String getExpiryAddress() {
            return (String) proxy.retrieveAttributeValue("expiryAddress");
         }

         public String getFirstMessageAsJSON() throws Exception {
            return (String) proxy.retrieveAttributeValue("firstMessageAsJSON");
         }

         public Long getFirstMessageTimestamp() throws Exception {
            return (Long) proxy.retrieveAttributeValue("firstMessageTimestamp");
         }

         public Long getFirstMessageAge() throws Exception {
            return (Long) proxy.retrieveAttributeValue("firstMessageAge");
         }

         public long getMessageCount() {
            return ((Number) proxy.retrieveAttributeValue("messageCount")).longValue();
         }

         public long getMessagesAdded() {
            return (Integer) proxy.retrieveAttributeValue("messagesAdded");
         }

         public String getName() {
            return (String) proxy.retrieveAttributeValue("name");
         }

         public long getScheduledCount() {
            return (Long) proxy.retrieveAttributeValue("scheduledCount");
         }

         public boolean isTemporary() {
            return (Boolean) proxy.retrieveAttributeValue("temporary");
         }

         public String listMessageCounter() throws Exception {
            return (String) proxy.invokeOperation("listMessageCounter");
         }

         public void resetMessageCounter() throws Exception {
            proxy.invokeOperation("resetMessageCounter");
         }

         public String listMessageCounterAsHTML() throws Exception {
            return (String) proxy.invokeOperation("listMessageCounterAsHTML");
         }

         public String listMessageCounterHistory() throws Exception {
            return (String) proxy.invokeOperation("listMessageCounterHistory");
         }

         @Override
         public Map<String, Object>[] listScheduledMessages() throws Exception {
            return null;
         }

         @Override
         public String listScheduledMessagesAsJSON() throws Exception {
            return null;
         }

         @Override
         public Map<String, Map<String, Object>[]> listDeliveringMessages() throws Exception {
            return null;
         }

         @Override
         public String listDeliveringMessagesAsJSON() throws Exception {
            return null;
         }

         public String listMessageCounterHistoryAsHTML() throws Exception {
            return (String) proxy.invokeOperation("listMessageCounterHistoryAsHTML");
         }

         public Map<String, Object>[] listMessages(final String filter) throws Exception {
            Object[] res = (Object[]) proxy.invokeOperation("listMessages", filter);
            Map<String, Object>[] results = new Map[res.length];
            for (int i = 0; i < res.length; i++) {
               results[i] = (Map<String, Object>) res[i];
            }
            return results;
         }

         public String listMessagesAsJSON(final String filter) throws Exception {
            return (String) proxy.invokeOperation("listMessagesAsJSON", filter);
         }

         public boolean moveMessage(String messageID,
                                    String otherQueueName,
                                    boolean rejectDuplicates) throws Exception {
            return (Boolean) proxy.invokeOperation("moveMessage", messageID, otherQueueName, rejectDuplicates);
         }

         public int moveMessages(String filter, String otherQueueName, boolean rejectDuplicates) throws Exception {
            return (Integer) proxy.invokeOperation("moveMessages", filter, otherQueueName, rejectDuplicates);
         }

         public int moveMessages(final String filter, final String otherQueueName) throws Exception {
            return (Integer) proxy.invokeOperation("moveMessages", filter, otherQueueName);
         }

         public boolean moveMessage(final String messageID, final String otherQueueName) throws Exception {
            return (Boolean) proxy.invokeOperation("moveMessage", messageID, otherQueueName);
         }

         public int removeMessages(final String filter) throws Exception {
            return (Integer) proxy.invokeOperation("removeMessages", filter);
         }

         public boolean removeMessage(final String messageID) throws Exception {
            return (Boolean) proxy.invokeOperation("removeMessage", messageID);
         }

         public boolean sendMessageToDeadLetterAddress(final String messageID) throws Exception {
            return (Boolean) proxy.invokeOperation("sendMessageToDeadLetterAddress", messageID);
         }

         public int sendMessagesToDeadLetterAddress(final String filterStr) throws Exception {
            return (Integer) proxy.invokeOperation("sendMessagesToDeadLetterAddress", filterStr);
         }

         public void setDeadLetterAddress(final String deadLetterAddress) throws Exception {
            proxy.invokeOperation("setDeadLetterAddress", deadLetterAddress);
         }

         public void setExpiryAddress(final String expiryAddress) throws Exception {
            proxy.invokeOperation("setExpiryAddress", expiryAddress);
         }

         public String getAddress() {
            return (String) proxy.retrieveAttributeValue("address");
         }

         public boolean isPaused() throws Exception {
            return (Boolean) proxy.invokeOperation("isPaused");
         }

         public void pause() throws Exception {
            proxy.invokeOperation("pause");
         }

         public void resume() throws Exception {
            proxy.invokeOperation("resume");
         }

         public String getSelector() {
            return (String) proxy.retrieveAttributeValue("selector");
         }

         public void addBinding(String jndi) throws Exception {
            // TODO: Add a test for this
            proxy.invokeOperation("addBindings", jndi);
         }

         public String[] getRegistryBindings() {
            // TODO: Add a test for this
            return null;
         }

         public String listConsumersAsJSON() throws Exception {
            return (String) proxy.invokeOperation("listConsumersAsJSON");
         }
      };
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
