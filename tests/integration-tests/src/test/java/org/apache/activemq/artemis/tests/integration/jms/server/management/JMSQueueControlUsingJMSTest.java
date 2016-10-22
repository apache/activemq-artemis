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

import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.management.openmbean.CompositeData;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.Parameter;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.api.jms.management.JMSQueueControl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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

      final JMSMessagingProxy proxy = new JMSMessagingProxy(session, managementQueue, queue.getQueueName());

      return new JMSQueueControl() {

         @Override
         public void flushExecutor() {
            try {
               proxy.invokeOperation("flushExecutor");
            } catch (Exception e) {
               throw new RuntimeException(e.getMessage(), e);
            }
         }

         @Override
         public boolean changeMessagePriority(final String messageID, final int newPriority) throws Exception {
            return (Boolean) proxy.invokeOperation("changeMessagePriority", messageID, newPriority);
         }

         @Override
         public int changeMessagesPriority(final String filter, final int newPriority) throws Exception {
            return (Integer) proxy.invokeOperation(Integer.class, "changeMessagesPriority", filter, newPriority);
         }

         @Override
         public long countMessages(final String filter) throws Exception {
            return ((Number) proxy.invokeOperation("countMessages", filter)).intValue();
         }

         @Override
         public boolean expireMessage(final String messageID) throws Exception {
            return (Boolean) proxy.invokeOperation("expireMessage", messageID);
         }

         @Override
         public int expireMessages(final String filter) throws Exception {
            return (Integer) proxy.invokeOperation(Integer.class, "expireMessages", filter);
         }

         @Override
         public int getConsumerCount() {
            return (Integer) proxy.retrieveAttributeValue("consumerCount", Integer.class);
         }

         @Override
         public long getMessagesExpired() {
            return ((Number) proxy.retrieveAttributeValue("getMessagesExpired")).longValue();
         }

         @Override
         public long getMessagesKilled() {
            return ((Number) proxy.retrieveAttributeValue("messagesKilled")).longValue();
         }

         @Override
         public String getDeadLetterAddress() {
            return (String) proxy.retrieveAttributeValue("deadLetterAddress");
         }

         @Override
         public int getDeliveringCount() {
            return (Integer) proxy.retrieveAttributeValue("deliveringCount", Integer.class);
         }

         @Override
         public String getExpiryAddress() {
            return (String) proxy.retrieveAttributeValue("expiryAddress");
         }

         @Override
         public String getFirstMessageAsJSON() throws Exception {
            return (String) proxy.retrieveAttributeValue("firstMessageAsJSON");
         }

         @Override
         public Long getFirstMessageTimestamp() throws Exception {
            return (Long) proxy.retrieveAttributeValue("firstMessageTimestamp");
         }

         @Override
         public Long getFirstMessageAge() throws Exception {
            return (Long) proxy.retrieveAttributeValue("firstMessageAge");
         }

         @Override
         public long getMessageCount() {
            return ((Number) proxy.retrieveAttributeValue("messageCount")).longValue();
         }

         @Override
         public long getMessagesAdded() {
            return (Integer) proxy.retrieveAttributeValue("messagesAdded", Integer.class);
         }

         @Override
         public String getName() {
            return (String) proxy.retrieveAttributeValue("name");
         }

         @Override
         public long getScheduledCount() {
            return (Long) proxy.retrieveAttributeValue("scheduledCount");
         }

         @Override
         public boolean isTemporary() {
            return (Boolean) proxy.retrieveAttributeValue("temporary");
         }

         @Override
         public String listMessageCounter() throws Exception {
            return (String) proxy.invokeOperation("listMessageCounter");
         }

         @Override
         public void resetMessageCounter() throws Exception {
            proxy.invokeOperation("resetMessageCounter");
         }

         @Override
         public String listMessageCounterAsHTML() throws Exception {
            return (String) proxy.invokeOperation("listMessageCounterAsHTML");
         }

         @Override
         public String listMessageCounterHistory() throws Exception {
            return (String) proxy.invokeOperation("listMessageCounterHistory");
         }

         public boolean retryMessage(@Parameter(name = "messageID", desc = "A message ID") long messageID) throws Exception {
            return (Boolean) proxy.invokeOperation("retryMessage", messageID);
         }

         @Override
         public int retryMessages() throws Exception {
            return (Integer) proxy.invokeOperation("retryMessages", Integer.class);
         }

         @Override
         public boolean retryMessage(final String messageID) throws Exception {
            return (Boolean) proxy.invokeOperation("retryMessage", messageID);
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

         @Override
         public String listMessageCounterHistoryAsHTML() throws Exception {
            return (String) proxy.invokeOperation("listMessageCounterHistoryAsHTML");
         }

         @Override
         public Map<String, Object>[] listMessages(final String filter) throws Exception {
            Object[] res = (Object[]) proxy.invokeOperation("listMessages", filter);
            Map<String, Object>[] results = new Map[res.length];
            for (int i = 0; i < res.length; i++) {
               results[i] = (Map<String, Object>) res[i];
            }
            return results;
         }

         @Override
         public String listMessagesAsJSON(final String filter) throws Exception {
            return (String) proxy.invokeOperation("listMessagesAsJSON", filter);
         }

         @Override
         public boolean moveMessage(String messageID,
                                    String otherQueueName,
                                    boolean rejectDuplicates) throws Exception {
            return (Boolean) proxy.invokeOperation("moveMessage", messageID, otherQueueName, rejectDuplicates);
         }

         @Override
         public int moveMessages(String filter, String otherQueueName, boolean rejectDuplicates) throws Exception {
            return (Integer) proxy.invokeOperation(Integer.class, "moveMessages", filter, otherQueueName, rejectDuplicates);
         }

         @Override
         public int moveMessages(final String filter, final String otherQueueName) throws Exception {
            return (Integer) proxy.invokeOperation(Integer.class, "moveMessages", filter, otherQueueName);
         }

         @Override
         public boolean moveMessage(final String messageID, final String otherQueueName) throws Exception {
            return (Boolean) proxy.invokeOperation("moveMessage", messageID, otherQueueName);
         }

         @Override
         public int removeMessages(final String filter) throws Exception {
            return (Integer) proxy.invokeOperation(Integer.class, "removeMessages", filter);
         }

         @Override
         public boolean removeMessage(final String messageID) throws Exception {
            return (Boolean) proxy.invokeOperation("removeMessage", messageID);
         }

         @Override
         public boolean sendMessageToDeadLetterAddress(final String messageID) throws Exception {
            return (Boolean) proxy.invokeOperation("sendMessageToDeadLetterAddress", messageID);
         }

         @Override
         public int sendMessagesToDeadLetterAddress(final String filterStr) throws Exception {
            return (Integer) proxy.invokeOperation(Integer.class, "sendMessagesToDeadLetterAddress", filterStr);
         }

         @Override
         public String sendTextMessage(@Parameter(name = "body") String body) throws Exception {
            return null;
         }

         @Override
         public String sendTextMessageWithProperties(String properties) throws Exception {
            return null;
         }

         @Override
         public String sendTextMessage(Map<String, String> headers, String body) throws Exception {
            return null;
         }

         @Override
         public String sendTextMessage(String body, String user, String password) throws Exception {
            return null;
         }

         @Override
         public String sendTextMessage(Map<String, String> headers,
                                       String body,
                                       String user,
                                       String password) throws Exception {
            return (String) proxy.invokeOperation("sendTextMessage", headers, body, user, password);
         }

         public void setDeadLetterAddress(final String deadLetterAddress) throws Exception {
            proxy.invokeOperation("setDeadLetterAddress", deadLetterAddress);
         }

         public void setExpiryAddress(final String expiryAddress) throws Exception {
            proxy.invokeOperation("setExpiryAddress", expiryAddress);
         }

         @Override
         public String getAddress() {
            return (String) proxy.retrieveAttributeValue("address");
         }

         @Override
         public boolean isPaused() throws Exception {
            return (Boolean) proxy.invokeOperation("isPaused");
         }

         @Override
         public void pause() throws Exception {
            proxy.invokeOperation("pause");
         }

         @Override
         public void pause(boolean persist) throws Exception {
            proxy.invokeOperation("pause", persist);
         }

         @Override
         public void resume() throws Exception {
            proxy.invokeOperation("resume");
         }

         @Override
         public CompositeData[] browse() throws Exception {
            Map map = (Map) proxy.invokeOperation("browse");
            CompositeData[] compositeDatas = (CompositeData[]) map.get(CompositeData.class.getName());
            if (compositeDatas == null) {
               compositeDatas = new CompositeData[0];
            }
            return compositeDatas;
         }

         @Override
         public CompositeData[] browse(String filter) throws Exception {
            return new CompositeData[0];
         }

         @Override
         public String getSelector() {
            return (String) proxy.retrieveAttributeValue("selector");
         }

         @Override
         public void addBinding(String jndi) throws Exception {
            // TODO: Add a test for this
            proxy.invokeOperation("addBindings", jndi);
         }

         @Override
         public String[] getRegistryBindings() {
            // TODO: Add a test for this
            return null;
         }

         @Override
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
