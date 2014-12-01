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
package org.apache.activemq.jms.server;

import java.util.List;
import java.util.Set;

import javax.naming.Context;

import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.security.Role;
import org.apache.activemq.core.server.ActiveMQComponent;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.settings.impl.AddressSettings;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.spi.core.naming.BindingRegistry;

/**
 * The JMS Management interface.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface JMSServerManager extends ActiveMQComponent
{
   String getVersion();

   /**
    * Has the Server been started.
    *
    * @return true if the server us running
    */
   boolean isStarted();

   /**
    * Creates a JMS Queue.
    *
    * @param queueName
    *           The name of the queue to create
    * @param selectorString
    * @param durable
    * @return true if the queue is created or if it existed and was added to
    *         JNDI
    * @throws Exception
    *            if problems were encountered creating the queue.
    */
   boolean createQueue(boolean storeConfig, String queueName, String selectorString, boolean durable, String ...bindings) throws Exception;

   boolean addTopicToJndi(final String topicName, final String binding) throws Exception;

   boolean addQueueToJndi(final String queueName, final String binding) throws Exception;

   boolean addConnectionFactoryToJNDI(final String name, final String binding) throws Exception;

   /**
    * Creates a JMS Topic
    *
    * @param topicName
    *           the name of the topic
    * @param bindings
    *           the names of the binding for JNDI or BindingRegistry
    * @return true if the topic was created or if it existed and was added to
    *         JNDI
    * @throws Exception
    *            if a problem occurred creating the topic
    */
   boolean createTopic(boolean storeConfig, String topicName, String ... bindings) throws Exception;

   /**
    * Remove the topic from JNDI or BindingRegistry.
    * Calling this method does <em>not</em> destroy the destination.
    *
    * @param name
    *           the name of the destination to remove from JNDI or BindingRegistry
    * @return true if removed
    * @throws Exception
    *            if a problem occurred removing the destination
    */
   boolean removeTopicFromJNDI(String name, String binding) throws Exception;

   /**
    * Remove the topic from JNDI or BindingRegistry.
    * Calling this method does <em>not</em> destroy the destination.
    *
    * @param name
    *           the name of the destination to remove from JNDI or BindingRegistry
    * @return true if removed
    * @throws Exception
    *            if a problem occurred removing the destination
    */
   boolean removeTopicFromJNDI(String name) throws Exception;

   /**
    * Remove the queue from JNDI or BindingRegistry.
    * Calling this method does <em>not</em> destroy the destination.
    *
    * @param name
    *           the name of the destination to remove from JNDI or BindingRegistry
    * @return true if removed
    * @throws Exception
    *            if a problem occurred removing the destination
    */
   boolean removeQueueFromJNDI(String name, String binding) throws Exception;

   /**
    * Remove the queue from JNDI or BindingRegistry.
    * Calling this method does <em>not</em> destroy the destination.
    *
    * @param name
    *           the name of the destination to remove from JNDI or BindingRegistry
    * @return true if removed
    * @throws Exception
    *            if a problem occurred removing the destination
    */
   boolean removeQueueFromJNDI(String name) throws Exception;

   boolean removeConnectionFactoryFromJNDI(String name, String binding) throws Exception;

   boolean removeConnectionFactoryFromJNDI(String name) throws Exception;

   /**
    * destroys a queue and removes it from JNDI or BindingRegistry
    *
    * @param name
    *           the name of the queue to destroy
    * @return true if destroyed
    * @throws Exception
    *            if a problem occurred destroying the queue
    */
   boolean destroyQueue(String name) throws Exception;

   /**
    * destroys a queue and removes it from JNDI or BindingRegistry.
    * disconnects any consumers connected to the queue.
    *
    * @param name
    *           the name of the queue to destroy
    * @return true if destroyed
    * @throws Exception
    *            if a problem occurred destroying the queue
    */
   boolean destroyQueue(String name, boolean removeConsumers) throws Exception;

   String[] getJNDIOnQueue(String queue);

   String[] getJNDIOnTopic(String topic);

   String[] getJNDIOnConnectionFactory(String factoryName);

   /**
    * destroys a topic and removes it from JNDI  or BindingRegistry
    *
    * @param name
    *           the name of the topic to destroy
    * @return true if the topic was destroyed
    * @throws Exception
    *            if a problem occurred destroying the topic
    */
   boolean destroyTopic(String name, boolean removeConsumers) throws Exception;

   /**
    * destroys a topic and removes it from JNDI  or BindingRegistry
    *
    * @param name
    *           the name of the topic to destroy
    * @return true if the topic was destroyed
    * @throws Exception
    *            if a problem occurred destroying the topic
    */
   boolean destroyTopic(String name) throws Exception;

   /** Call this method to have a CF rebound to JNDI and stored on the Journal
    * @throws Exception */
   ActiveMQConnectionFactory recreateCF(String name,  ConnectionFactoryConfiguration cf) throws Exception;

   void createConnectionFactory(String name, boolean ha, JMSFactoryType cfType, String discoveryGroupName, String ... jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                boolean ha,
                                JMSFactoryType cfType,
                                List<String> connectorNames,
                                String ... bindings) throws Exception;

   void createConnectionFactory(String name,
                                boolean ha,
                                JMSFactoryType cfType,
                                List<String> connectorNames,
                                String clientID,
                                long clientFailureCheckPeriod,
                                long connectionTTL,
                                long callTimeout,
                                long callFailoverTimeout,
                                boolean cacheLargeMessagesClient,
                                int minLargeMessageSize,
                                boolean compressLargeMessage,
                                int consumerWindowSize,
                                int consumerMaxRate,
                                int confirmationWindowSize,
                                int producerWindowSize,
                                int producerMaxRate,
                                boolean blockOnAcknowledge,
                                boolean blockOnDurableSend,
                                boolean blockOnNonDurableSend,
                                boolean autoGroup,
                                boolean preAcknowledge,
                                String loadBalancingPolicyClassName,
                                int transactionBatchSize,
                                int dupsOKBatchSize,
                                boolean useGlobalPools,
                                int scheduledThreadPoolMaxSize,
                                int threadPoolMaxSize,
                                long retryInterval,
                                double retryIntervalMultiplier,
                                long maxRetryInterval,
                                int reconnectAttempts,
                                boolean failoverOnInitialConnection,
                                String groupId,
                                String ... bindings) throws Exception;

   void createConnectionFactory(String name,
                                boolean ha,
                                JMSFactoryType cfType,
                                String discoveryGroupName,
                                String clientID,
                                long clientFailureCheckPeriod,
                                long connectionTTL,
                                long callTimeout,
                                long callFailoverTimeout,
                                boolean cacheLargeMessagesClient,
                                int minLargeMessageSize,
                                boolean compressLargeMessages,
                                int consumerWindowSize,
                                int consumerMaxRate,
                                int confirmationWindowSize,
                                int producerWindowSize,
                                int producerMaxRate,
                                boolean blockOnAcknowledge,
                                boolean blockOnDurableSend,
                                boolean blockOnNonDurableSend,
                                boolean autoGroup,
                                boolean preAcknowledge,
                                String loadBalancingPolicyClassName,
                                int transactionBatchSize,
                                int dupsOKBatchSize,
                                boolean useGlobalPools,
                                int scheduledThreadPoolMaxSize,
                                int threadPoolMaxSize,
                                long retryInterval,
                                double retryIntervalMultiplier,
                                long maxRetryInterval,
                                int reconnectAttempts,
                                boolean failoverOnInitialConnection,
                                String groupId,
                                String ... bindings) throws Exception;

   void createConnectionFactory(boolean storeConfig, ConnectionFactoryConfiguration cfConfig, String... bindings) throws Exception;

   /**
    * destroys a connection factory.
    *
    * @param name
    *           the name of the connection factory to destroy
    * @return true if the connection factory was destroyed
    * @throws Exception
    *            if a problem occurred destroying the connection factory
    */
   boolean destroyConnectionFactory(String name) throws Exception;

   String[] listRemoteAddresses() throws Exception;

   String[] listRemoteAddresses(String ipAddress) throws Exception;

   boolean closeConnectionsForAddress(String ipAddress) throws Exception;

   boolean closeConsumerConnectionsForAddress(String address) throws Exception;

   boolean closeConnectionsForUser(String address) throws Exception;

   String[] listConnectionIDs() throws Exception;

   String[] listSessions(String connectionID) throws Exception;

   String listPreparedTransactionDetailsAsJSON() throws Exception;

   String listPreparedTransactionDetailsAsHTML() throws Exception;

   void setContext(final Context context);

   ActiveMQServer getActiveMQServer();

   void addAddressSettings(String address, AddressSettings addressSettings);

   AddressSettings getAddressSettings(String address);

   void addSecurity(String addressMatch, Set<Role> roles);

   Set<Role> getSecurity(final String addressMatch);

   BindingRegistry getRegistry();

   /**
    * Set this property if you want something other than JNDI for your registry
    *
    * @param registry
    */
   void setRegistry(BindingRegistry registry);
}
