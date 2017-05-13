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
package org.apache.activemq.artemis.api.jms;

import javax.jms.Queue;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.uri.ConnectionFactoryParser;

/**
 * A utility class for creating ActiveMQ Artemis client-side JMS managed resources.
 */
public class ActiveMQJMSClient {

   /**
    * Creates an ActiveMQConnectionFactory;
    *
    * @return the ActiveMQConnectionFactory
    */
   public static ActiveMQConnectionFactory createConnectionFactory(final String url, String name) throws Exception {
      ConnectionFactoryParser parser = new ConnectionFactoryParser();
      return parser.newObject(parser.expandURI(url), name);
   }

   /**
    * Creates an ActiveMQConnectionFactory that receives cluster topology updates from the cluster as
    * servers leave or join and new backups are appointed or removed.
    * <p>
    * The discoveryAddress and discoveryPort parameters in this method are used to listen for UDP
    * broadcasts which contain connection information for members of the cluster. The broadcasted
    * connection information is simply used to make an initial connection to the cluster, once that
    * connection is made, up to date cluster topology information is downloaded and automatically
    * updated whenever the cluster topology changes. If the topology includes backup servers that
    * information is also propagated to the client so that it can know which server to failover onto
    * in case of live server failure.
    *
    * @param groupConfiguration
    * @param jmsFactoryType
    * @return the ActiveMQConnectionFactory
    */
   public static ActiveMQConnectionFactory createConnectionFactoryWithHA(final DiscoveryGroupConfiguration groupConfiguration,
                                                                         JMSFactoryType jmsFactoryType) {
      return jmsFactoryType.createConnectionFactoryWithHA(groupConfiguration);
   }

   /**
    * Create an ActiveMQConnectionFactory which creates session factories from a set of live servers, no HA backup information is propagated to the client
    *
    * The UDP address and port are used to listen for live servers in the cluster
    *
    * @param groupConfiguration
    * @param jmsFactoryType
    * @return the ActiveMQConnectionFactory
    */
   public static ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final DiscoveryGroupConfiguration groupConfiguration,
                                                                            JMSFactoryType jmsFactoryType) {
      return jmsFactoryType.createConnectionFactoryWithoutHA(groupConfiguration);
   }

   /**
    * Create an ActiveMQConnectionFactory which will receive cluster topology updates from the cluster
    * as servers leave or join and new backups are appointed or removed.
    * <p>
    * The initial list of servers supplied in this method is simply to make an initial connection to
    * the cluster, once that connection is made, up to date cluster topology information is
    * downloaded and automatically updated whenever the cluster topology changes. If the topology
    * includes backup servers that information is also propagated to the client so that it can know
    * which server to failover onto in case of live server failure.
    *
    * @param jmsFactoryType
    * @param initialServers The initial set of servers used to make a connection to the cluster.
    *                       Each one is tried in turn until a successful connection is made. Once a connection
    *                       is made, the cluster topology is downloaded and the rest of the list is ignored.
    * @return the ActiveMQConnectionFactory
    */
   public static ActiveMQConnectionFactory createConnectionFactoryWithHA(JMSFactoryType jmsFactoryType,
                                                                         final TransportConfiguration... initialServers) {
      return jmsFactoryType.createConnectionFactoryWithHA(initialServers);
   }

   /**
    * Create an ActiveMQConnectionFactory which creates session factories using a static list of
    * transportConfigurations.
    * <p>
    * The ActiveMQConnectionFactory is not updated automatically as the cluster topology changes, and
    * no HA backup information is propagated to the client
    *
    * @param jmsFactoryType
    * @param transportConfigurations
    * @return the ActiveMQConnectionFactory
    */
   public static ActiveMQConnectionFactory createConnectionFactoryWithoutHA(JMSFactoryType jmsFactoryType,
                                                                            final TransportConfiguration... transportConfigurations) {
      return jmsFactoryType.createConnectionFactoryWithoutHA(transportConfigurations);
   }

   /**
    * Creates a client-side representation of a JMS Topic.
    *
    * @param name the name of the topic
    * @return The Topic
    */
   public static Topic createTopic(final String name) {
      return ActiveMQDestination.createTopic(name);
   }

   /**
    * Creates a client-side representation of a JMS Queue.
    *
    * @param name the name of the queue
    * @return The Queue
    */
   public static Queue createQueue(final String name) {
      return ActiveMQDestination.createQueue(name);
   }

   private ActiveMQJMSClient() {
      // Utility class
   }
}
