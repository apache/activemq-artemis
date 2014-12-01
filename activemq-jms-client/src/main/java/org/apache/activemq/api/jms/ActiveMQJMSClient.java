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
package org.apache.activemq.api.jms;

import javax.jms.Queue;
import javax.jms.Topic;

import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.jms.client.ActiveMQDestination;
import org.apache.activemq.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.activemq.jms.client.ActiveMQQueueConnectionFactory;
import org.apache.activemq.jms.client.ActiveMQTopicConnectionFactory;
import org.apache.activemq.jms.client.ActiveMQXAConnectionFactory;
import org.apache.activemq.jms.client.ActiveMQXAQueueConnectionFactory;
import org.apache.activemq.jms.client.ActiveMQXATopicConnectionFactory;

/**
 * A utility class for creating ActiveMQ client-side JMS managed resources.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ActiveMQJMSClient
{

   /**
    * Creates a ActiveMQConnectionFactory that receives cluster topology updates from the cluster as
    * servers leave or join and new backups are appointed or removed.
    * <p>
    * The discoveryAddress and discoveryPort parameters in this method are used to listen for UDP
    * broadcasts which contain connection information for members of the cluster. The broadcasted
    * connection information is simply used to make an initial connection to the cluster, once that
    * connection is made, up to date cluster topology information is downloaded and automatically
    * updated whenever the cluster topology changes. If the topology includes backup servers that
    * information is also propagated to the client so that it can know which server to failover onto
    * in case of live server failure.
    * @param discoveryAddress The UDP group address to listen for updates
    * @param discoveryPort the UDP port to listen for updates
    * @return the ActiveMQConnectionFactory
    */
   public static ActiveMQConnectionFactory createConnectionFactoryWithHA(final DiscoveryGroupConfiguration groupConfiguration, JMSFactoryType jmsFactoryType)
   {
      ActiveMQConnectionFactory factory = null;
      if (jmsFactoryType.equals(JMSFactoryType.CF))
      {
         factory = new ActiveMQJMSConnectionFactory(true, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_CF))
      {
         factory = new ActiveMQQueueConnectionFactory(true, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_CF))
      {
         factory = new ActiveMQTopicConnectionFactory(true, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.XA_CF))
      {
         factory = new ActiveMQXAConnectionFactory(true, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_XA_CF))
      {
         factory = new ActiveMQXAQueueConnectionFactory(true, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_XA_CF))
      {
         factory = new ActiveMQXATopicConnectionFactory(true, groupConfiguration);
      }

      return factory;
   }

   /**
    * Create a ActiveMQConnectionFactory which creates session factories from a set of live servers, no HA backup information is propagated to the client
    *
    * The UDP address and port are used to listen for live servers in the cluster
    *
    * @param discoveryAddress The UDP group address to listen for updates
    * @param discoveryPort the UDP port to listen for updates
    * @return the ActiveMQConnectionFactory
    */
   public static ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final DiscoveryGroupConfiguration groupConfiguration, JMSFactoryType jmsFactoryType)
   {
      ActiveMQConnectionFactory factory = null;
      if (jmsFactoryType.equals(JMSFactoryType.CF))
      {
         factory = new ActiveMQJMSConnectionFactory(false, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_CF))
      {
         factory = new ActiveMQQueueConnectionFactory(false, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_CF))
      {
         factory = new ActiveMQTopicConnectionFactory(false, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.XA_CF))
      {
         factory = new ActiveMQXAConnectionFactory(false, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_XA_CF))
      {
         factory = new ActiveMQXAQueueConnectionFactory(false, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_XA_CF))
      {
         factory = new ActiveMQXATopicConnectionFactory(false, groupConfiguration);
      }

      return factory;
   }

   /**
    * Create a ActiveMQConnectionFactory which will receive cluster topology updates from the cluster
    * as servers leave or join and new backups are appointed or removed.
    * <p>
    * The initial list of servers supplied in this method is simply to make an initial connection to
    * the cluster, once that connection is made, up to date cluster topology information is
    * downloaded and automatically updated whenever the cluster topology changes. If the topology
    * includes backup servers that information is also propagated to the client so that it can know
    * which server to failover onto in case of live server failure.
    * @param initialServers The initial set of servers used to make a connection to the cluster.
    *           Each one is tried in turn until a successful connection is made. Once a connection
    *           is made, the cluster topology is downloaded and the rest of the list is ignored.
    * @return the ActiveMQConnectionFactory
    */
   public static ActiveMQConnectionFactory createConnectionFactoryWithHA(JMSFactoryType jmsFactoryType, final TransportConfiguration... initialServers)
   {
      ActiveMQConnectionFactory factory = null;
      if (jmsFactoryType.equals(JMSFactoryType.CF))
      {
         factory = new ActiveMQJMSConnectionFactory(true, initialServers);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_CF))
      {
         factory = new ActiveMQQueueConnectionFactory(true, initialServers);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_CF))
      {
         factory = new ActiveMQTopicConnectionFactory(true, initialServers);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.XA_CF))
      {
         factory = new ActiveMQXAConnectionFactory(true, initialServers);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_XA_CF))
      {
         factory = new ActiveMQXAQueueConnectionFactory(true, initialServers);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_XA_CF))
      {
         factory = new ActiveMQXATopicConnectionFactory(true, initialServers);
      }

      return factory;
   }

   /**
    * Create a ActiveMQConnectionFactory which creates session factories using a static list of
    * transportConfigurations.
    * <p>
    * The ActiveMQConnectionFactory is not updated automatically as the cluster topology changes, and
    * no HA backup information is propagated to the client
    * @param transportConfigurations
    * @return the ActiveMQConnectionFactory
    */
   public static ActiveMQConnectionFactory createConnectionFactoryWithoutHA(JMSFactoryType jmsFactoryType, final TransportConfiguration... transportConfigurations)
   {
      ActiveMQConnectionFactory factory = null;
      if (jmsFactoryType.equals(JMSFactoryType.CF))
      {
         factory = new ActiveMQJMSConnectionFactory(false, transportConfigurations);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_CF))
      {
         factory = new ActiveMQQueueConnectionFactory(false, transportConfigurations);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_CF))
      {
         factory = new ActiveMQTopicConnectionFactory(false, transportConfigurations);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.XA_CF))
      {
         factory = new ActiveMQXAConnectionFactory(false, transportConfigurations);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_XA_CF))
      {
         factory = new ActiveMQXAQueueConnectionFactory(false, transportConfigurations);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_XA_CF))
      {
         factory = new ActiveMQXATopicConnectionFactory(false, transportConfigurations);
      }

      return factory;
   }

   /**
    * Creates a client-side representation of a JMS Topic.
    *
    * @param name the name of the topic
    * @return The Topic
    */
   public static Topic createTopic(final String name)
   {
      return ActiveMQDestination.createTopic(name);
   }

   /**
    * Creates a client-side representation of a JMS Queue.
    *
    * @param name the name of the queue
    * @return The Queue
    */
   public static Queue createQueue(final String name)
   {
      return ActiveMQDestination.createQueue(name);
   }

   private ActiveMQJMSClient()
   {
      // Utility class
   }
}
