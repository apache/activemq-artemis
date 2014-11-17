/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.api.jms;

import javax.jms.Queue;
import javax.jms.Topic;

import org.apache.activemq6.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.jms.client.HornetQConnectionFactory;
import org.apache.activemq6.jms.client.HornetQDestination;
import org.apache.activemq6.jms.client.HornetQJMSConnectionFactory;
import org.apache.activemq6.jms.client.HornetQQueueConnectionFactory;
import org.apache.activemq6.jms.client.HornetQTopicConnectionFactory;
import org.apache.activemq6.jms.client.HornetQXAConnectionFactory;
import org.apache.activemq6.jms.client.HornetQXAQueueConnectionFactory;
import org.apache.activemq6.jms.client.HornetQXATopicConnectionFactory;

/**
 * A utility class for creating HornetQ client-side JMS managed resources.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class HornetQJMSClient
{

   /**
    * Creates a HornetQConnectionFactory that receives cluster topology updates from the cluster as
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
    * @return the HornetQConnectionFactory
    */
   public static HornetQConnectionFactory createConnectionFactoryWithHA(final DiscoveryGroupConfiguration groupConfiguration, JMSFactoryType jmsFactoryType)
   {
      HornetQConnectionFactory factory = null;
      if (jmsFactoryType.equals(JMSFactoryType.CF))
      {
         factory = new HornetQJMSConnectionFactory(true, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_CF))
      {
         factory = new HornetQQueueConnectionFactory(true, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_CF))
      {
         factory = new HornetQTopicConnectionFactory(true, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.XA_CF))
      {
         factory = new HornetQXAConnectionFactory(true, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_XA_CF))
      {
         factory = new HornetQXAQueueConnectionFactory(true, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_XA_CF))
      {
         factory = new HornetQXATopicConnectionFactory(true, groupConfiguration);
      }

      return factory;
   }

   /**
    * Create a HornetQConnectionFactory which creates session factories from a set of live servers, no HA backup information is propagated to the client
    *
    * The UDP address and port are used to listen for live servers in the cluster
    *
    * @param discoveryAddress The UDP group address to listen for updates
    * @param discoveryPort the UDP port to listen for updates
    * @return the HornetQConnectionFactory
    */
   public static HornetQConnectionFactory createConnectionFactoryWithoutHA(final DiscoveryGroupConfiguration groupConfiguration, JMSFactoryType jmsFactoryType)
   {
      HornetQConnectionFactory factory = null;
      if (jmsFactoryType.equals(JMSFactoryType.CF))
      {
         factory = new HornetQJMSConnectionFactory(false, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_CF))
      {
         factory = new HornetQQueueConnectionFactory(false, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_CF))
      {
         factory = new HornetQTopicConnectionFactory(false, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.XA_CF))
      {
         factory = new HornetQXAConnectionFactory(false, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_XA_CF))
      {
         factory = new HornetQXAQueueConnectionFactory(false, groupConfiguration);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_XA_CF))
      {
         factory = new HornetQXATopicConnectionFactory(false, groupConfiguration);
      }

      return factory;
   }

   /**
    * Create a HornetQConnectionFactory which will receive cluster topology updates from the cluster
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
    * @return the HornetQConnectionFactory
    */
   public static HornetQConnectionFactory createConnectionFactoryWithHA(JMSFactoryType jmsFactoryType, final TransportConfiguration... initialServers)
   {
      HornetQConnectionFactory factory = null;
      if (jmsFactoryType.equals(JMSFactoryType.CF))
      {
         factory = new HornetQJMSConnectionFactory(true, initialServers);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_CF))
      {
         factory = new HornetQQueueConnectionFactory(true, initialServers);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_CF))
      {
         factory = new HornetQTopicConnectionFactory(true, initialServers);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.XA_CF))
      {
         factory = new HornetQXAConnectionFactory(true, initialServers);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_XA_CF))
      {
         factory = new HornetQXAQueueConnectionFactory(true, initialServers);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_XA_CF))
      {
         factory = new HornetQXATopicConnectionFactory(true, initialServers);
      }

      return factory;
   }

   /**
    * Create a HornetQConnectionFactory which creates session factories using a static list of
    * transportConfigurations.
    * <p>
    * The HornetQConnectionFactory is not updated automatically as the cluster topology changes, and
    * no HA backup information is propagated to the client
    * @param transportConfigurations
    * @return the HornetQConnectionFactory
    */
   public static HornetQConnectionFactory createConnectionFactoryWithoutHA(JMSFactoryType jmsFactoryType, final TransportConfiguration... transportConfigurations)
   {
      HornetQConnectionFactory factory = null;
      if (jmsFactoryType.equals(JMSFactoryType.CF))
      {
         factory = new HornetQJMSConnectionFactory(false, transportConfigurations);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_CF))
      {
         factory = new HornetQQueueConnectionFactory(false, transportConfigurations);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_CF))
      {
         factory = new HornetQTopicConnectionFactory(false, transportConfigurations);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.XA_CF))
      {
         factory = new HornetQXAConnectionFactory(false, transportConfigurations);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_XA_CF))
      {
         factory = new HornetQXAQueueConnectionFactory(false, transportConfigurations);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_XA_CF))
      {
         factory = new HornetQXATopicConnectionFactory(false, transportConfigurations);
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
      return HornetQDestination.createTopic(name);
   }

   /**
    * Creates a client-side representation of a JMS Queue.
    *
    * @param name the name of the queue
    * @return The Queue
    */
   public static Queue createQueue(final String name)
   {
      return HornetQDestination.createQueue(name);
   }

   private HornetQJMSClient()
   {
      // Utility class
   }
}
