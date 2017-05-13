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

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.jms.client.*;

// XXX no javadocs
public enum JMSFactoryType {
   CF {
      public ActiveMQConnectionFactory createConnectionFactoryWithHA(final DiscoveryGroupConfiguration groupConfiguration) {
         return new ActiveMQJMSConnectionFactory(true, groupConfiguration);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final DiscoveryGroupConfiguration groupConfiguration) {
         return new ActiveMQJMSConnectionFactory(false, groupConfiguration);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithHA(final TransportConfiguration... initialServers) {
         return new ActiveMQJMSConnectionFactory(true, initialServers);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final TransportConfiguration... transportConfigurations) {
         return new ActiveMQJMSConnectionFactory(false, transportConfigurations);
      }
   },
   QUEUE_CF {
      public ActiveMQConnectionFactory createConnectionFactoryWithHA(final DiscoveryGroupConfiguration groupConfiguration) {
         return new ActiveMQQueueConnectionFactory(true, groupConfiguration);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final DiscoveryGroupConfiguration groupConfiguration) {
         return new ActiveMQQueueConnectionFactory(false, groupConfiguration);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithHA(final TransportConfiguration... initialServers) {
         return new ActiveMQQueueConnectionFactory(true, initialServers);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final TransportConfiguration... transportConfigurations) {
         return new ActiveMQQueueConnectionFactory(false, transportConfigurations);
      }
   },
   TOPIC_CF {
      public ActiveMQConnectionFactory createConnectionFactoryWithHA(final DiscoveryGroupConfiguration groupConfiguration) {
         return new ActiveMQTopicConnectionFactory(true, groupConfiguration);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final DiscoveryGroupConfiguration groupConfiguration) {
         return new ActiveMQTopicConnectionFactory(false, groupConfiguration);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithHA(final TransportConfiguration... initialServers) {
         return new ActiveMQTopicConnectionFactory(true, initialServers);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final TransportConfiguration... transportConfigurations) {
         return new ActiveMQTopicConnectionFactory(false, transportConfigurations);
      }
   },
   XA_CF {
      public ActiveMQConnectionFactory createConnectionFactoryWithHA(final DiscoveryGroupConfiguration groupConfiguration) {
         return new ActiveMQXAConnectionFactory(true, groupConfiguration);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final DiscoveryGroupConfiguration groupConfiguration) {
         return new ActiveMQXAConnectionFactory(false, groupConfiguration);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithHA(final TransportConfiguration... initialServers) {
         return new ActiveMQXAConnectionFactory(true, initialServers);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final TransportConfiguration... transportConfigurations) {
         return new ActiveMQXAConnectionFactory(false, transportConfigurations);
      }
   },
   QUEUE_XA_CF {
      public ActiveMQConnectionFactory createConnectionFactoryWithHA(final DiscoveryGroupConfiguration groupConfiguration) {
         return new ActiveMQXAQueueConnectionFactory(true, groupConfiguration);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final DiscoveryGroupConfiguration groupConfiguration) {
         return new ActiveMQXAQueueConnectionFactory(false, groupConfiguration);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithHA(final TransportConfiguration... initialServers) {
         return new ActiveMQXAQueueConnectionFactory(true, initialServers);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final TransportConfiguration... transportConfigurations) {
         return new ActiveMQXAQueueConnectionFactory(false, transportConfigurations);
      }
   },
   TOPIC_XA_CF {
      public ActiveMQConnectionFactory createConnectionFactoryWithHA(final DiscoveryGroupConfiguration groupConfiguration) {
         return new ActiveMQXATopicConnectionFactory(true, groupConfiguration);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final DiscoveryGroupConfiguration groupConfiguration) {
         return new ActiveMQXATopicConnectionFactory(false, groupConfiguration);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithHA(final TransportConfiguration... initialServers) {
         return new ActiveMQXATopicConnectionFactory(true, initialServers);
      }

      public ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final TransportConfiguration... transportConfigurations) {
         return new ActiveMQXATopicConnectionFactory(false, transportConfigurations);
      }
   };

   public int intValue() {
      int val = 0;
      switch (this) {
         case CF:
            val = 0;
            break;
         case QUEUE_CF:
            val = 1;
            break;
         case TOPIC_CF:
            val = 2;
            break;
         case XA_CF:
            val = 3;
            break;
         case QUEUE_XA_CF:
            val = 4;
            break;
         case TOPIC_XA_CF:
            val = 5;
            break;
      }
      return val;
   }

   public static JMSFactoryType valueOf(int val) {
      JMSFactoryType type;
      switch (val) {
         case 0:
            type = CF;
            break;
         case 1:
            type = QUEUE_CF;
            break;
         case 2:
            type = TOPIC_CF;
            break;
         case 3:
            type = XA_CF;
            break;
         case 4:
            type = QUEUE_XA_CF;
            break;
         case 5:
            type = TOPIC_XA_CF;
            break;
         default:
            type = XA_CF;
            break;
      }
      return type;
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
    * @return the ActiveMQConnectionFactory
    */
   public abstract ActiveMQConnectionFactory createConnectionFactoryWithHA(final DiscoveryGroupConfiguration groupConfiguration);

   /**
    * Create an ActiveMQConnectionFactory which creates session factories from a set of live servers, no HA backup information is propagated to the client
    * <p>
    * The UDP address and port are used to listen for live servers in the cluster
    *
    * @param groupConfiguration
    * @return the ActiveMQConnectionFactory
    */
   public abstract ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final DiscoveryGroupConfiguration groupConfiguration);

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
    * @param initialServers The initial set of servers used to make a connection to the cluster.
    *                       Each one is tried in turn until a successful connection is made. Once a connection
    *                       is made, the cluster topology is downloaded and the rest of the list is ignored.
    * @return the ActiveMQConnectionFactory
    */
   public abstract ActiveMQConnectionFactory createConnectionFactoryWithHA(final TransportConfiguration... initialServers);

   /**
    * Create an ActiveMQConnectionFactory which creates session factories using a static list of
    * transportConfigurations.
    * <p>
    * The ActiveMQConnectionFactory is not updated automatically as the cluster topology changes, and
    * no HA backup information is propagated to the client
    *
    * @param transportConfigurations
    * @return the ActiveMQConnectionFactory
    */
   public abstract ActiveMQConnectionFactory createConnectionFactoryWithoutHA(final TransportConfiguration... transportConfigurations);


}
