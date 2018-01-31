package servers

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

// starts a clustered backup hornetq server
import org.hornetq.api.core.TransportConfiguration
import org.hornetq.api.core.BroadcastGroupConfiguration
import org.hornetq.api.core.UDPBroadcastGroupConfiguration
import org.hornetq.api.core.DiscoveryGroupConfiguration
import org.hornetq.core.config.impl.ConfigurationImpl
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory
import org.hornetq.core.remoting.impl.netty.TransportConstants
import org.hornetq.jms.server.config.impl.JMSConfigurationImpl
import org.hornetq.jms.server.config.impl.*
import org.hornetq.jms.server.embedded.EmbeddedJMS
import org.hornetq.core.config.ClusterConnectionConfiguration

String folder = arg[0];
String id = arg[1];
String offset = arg[2];

configuration = new ConfigurationImpl();
configuration.setSecurityEnabled(false);
configuration.setJournalDirectory(folder + "/" + id + "/journal");
configuration.setBindingsDirectory(folder + "/" + id + "/binding");
configuration.setPagingDirectory(folder + "/" + id + "/paging");
configuration.setLargeMessagesDirectory(folder + "/" + id + "/largemessage");
configuration.setJournalType(org.hornetq.core.server.JournalType.NIO);
configuration.setPersistenceEnabled(true);

configuration.setFailoverOnServerShutdown(true);
configuration.setBackup(true);
configuration.setSharedStore(true);

HashMap map = new HashMap();
map.put(TransportConstants.HOST_PROP_NAME, "localhost");
map.put(TransportConstants.PORT_PROP_NAME, (61616 + Integer.parseInt(offset)));
TransportConfiguration tpc = new TransportConfiguration(NettyAcceptorFactory.class.getName(), map);
configuration.getAcceptorConfigurations().add(tpc);

TransportConfiguration connectorConfig = new TransportConfiguration(NettyConnectorFactory.class.getName(), map, "netty");
configuration.getConnectorConfigurations().put("netty", connectorConfig);

ClusterConnectionConfiguration cc = new ClusterConnectionConfiguration("test-cluster", "jms", "netty", 200,
        true,
        true,
        1,
        1024,
        "dg");
configuration.getClusterConfigurations().add(cc);

UDPBroadcastGroupConfiguration endpoint = new UDPBroadcastGroupConfiguration("231.7.7.7", 9876, null, -1);
List<String> connectors = new ArrayList<>();
connectors.add("netty");
BroadcastGroupConfiguration bcConfig = new BroadcastGroupConfiguration("bg", 250,
        connectors,
        endpoint);

configuration.getBroadcastGroupConfigurations().add(bcConfig);

DiscoveryGroupConfiguration dcConfig = new DiscoveryGroupConfiguration("dg", 5000, 5000, endpoint);
configuration.getDiscoveryGroupConfigurations().put(dcConfig.getName(), dcConfig);

jmsConfiguration = new JMSConfigurationImpl();

JMSQueueConfigurationImpl queueConfiguration = new JMSQueueConfigurationImpl("queue", null, true);
TopicConfigurationImpl topicConfiguration = new TopicConfigurationImpl("topic");


jmsConfiguration.getQueueConfigurations().add(queueConfiguration);
jmsConfiguration.getTopicConfigurations().add(topicConfiguration);
backupServer = new EmbeddedJMS();
backupServer.setConfiguration(configuration);
backupServer.setJmsConfiguration(jmsConfiguration);
backupServer.start();
