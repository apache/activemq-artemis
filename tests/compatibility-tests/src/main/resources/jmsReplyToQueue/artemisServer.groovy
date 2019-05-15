package jmsReplyToQueue

import org.apache.activemq.artemis.api.core.RoutingType
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl
import org.apache.activemq.artemis.core.server.JournalType

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

// starts an artemis server
import org.apache.activemq.artemis.core.server.impl.AddressInfo
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS

String folder = arg[0];
String queueAddress = "jms.queue.myQueue";
String replyQueueAddress = "jms.queue.myReplyQueue";

configuration = new ConfigurationImpl();
configuration.setJournalType(JournalType.NIO);
configuration.setBrokerInstance(new File(folder + "/server"));
configuration.addAcceptorConfiguration("artemis", "tcp://0.0.0.0:61616");
configuration.setSecurityEnabled(false);
configuration.setPersistenceEnabled(false);


jmsConfiguration = new JMSConfigurationImpl();

server = new EmbeddedJMS();
server.setConfiguration(configuration);
server.setJmsConfiguration(jmsConfiguration);
server.start();

server.getActiveMQServer().addAddressInfo(new AddressInfo(SimpleString.toSimpleString(queueAddress), RoutingType.ANYCAST));
server.getActiveMQServer().createQueue(SimpleString.toSimpleString(queueAddress), RoutingType.ANYCAST, SimpleString.toSimpleString(queueAddress), null, true, false);

server.getActiveMQServer().addAddressInfo(new AddressInfo(SimpleString.toSimpleString(replyQueueAddress), RoutingType.ANYCAST));
server.getActiveMQServer().createQueue(SimpleString.toSimpleString(replyQueueAddress), RoutingType.ANYCAST, SimpleString.toSimpleString(replyQueueAddress), null, true, false);
