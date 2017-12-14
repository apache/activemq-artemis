package clients
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

// This script is called by sendMessages.groovy

import javax.jms.ConnectionFactory;
import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSConstants;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.jms.client.HornetQJMSConnectionFactory;


properties = new HashMap();
properties.put(TransportConstants.HOST_PROP_NAME, "localhost");
properties.put(TransportConstants.PORT_PROP_NAME, "61616");
configuration = new TransportConfiguration(NettyConnectorFactory.class.getName(), properties);
cf = new HornetQJMSConnectionFactory(false, configuration);
