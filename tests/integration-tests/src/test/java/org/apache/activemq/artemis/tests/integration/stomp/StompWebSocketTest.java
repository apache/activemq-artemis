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
package org.apache.activemq.artemis.tests.integration.stomp;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.protocol.stomp.StompProtocolManagerFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class StompWebSocketTest extends ActiveMQTestBase {

   private JMSServerManager server;

   /**
    * to test the Stomp over Web Sockets protocol,
    * uncomment the sleep call and run the stomp-websockets Javascript test suite
    * from http://github.com/jmesnil/stomp-websocket
    */
   @Test
   public void testConnect() throws Exception {
      //Thread.sleep(10000000);
   }

   // Implementation methods
   //-------------------------------------------------------------------------
   @Override
   @Before
   public void setUp() throws Exception {
      server = createServer();
      server.start();
   }

   /**
    * @return
    * @throws Exception
    */
   private JMSServerManager createServer() throws Exception {
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, StompProtocolManagerFactory.STOMP_PROTOCOL_NAME);
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT + 1);
      TransportConfiguration stompTransport = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);

      Configuration config = createBasicConfig().addAcceptorConfiguration(stompTransport).addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName())).addQueueConfiguration(new CoreQueueConfiguration().setAddress(getQueueName()).setName(getQueueName()).setDurable(false));

      ActiveMQServer activeMQServer = addServer(ActiveMQServers.newActiveMQServer(config));

      JMSConfiguration jmsConfig = new JMSConfigurationImpl();
      server = new JMSServerManagerImpl(activeMQServer, jmsConfig);
      server.setRegistry(null);
      return server;
   }

   protected String getQueueName() {
      return "/queue/test";
   }
}
