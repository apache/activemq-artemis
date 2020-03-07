/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.mqtt;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTConnection;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptor;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.integration.mqtt.imported.MQTTTestSupport;
import org.apache.activemq.artemis.utils.Wait;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MQTTConnnectionCleanupTest extends MQTTTestSupport {

   @Override
   protected void addMQTTConnector() {

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PORT_PROP_NAME, "" + port);
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, "MQTT");
      params.put(TransportConstants.CONNECTIONS_ALLOWED, 1);
      params.put(TransportConstants.HOST_PROP_NAME, "localhost");

      TransportConfiguration mqtt = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, "MQTT");

      server.getConfiguration().addAcceptorConfiguration(mqtt);
   }

   @Test(timeout = 30 * 1000)
   public void testBadClient() throws Exception {
      MQTT mqtt = createMQTTConnection();
      mqtt.setClientId("");
      mqtt.setCleanSession(true);
      BlockingConnection connection = mqtt.blockingConnection();
      connection.connect();

      try {
         connection = mqtt.blockingConnection();
         connection.connect();
         fail("second connection shouldn't be allowed");
      } catch (Exception e) {
         //ignore.
      }

      NettyAcceptor acceptor = (NettyAcceptor) server.getRemotingService().getAcceptor("MQTT");
      assertEquals(1, acceptor.getConnections().size());

      //now simulate a bad client by manually fail the server connection
      RemotingConnection conn = server.getRemotingService().getConnections().iterator().next();

      assertTrue(conn instanceof MQTTConnection);

      conn.fail(new ActiveMQException("testBadClient"));

      Wait.assertEquals(0, ()->acceptor.getConnections().size());

      //another connection should be ok
      connection = mqtt.blockingConnection();
      connection.connect();
      connection.disconnect();
   }

}
