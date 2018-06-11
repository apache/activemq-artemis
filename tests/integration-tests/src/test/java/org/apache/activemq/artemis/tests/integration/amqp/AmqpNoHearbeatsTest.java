/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.qpid.proton.engine.Connection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AmqpNoHearbeatsTest extends AmqpClientTestSupport {

   @Parameterized.Parameters(name = "useOverride={0}")
   public static Collection<Object[]> parameters() {
      return Arrays.asList(new Object[][] {
         {true}, {false}
      });
   }

   @Parameterized.Parameter(0)
   public boolean useOverride;

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      if (useOverride) {
         server.getConfiguration().setConnectionTTLOverride(0);
      } else {
         server.getConfiguration().setConnectionTtlCheckInterval(500);
      }
   }


   @Override
   protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
      if (!useOverride) {
         params.put("amqpIdleTimeout", "0");
      }
   }


   @Test(timeout = 60000)
   public void testBrokerSendsHalfConfiguredIdleTimeout() throws Exception {
      AmqpClient client = createAmqpClient();
      assertNotNull(client);

      client.setValidator(new AmqpValidator() {

         @Override
         public void inspectOpenedResource(Connection connection) {
            assertEquals("idle timeout was not disabled", 0, connection.getTransport().getRemoteIdleTimeout());
         }
      });

      AmqpConnection connection = addConnection(client.connect());
      assertNotNull(connection);

      connection.getStateInspector().assertValid();
      connection.close();
   }

}
