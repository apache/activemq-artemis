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

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.SpawnedVMSupport;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.qpid.proton.engine.Connection;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AmqpNoHearbeatsTest extends AmqpClientTestSupport {

   private static final int OK = 0x33;

   @Parameterized.Parameter(0)
   public boolean useOverride;

   @Parameterized.Parameters(name = "useOverride={0}")
   public static Collection<Object[]> parameters() {
      return Arrays.asList(new Object[][] {
         {true}, {false}
      });
   }


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
   public void testHeartless() throws Exception {
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

   // This test is validating a scenario where the client will leave with connection reset
   // This is done by setting soLinger=0 on the socket, which will make the system to issue a connection.reset instead of sending a
   // disconnect.
   @Test(timeout = 60000)
   public void testCloseConsumerOnConnectionReset() throws Exception {

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
      AmqpSession session = connection.createSession();
      AmqpReceiver receiver = session.createReceiver(getQueueName());

      // This test needs a remote process exiting without closing the socket
      // with soLinger=0 on the socket so it will issue a connection.reset
      Process p = SpawnedVMSupport.spawnVM(AmqpNoHearbeatsTest.class.getName(), getTestName(), getQueueName());
      Assert.assertEquals(OK, p.waitFor());

      AmqpSender sender = session.createSender(getQueueName());

      for (int i = 0; i < 10; i++) {
         AmqpMessage msg = new AmqpMessage();
         msg.setBytes(new byte[] {0});
         sender.send(msg);
      }

      receiver.flow(20);

      for (int i = 0; i < 10; i++) {
         AmqpMessage msg = receiver.receive(1, TimeUnit.SECONDS);
         Assert.assertNotNull(msg);
         msg.accept();
      }
   }

   public static void main(String[] arg) {
      if (arg.length == 2 && arg[0].startsWith("testCloseConsumerOnConnectionReset")) {
         try {
            String queueName = arg[1];
            AmqpClient client = new AmqpClient(new URI("tcp://127.0.0.1:5672?transport.soLinger=0"), null, null);
            AmqpConnection connection = client.connect();
            AmqpSession session = connection.createSession();
            AmqpReceiver receiver = session.createReceiver(queueName);
            receiver.flow(10);
            System.exit(OK);
         } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
         }
      } else {
         System.err.println("Test " + arg[0] + " unkown");
         System.exit(-2);
      }
   }

}
