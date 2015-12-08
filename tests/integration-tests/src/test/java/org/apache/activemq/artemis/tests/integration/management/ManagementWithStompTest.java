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
package org.apache.activemq.artemis.tests.integration.management;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.protocol.stomp.Stomp;
import org.apache.activemq.artemis.core.protocol.stomp.StompProtocolManagerFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ManagementWithStompTest extends ManagementTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ActiveMQServer server;

   protected ClientSession session;

   private Socket stompSocket;

   private ByteArrayOutputStream inputBuffer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testGetManagementAttributeFromStomp() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + queue + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      // retrieve the address of the queue
      frame = "\nSEND\n" + "destination:" + ActiveMQDefaultConfiguration.getDefaultManagementAddress() + "\n" +
         "reply-to:" + address + "\n" +
         "_AMQ_ResourceName:" + ResourceNames.CORE_QUEUE + queue + "\n" +
         "_AMQ_Attribute: Address\n\n" +
         Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      System.out.println(frame);
      Assert.assertTrue(frame.contains("_AMQ_OperationSucceeded:true"));
      // the address will be returned in the message body in a JSON array
      Assert.assertTrue(frame.contains("[\"" + address + "\"]"));

      frame = "UNSUBSCRIBE\n" + "destination:" + queue + "\n" +
         "receipt: 123\n\n" +
         Stomp.NULL;
      sendFrame(frame);
      waitForReceipt();

      String disconnectFrame = "DISCONNECT\n\n" + Stomp.NULL;
      sendFrame(disconnectFrame);

      session.deleteQueue(queue);
   }

   @Test
   public void testInvokeOperationFromStomp() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + queue + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      // count number of message with filter "color = 'blue'"
      frame = "\nSEND\n" + "destination:" + ActiveMQDefaultConfiguration.getDefaultManagementAddress() + "\n" +
         "reply-to:" + address + "\n" +
         "_AMQ_ResourceName:" + ResourceNames.CORE_QUEUE + queue + "\n" +
         "_AMQ_OperationName: countMessages\n\n" +
         "[\"color = 'blue'\"]" +
         Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      System.out.println(frame);
      Assert.assertTrue(frame.contains("_AMQ_OperationSucceeded:true"));
      // there is no such messages => 0 returned in a JSON array
      Assert.assertTrue(frame.contains("[0]"));

      frame = "UNSUBSCRIBE\n" + "destination:" + queue + "\n" +
         "receipt: 123\n\n" +
         Stomp.NULL;
      sendFrame(frame);
      waitForReceipt();

      String disconnectFrame = "DISCONNECT\n\n" + Stomp.NULL;
      sendFrame(disconnectFrame);

      session.deleteQueue(queue);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, StompProtocolManagerFactory.STOMP_PROTOCOL_NAME);
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT);
      TransportConfiguration stompTransport = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);

      Configuration config = createDefaultInVMConfig().addAcceptorConfiguration(stompTransport);

      server = addServer(ActiveMQServers.newActiveMQServer(config, mbeanServer, false, "brianm", "wombats"));

      server.start();

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true);
      ClientSessionFactory sf = createSessionFactory(locator);
      session = sf.createSession(false, true, false);
      session.start();

      stompSocket = new Socket("127.0.0.1", TransportConstants.DEFAULT_STOMP_PORT);
      inputBuffer = new ByteArrayOutputStream();
   }

   // Private -------------------------------------------------------

   public void sendFrame(String data) throws Exception {
      byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
      OutputStream outputStream = stompSocket.getOutputStream();
      for (int i = 0; i < bytes.length; i++) {
         outputStream.write(bytes[i]);
      }
      outputStream.flush();
   }

   public String receiveFrame(long timeOut) throws Exception {
      stompSocket.setSoTimeout((int) timeOut);
      InputStream is = stompSocket.getInputStream();
      int c = 0;
      for (;;) {
         c = is.read();
         if (c < 0) {
            throw new IOException("socket closed.");
         }
         else if (c == 0) {
            c = is.read();
            if (c != '\n') {
               byte[] ba = inputBuffer.toByteArray();
               System.out.println(new String(ba, StandardCharsets.UTF_8));
            }
            Assert.assertEquals("Expecting stomp frame to terminate with \0\n", c, '\n');
            byte[] ba = inputBuffer.toByteArray();
            inputBuffer.reset();
            return new String(ba, StandardCharsets.UTF_8);
         }
         else {
            inputBuffer.write(c);
         }
      }
   }

   protected void waitForReceipt() throws Exception {
      String frame = receiveFrame(50000);
      Assert.assertNotNull(frame);
      Assert.assertTrue(frame.indexOf("RECEIPT") > -1);
   }

   // Inner classes -------------------------------------------------

}
