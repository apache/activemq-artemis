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
package org.apache.activemq.artemis.rest.test;

import java.util.HashMap;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Play with ActiveMQ
 */
public class RawAckTest {

   protected static ActiveMQServer activeMQServer;
   static ServerLocator serverLocator;
   static ClientSessionFactory sessionFactory;
   static ClientSessionFactory consumerSessionFactory;
   static ClientProducer producer;
   static ClientSession session;

   @BeforeClass
   public static void setup() throws Exception {
      Configuration configuration = new ConfigurationImpl().setPersistenceEnabled(false).setSecurityEnabled(false).addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

      activeMQServer = ActiveMQServers.newActiveMQServer(configuration);
      activeMQServer.start();

      HashMap<String, Object> transportConfig = new HashMap<>();

      serverLocator = new ServerLocatorImpl(false, new TransportConfiguration(InVMConnectorFactory.class.getName(), transportConfig));
      sessionFactory = serverLocator.createSessionFactory();
      consumerSessionFactory = serverLocator.createSessionFactory();

      SimpleString addr = SimpleString.toSimpleString("testQueue");
      activeMQServer.addAddressInfo(new AddressInfo(addr, RoutingType.MULTICAST));
      activeMQServer.createQueue(addr, RoutingType.MULTICAST, addr, null, false, false);
      session = sessionFactory.createSession(true, true);
      producer = session.createProducer(addr);
      session.start();
   }

   @AfterClass
   public static void shutdown() throws Exception {
      serverLocator.close();
      activeMQServer.stop();
   }

   static boolean passed = false;

   private static class MyThread extends Thread {

      final ClientConsumer consumer;

      private MyThread(ClientConsumer consumer) {
         this.consumer = consumer;
      }

      @Override
      public void run() {
         try {
            ClientMessage message = consumer.receiveImmediate();
            int size = message.getBodyBuffer().readInt();
            byte[] bytes = new byte[size];
            message.getBodyBuffer().readBytes(bytes);
            String str = new String(bytes);
            System.out.println(str);
            message.acknowledge();
            message = consumer.receive(1);
            if (message != null) {
               System.err.println("Not expecting another message: type=" + message.getType());
               throw new RuntimeException("Failed, receive extra message");
            }
            Assert.assertNull(message);
            passed = true;
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }

   @Test
   public void testAck() throws Exception {

      ClientMessage message;

      message = session.createMessage(Message.OBJECT_TYPE, false);
      message.getBodyBuffer().writeInt("hello".getBytes().length);
      message.getBodyBuffer().writeBytes("hello".getBytes());
      producer.send(message);

      Thread.sleep(100);

      ClientSession sessionConsumer = sessionFactory.createSession(true, true);
      ClientConsumer consumer = sessionConsumer.createConsumer("testQueue");
      sessionConsumer.start();

      MyThread t = new MyThread(consumer);

      t.start();
      t.join();
      Assert.assertTrue(passed);

      passed = false;

      message = session.createMessage(false);
      message.getBodyBuffer().writeInt("hello2".getBytes().length);
      message.getBodyBuffer().writeBytes("hello2".getBytes());
      producer.send(message);

      Thread.sleep(100);

      t = new MyThread(consumer);

      t.start();
      t.join();
      Assert.assertTrue(passed);

   }
}
