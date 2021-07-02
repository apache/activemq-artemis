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
package org.apache.activemq.artemis.tests.integration.openwire.management;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.artemis.tests.integration.openwire.OpenWireTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Before;
import org.junit.Test;

public class OpenWireDeleteQueueTest extends OpenWireTestBase {

   private ActiveMQServerControl serverControl;
   private SimpleString queueName1 = new SimpleString("queue1");

   private ConnectionFactory factory;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      serverControl = (ActiveMQServerControl) ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getActiveMQServerObjectName(), ActiveMQServerControl.class, mbeanServer);
      factory = new ActiveMQConnectionFactory("failover:(" + urlString + ")");
   }

   @Override
   protected void configureAddressSettings(Map<String, AddressSettings> addressSettingsMap) {
      addressSettingsMap.put("#", new AddressSettings().setAutoCreateQueues(false).setAutoCreateAddresses(false).setDeadLetterAddress(new SimpleString("ActiveMQ.DLQ")));
      addressSettingsMap.put(queueName1.toString(), new AddressSettings().setAutoCreateQueues(true).setAutoCreateAddresses(true).setDeadLetterAddress(new SimpleString("ActiveMQ.DLQ")));

   }

   @Override
   protected void extraServerConfig(Configuration serverConfig) {
      serverConfig.setJMXManagementEnabled(true);
      Set<TransportConfiguration> acceptorConfigs = serverConfig.getAcceptorConfigurations();
      for (TransportConfiguration tconfig : acceptorConfigs) {
         if ("netty".equals(tconfig.getName())) {
            Map<String, Object> params = tconfig.getExtraParams();
            params.put("supportAdvisory", false);
            params.put("suppressInternalManagementObjects", false);
         }
      }

   }
   @Test
   public void testDestroyQueueClosingConsumers() throws Exception {

      // need auto queue creation on this address to have failover reconnect recreate the deleted queue
      try (Connection connection = factory.createConnection()) {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination destination = session.createQueue(queueName1.toString());

         MessageProducer producer = session.createProducer(destination);
         CountDownLatch one = new CountDownLatch(1);
         CountDownLatch two = new CountDownLatch(2);
         CountDownLatch three = new CountDownLatch(3);

         final MessageConsumer messageConsumer = session.createConsumer(destination);
         messageConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
               one.countDown();
               two.countDown();
               three.countDown();
            }
         });

         producer.send(session.createTextMessage("one"));
         assertTrue(one.await(5, TimeUnit.SECONDS));

         assertTrue(Wait.waitFor(() -> {
            String bindings = serverControl.listBindingsForAddress(queueName1.toString());
            return bindings.contains(queueName1);
         }));

         // the test op, will force a disconnect, failover will kick in..
         serverControl.destroyQueue(queueName1.toString(), true);

         assertTrue("Binding gone!", Wait.waitFor(() -> {
            String bindings = serverControl.listBindingsForAddress(queueName1.toString());
            return !bindings.contains(queueName1);
         }));


         // expect a failover event
         producer.send(session.createTextMessage("two"));
         assertTrue(two.await(5, TimeUnit.SECONDS));

         assertTrue("binding auto created for message two", Wait.waitFor(() -> {
            String bindings = serverControl.listBindingsForAddress(queueName1.toString());
            return bindings.contains(queueName1);
         }, 5000));

         // sanity check on third
         producer.send(session.createTextMessage("three"));
         assertTrue(three.await(5, TimeUnit.SECONDS));

      }
   }
}
