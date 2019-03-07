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
package org.apache.activemq.artemis.tests.integration.federation;

import java.util.Collections;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.FederationConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationUpstreamConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationAddressPolicyConfiguration;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Before;
import org.junit.Test;

/**
 * Federated Address Test
 */
public class FederatedAddressTest extends FederatedTestBase {


   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }


   protected ConnectionFactory getCF(int i) throws Exception {
      return new ActiveMQConnectionFactory("vm://" + i);
   }



   @Test
   public void testFederatedAddressReplication() throws Exception {
      String address = getName();

      FederationConfiguration federationConfiguration = createFederationConfiguration("server1", address);
      getServer(0).getConfiguration().getFederationConfigurations().add(federationConfiguration);
      getServer(0).getFederationManager().deploy();

      FederationConfiguration federationConfiguration2 = createFederationConfiguration("server0", address);
      getServer(1).getConfiguration().getFederationConfigurations().add(federationConfiguration2);
      getServer(1).getFederationManager().deploy();

      ConnectionFactory cf1 = getCF(1);
      ConnectionFactory cf0 = getCF(0);
      try (Connection connection1 = cf1.createConnection(); Connection connection0 = cf0.createConnection()) {
         connection1.start();
         connection0.start();

         Session session1 = connection1.createSession();
         Topic topic1 = session1.createTopic(address);
         MessageProducer producer = session1.createProducer(topic1);
         producer.send(session1.createTextMessage("hello"));


         Session session0 = connection0.createSession();
         Topic topic0 = session0.createTopic(address);
         MessageConsumer consumer0 = session0.createConsumer(topic0);

         Wait.waitFor(() -> getServer(1).getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(address)).getBindings().size() == 1);

         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer0.receive(10000));


         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer0.receive(10000));

         MessageConsumer consumer1 = session1.createConsumer(topic1);

         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer1.receive(10000));
         assertNotNull(consumer0.receive(10000));
         consumer1.close();

         //Groups
         producer.send(session1.createTextMessage("hello"));
         assertNotNull(consumer0.receive(10000));

         producer.send(createTextMessage(session1, "groupA"));

         assertNotNull(consumer0.receive(10000));
         consumer1 = session1.createConsumer(topic1);

         producer.send(createTextMessage(session1, "groupA"));
         assertNotNull(consumer1.receive(10000));
         assertNotNull(consumer0.receive(10000));

      }

   }


   @Test
   public void testFederatedAddressDeployAfterQueuesExist() throws Exception {
      String address = getName();

      ConnectionFactory cf1 = getCF(1);
      ConnectionFactory cf0 = getCF(0);
      try (Connection connection1 = cf1.createConnection(); Connection connection0 = cf0.createConnection()) {
         connection1.start();
         connection0.start();

         Session session1 = connection1.createSession();
         Topic topic1 = session1.createTopic(address);
         MessageProducer producer = session1.createProducer(topic1);
         producer.send(session1.createTextMessage("hello"));


         Session session0 = connection0.createSession();
         Topic topic0 = session0.createTopic(address);
         MessageConsumer consumer0 = session0.createConsumer(topic0);


         producer.send(session1.createTextMessage("hello"));

         assertNull(consumer0.receive(100));

         FederationConfiguration federationConfiguration = createFederationConfiguration("server1", address);
         getServer(0).getConfiguration().getFederationConfigurations().add(federationConfiguration);
         getServer(0).getFederationManager().deploy();

         Wait.waitFor(() -> getServer(1).getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(address)).getBindings().size() == 1);


         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer0.receive(10000));

      }
   }

   @Test
   public void testFederatedAddressRemoteBrokerRestart() throws Exception {
      String address = getName();

      FederationConfiguration federationConfiguration = createFederationConfiguration("server1", address);
      getServer(0).getConfiguration().getFederationConfigurations().add(federationConfiguration);
      getServer(0).getFederationManager().deploy();

      ConnectionFactory cf1 = getCF(1);
      ConnectionFactory cf0 = getCF(0);
      try (Connection connection1 = cf1.createConnection(); Connection connection0 = cf0.createConnection()) {
         connection1.start();
         connection0.start();

         Session session1 = connection1.createSession();
         Topic topic1 = session1.createTopic(address);
         MessageProducer producer = session1.createProducer(topic1);
         producer.send(session1.createTextMessage("hello"));


         Session session0 = connection0.createSession();
         Topic topic0 = session0.createTopic(address);
         MessageConsumer consumer0 = session0.createConsumer(topic0);

         Wait.waitFor(() -> getServer(1).getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(address)).getBindings().size() == 1);

         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer0.receive(10000));


         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer0.receive(1000));

         connection1.close();
         getServer(1).stop();
         Wait.waitFor(() -> !getServer(1).isStarted());

         assertNull(consumer0.receive(100));

         getServer(1).start();
         Wait.waitFor(() -> getServer(1).isActive());

         Connection c1 = cf1.createConnection();
         c1.start();
         Wait.waitFor(() -> getServer(1).isStarted());
         session1 = c1.createSession();
         topic1 = session1.createTopic(address);
         producer = session1.createProducer(topic1);

         Wait.waitFor(() -> getServer(1).getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(address)).getBindings().size() == 1);
         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer0.receive(1000));
      }
   }

   @Test
   public void testFederatedAddressLocalBrokerRestart() throws Exception {
      String address = getName();

      FederationConfiguration federationConfiguration = createFederationConfiguration("server1", address);
      getServer(0).getConfiguration().getFederationConfigurations().add(federationConfiguration);
      getServer(0).getFederationManager().deploy();

      ConnectionFactory cf1 = getCF(1);
      ConnectionFactory cf0 = getCF(0);
      try (Connection connection1 = cf1.createConnection(); Connection connection0 = cf0.createConnection()) {
         connection1.start();
         connection0.start();

         Session session1 = connection1.createSession();
         Topic topic1 = session1.createTopic(address);
         MessageProducer producer = session1.createProducer(topic1);
         producer.send(session1.createTextMessage("hello"));


         Session session0 = connection0.createSession();
         Topic topic0 = session0.createTopic(address);
         MessageConsumer consumer0 = session0.createConsumer(topic0);

         Wait.waitFor(() -> getServer(1).getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(address)).getBindings().size() == 1);

         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer0.receive(10000));


         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer0.receive(1000));

         connection0.close();
         getServer(0).stop();
         Wait.waitFor(() -> !getServer(0).isStarted());

         producer.send(session1.createTextMessage("hello"));

         getServer(0).start();
         Wait.waitFor(() -> getServer(0).isActive());

         Connection newConnection = getCF(0).createConnection();
         newConnection.start();
         session0 = newConnection.createSession();
         topic0 =  session0.createTopic(address);
         consumer0 = session0.createConsumer(topic0);

         Wait.waitFor(() -> getServer(1).getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(address)).getBindings().size() == 1);
         producer.send(session1.createTextMessage("hello"));

         assertNotNull(consumer0.receive(1000));

         newConnection.close();
      }
   }


   private FederationConfiguration createFederationConfiguration(String connector, String address) {
      FederationUpstreamConfiguration upstreamConfiguration = new FederationUpstreamConfiguration();
      upstreamConfiguration.setName(connector);
      upstreamConfiguration.getConnectionConfiguration().setStaticConnectors(Collections.singletonList(connector));
      upstreamConfiguration.getConnectionConfiguration().setCircuitBreakerTimeout(-1);
      upstreamConfiguration.addPolicyRef("AddressPolicy" + address);


      FederationAddressPolicyConfiguration addressPolicyConfiguration = new FederationAddressPolicyConfiguration();
      addressPolicyConfiguration.setName( "AddressPolicy" + address);
      addressPolicyConfiguration.addInclude(new FederationAddressPolicyConfiguration.Matcher().setAddressMatch(address));
      addressPolicyConfiguration.setMaxHops(1);

      FederationConfiguration federationConfiguration = new FederationConfiguration();
      federationConfiguration.setName("default");
      federationConfiguration.addUpstreamConfiguration(upstreamConfiguration);
      federationConfiguration.addFederationPolicy(addressPolicyConfiguration);

      return federationConfiguration;
   }

   private Message createTextMessage(Session session1, String group) throws JMSException {
      Message message = session1.createTextMessage("hello");
      message.setStringProperty("JMSXGroupID", group);
      return message;
   }


}
