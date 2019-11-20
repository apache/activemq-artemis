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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import java.util.Collections;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.FederationConfiguration;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationAddressPolicyConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationDownstreamConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationTransformerConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationUpstreamConfiguration;
import org.apache.activemq.artemis.core.server.transformer.Transformer;
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
   public void testDownstreamFederatedAddressReplication() throws Exception {
      String address = getName();

      FederationConfiguration federationConfiguration = createDownstreamFederationConfiguration("server1", address,
          getServer(0).getConfiguration().getTransportConfigurations("server0")[0]);
      getServer(0).getConfiguration().getFederationConfigurations().add(federationConfiguration);
      getServer(0).getFederationManager().deploy();

      FederationConfiguration federationConfiguration2 = createDownstreamFederationConfiguration("server0", address,
          getServer(1).getConfiguration().getTransportConfigurations("server1")[0]);
      getServer(1).getConfiguration().getFederationConfigurations().add(federationConfiguration2);
      getServer(1).getFederationManager().deploy();

      testFederatedAddressReplication(address);
   }

   @Test
   public void testDownstreamFederatedAddressReplicationRef() throws Exception {
      String address = getName();

      FederationConfiguration federationConfiguration = createDownstreamFederationConfiguration("server1", address,
         "server0");
      getServer(0).getConfiguration().getFederationConfigurations().add(federationConfiguration);
      getServer(0).getFederationManager().deploy();

      FederationConfiguration federationConfiguration2 = createDownstreamFederationConfiguration("server0", address,
          "server1");
      getServer(1).getConfiguration().getFederationConfigurations().add(federationConfiguration2);
      getServer(1).getFederationManager().deploy();

      testFederatedAddressReplication(address);
   }

   @Test
   public void testDownstreamFederatedAddressReplicationRefOneWay() throws Exception {
      String address = getName();

      FederationConfiguration federationConfiguration2 = createDownstreamFederationConfiguration("server0", address,
          "server1");
      getServer(1).getConfiguration().getFederationConfigurations().add(federationConfiguration2);
      getServer(1).getFederationManager().deploy();

      testFederatedAddressReplication(address);
   }

   @Test
   public void testUpstreamFederatedAddressReplication() throws Exception {
      String address = getName();

      FederationConfiguration federationConfiguration = createUpstreamFederationConfiguration("server1", address);
      getServer(0).getConfiguration().getFederationConfigurations().add(federationConfiguration);
      getServer(0).getFederationManager().deploy();

      FederationConfiguration federationConfiguration2 = createUpstreamFederationConfiguration("server0", address);
      getServer(1).getConfiguration().getFederationConfigurations().add(federationConfiguration2);
      getServer(1).getFederationManager().deploy();

      testFederatedAddressReplication(address);
   }

   @Test
   public void testDownstreamFederatedAddressReplicationRefOneWayTransformer() throws Exception {
      String address = getName();

      FederationConfiguration federationConfiguration2 = createDownstreamFederationConfiguration("server0", address, "server1");
      addTransformerConfiguration(federationConfiguration2, address);
      getServer(1).getConfiguration().getFederationConfigurations().add(federationConfiguration2);
      getServer(1).getFederationManager().deploy();

      verifyTransformer(address);
   }

   private void verifyTransformer(String address) throws Exception {
      ConnectionFactory cf1 = getCF(1);
      ConnectionFactory cf0 = getCF(0);
      try (Connection connection1 = cf1.createConnection(); Connection connection0 = cf0.createConnection()) {
         connection1.start();
         connection0.start();

         Session session1 = connection1.createSession();
         Topic topic1 = session1.createTopic(address);
         MessageProducer producer1 = session1.createProducer(topic1);

         Session session0 = connection0.createSession();
         Topic topic0 = session0.createTopic(address);
         MessageConsumer consumer0 = session0.createConsumer(topic0);

         assertTrue(Wait.waitFor(() -> getServer(1).getPostOffice().getBindingsForAddress(
            SimpleString.toSimpleString(address)).getBindings().size() == 1));

         producer1.send(session1.createTextMessage("hello"));
         Message message = consumer0.receive(1000);
         assertNotNull(message);
         assertEquals(message.getBooleanProperty(FederatedQueueTest.TestTransformer.TEST_PROPERTY), true);
      }
   }

   @Test
   public void testUpstreamFederatedAddressReplicationOneWay() throws Exception {
      String address = getName();

      FederationConfiguration federationConfiguration = createUpstreamFederationConfiguration("server1", address);
      getServer(0).getConfiguration().getFederationConfigurations().add(federationConfiguration);
      getServer(0).getFederationManager().deploy();

      testFederatedAddressReplication(address);
   }

   @Test
   public void testUpstreamFederatedAddressReplicationOneWayTransformer() throws Exception {
      String address = getName();

      FederationConfiguration federationConfiguration = createUpstreamFederationConfiguration("server1", address);
      addTransformerConfiguration(federationConfiguration, address);
      getServer(0).getConfiguration().getFederationConfigurations().add(federationConfiguration);
      getServer(0).getFederationManager().deploy();

      verifyTransformer(address);
   }

   private void testFederatedAddressReplication(String address) throws Exception {

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

         assertTrue(Wait.waitFor(() -> getServer(1).getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(address)).getBindings().size() == 1));

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

         FederationConfiguration federationConfiguration = createUpstreamFederationConfiguration("server1", address);
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

      FederationConfiguration federationConfiguration = createUpstreamFederationConfiguration("server1", address);
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

      FederationConfiguration federationConfiguration = createUpstreamFederationConfiguration("server1", address);
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

   @Test
   public void testFederatedAddressChainOfBrokers() throws Exception {
      String address = getName();

      //Set queue up on all three brokers
//      for (int i = 0; i < 3; i++) {
//         getServer(i).createQueue(SimpleString.toSimpleString(queueName), RoutingType.ANYCAST, SimpleString.toSimpleString(queueName), null, true, false);
//      }

      //Connect broker 0 (consumer will be here at end of chain) to broker 1
      FederationConfiguration federationConfiguration0 = createUpstreamFederationConfiguration("server1", address, 2);
      getServer(0).getConfiguration().getFederationConfigurations().add(federationConfiguration0);
      getServer(0).getFederationManager().deploy();

      //Connect broker 1 (middle of chain) to broker 2
      FederationConfiguration federationConfiguration1 = createUpstreamFederationConfiguration("server2", address, 2);
      getServer(1).getConfiguration().getFederationConfigurations().add(federationConfiguration1);
      getServer(1).getFederationManager().deploy();
      //Broker 2 we dont setup any federation as he is the upstream (head of the chain)

      //Now the test.


      ConnectionFactory cf2 = getCF(2);
      ConnectionFactory cf0 = getCF(0);
      try (Connection connection2 = cf2.createConnection(); Connection connection0 = cf0.createConnection()) {
         connection0.start();
         Session session0 = connection0.createSession();
         Topic topic0 = session0.createTopic(address);

         connection2.start();
         Session session2 = connection2.createSession();
         Topic topic2 = session2.createTopic(address);

         MessageProducer producer2 = session2.createProducer(topic2);
         MessageConsumer consumer0 = session0.createConsumer(topic0);

         assertTrue(Wait.waitFor(() -> getServer(1).getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(address)).getBindings().size() == 1));
         assertTrue(Wait.waitFor(() -> getServer(2).getPostOffice().getBindingsForAddress(SimpleString.toSimpleString(address)).getBindings().size() == 1));

         //Test producers being on broker 2 and consumer on broker 0, with broker 2 being in the middle of the chain.
         producer2.send(session2.createTextMessage("hello"));
         assertNotNull(consumer0.receive(1000));
      }
   }

   private FederationConfiguration createFederationConfiguration(String address, int hops) {
      FederationAddressPolicyConfiguration addressPolicyConfiguration = new FederationAddressPolicyConfiguration();
      addressPolicyConfiguration.setName( "AddressPolicy" + address);
      addressPolicyConfiguration.addInclude(new FederationAddressPolicyConfiguration.Matcher().setAddressMatch(address));
      addressPolicyConfiguration.setMaxHops(hops);

      FederationConfiguration federationConfiguration = new FederationConfiguration();
      federationConfiguration.setName("default");
      federationConfiguration.addFederationPolicy(addressPolicyConfiguration);

      return federationConfiguration;
   }

   private FederationConfiguration createUpstreamFederationConfiguration(String connector, String address, int hops) {
      FederationUpstreamConfiguration upstreamConfiguration = new FederationUpstreamConfiguration();
      upstreamConfiguration.setName(connector);
      upstreamConfiguration.getConnectionConfiguration().setStaticConnectors(Collections.singletonList(connector));
      upstreamConfiguration.getConnectionConfiguration().setCircuitBreakerTimeout(-1);
      upstreamConfiguration.addPolicyRef("AddressPolicy" + address);

      FederationConfiguration federationConfiguration = createFederationConfiguration(address, hops);
      federationConfiguration.addUpstreamConfiguration(upstreamConfiguration);

      return federationConfiguration;
   }
   private FederationConfiguration createUpstreamFederationConfiguration(String connector, String address) {
      return createUpstreamFederationConfiguration(connector, address, 1);
   }

   private FederationConfiguration createDownstreamFederationConfiguration(String connector, String address, TransportConfiguration transportConfiguration) {
      return createDownstreamFederationConfiguration(connector, address, transportConfiguration, 1);
   }

   private FederationConfiguration createDownstreamFederationConfiguration(String connector, String address, TransportConfiguration transportConfiguration,
       int hops) {
      FederationDownstreamConfiguration downstreamConfiguration = new FederationDownstreamConfiguration();
      downstreamConfiguration.setName(connector);
      downstreamConfiguration.getConnectionConfiguration().setStaticConnectors(Collections.singletonList(connector));
      downstreamConfiguration.getConnectionConfiguration().setCircuitBreakerTimeout(-1);
      downstreamConfiguration.addPolicyRef("AddressPolicy" + address);
      downstreamConfiguration.setUpstreamConfiguration(transportConfiguration);

      FederationConfiguration federationConfiguration = createFederationConfiguration(address, hops);
      federationConfiguration.addDownstreamConfiguration(downstreamConfiguration);

      return federationConfiguration;
   }

   private FederationConfiguration createDownstreamFederationConfiguration(String connector, String address, String transportConfigurationRef,
       int hops) {
      FederationDownstreamConfiguration downstreamConfiguration = new FederationDownstreamConfiguration();
      downstreamConfiguration.setName(connector);
      downstreamConfiguration.getConnectionConfiguration().setStaticConnectors(Collections.singletonList(connector));
      downstreamConfiguration.getConnectionConfiguration().setCircuitBreakerTimeout(-1);
      downstreamConfiguration.addPolicyRef("AddressPolicy" + address);
      downstreamConfiguration.setUpstreamConfigurationRef(transportConfigurationRef);

      FederationConfiguration federationConfiguration = createFederationConfiguration(address, hops);
      federationConfiguration.addDownstreamConfiguration(downstreamConfiguration);

      return federationConfiguration;
   }

   private FederationConfiguration createDownstreamFederationConfiguration(String connector, String address, String transportConfigurationRef) {
      return createDownstreamFederationConfiguration(connector, address, transportConfigurationRef, 1);
   }

   private void addTransformerConfiguration(final FederationConfiguration federationConfiguration, final String address) {
      federationConfiguration.addTransformerConfiguration(
         new FederationTransformerConfiguration("transformer", new TransformerConfiguration(TestTransformer.class.getName())));
      FederationAddressPolicyConfiguration policy = (FederationAddressPolicyConfiguration) federationConfiguration.getFederationPolicyMap().get("AddressPolicy" + address);
      policy.setTransformerRef("transformer");
   }

   private Message createTextMessage(Session session1, String group) throws JMSException {
      Message message = session1.createTextMessage("hello");
      message.setStringProperty("JMSXGroupID", group);
      return message;
   }

   public static class TestTransformer implements Transformer {

      static String TEST_PROPERTY = "transformed";

      @Override
      public org.apache.activemq.artemis.api.core.Message transform(org.apache.activemq.artemis.api.core.Message message) {
         message.putBooleanProperty(TEST_PROPERTY, true);
         return message;
      }
   }

}
