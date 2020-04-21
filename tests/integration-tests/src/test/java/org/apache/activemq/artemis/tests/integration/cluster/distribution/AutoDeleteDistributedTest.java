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
package org.apache.activemq.artemis.tests.integration.cluster.distribution;

import javax.jms.BytesMessage;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.MessageListener;
import javax.naming.NamingException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AutoDeleteDistributedTest extends ClusterTestBase {
   private static final Logger log = Logger.getLogger(AutoDeleteDistributedTest.class);

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      start();
   }

   /**
    * @param serverID
    * @return
    * @throws Exception
    */
   @Override
   protected ConfigurationImpl createBasicConfig(final int serverID) {
      ConfigurationImpl configuration = super.createBasicConfig(serverID);

      // this particular test can't rush things as it's clustering it has to come after proper syncs
      configuration.setJournalBufferTimeout_AIO(ActiveMQDefaultConfiguration.getDefaultJournalBufferTimeoutAio()).
         setJournalBufferTimeout_NIO(ActiveMQDefaultConfiguration.getDefaultJournalBufferTimeoutNio());

      return configuration;
   }


   private void start() throws Exception {
      setupServers();

      setRedistributionDelay(0);

      setupCluster(MessageLoadBalancingType.ON_DEMAND);

      servers[0].start();
      servers[1].start();
      servers[2].start();
   }

   protected boolean isNetty() {
      return true;
   }

   @Override
   protected void setSessionFactoryCreateLocator(int node, boolean ha, TransportConfiguration serverTotc) {
      super.setSessionFactoryCreateLocator(node, ha, serverTotc);

      locators[node].setConsumerWindowSize(0);

   }

   @Test
   public void testAutoDelete() throws Exception {

      AssertionLoggerHandler.startCapture();

      try {

         AtomicBoolean error = new AtomicBoolean(false);

         int messageCount = 30;

         JMSContext client1JmsContext = createContext(0);
         JMSContext client2JmsContext = createContext(1);

         final JMSConsumer client2JmsConsumer = client2JmsContext.createConsumer(client2JmsContext.createQueue("queues.myQueue"));
         final CountDownLatch onMessageReceived = new CountDownLatch(messageCount);
         client2JmsConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(final javax.jms.Message m) {
               log.debug("Message received. " + m);
               onMessageReceived.countDown();
            }
         });

        /*
         * sending a message to broker1
         */
         {

            final CountDownLatch onMessageSent = new CountDownLatch(1);
            final JMSProducer jmsProducer = client1JmsContext.createProducer();
            jmsProducer.setAsync(new javax.jms.CompletionListener() {
               @Override
               public void onCompletion(final javax.jms.Message m) {
                  log.debug("Message sent. " + m);
                  onMessageSent.countDown();
               }

               @Override
               public void onException(final javax.jms.Message m, final Exception ex) {
                  ex.printStackTrace();
                  error.set(true);
               }
            });
            for (int i = 0; i < messageCount; i++) {
               final BytesMessage jmsMsg = client1JmsContext.createBytesMessage();
               jmsMsg.setJMSType("MyType");
               jmsProducer.send(client1JmsContext.createQueue("queues.myQueue"), jmsMsg);
            }

            log.debug("Waiting for message to be sent...");
            onMessageSent.await(5, TimeUnit.SECONDS);
         }

         log.debug("Waiting for message to be received...");
         Assert.assertTrue(onMessageReceived.await(5, TimeUnit.SECONDS));

         client2JmsConsumer.close();
         Assert.assertFalse(error.get());

         Thread.sleep(100); // I couldn't make it to fail without a minimal sleep here
         Assert.assertFalse(AssertionLoggerHandler.findText("java.lang.IllegalStateException"));
         Assert.assertFalse(AssertionLoggerHandler.findText("Cannot find binding"));
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

   private JMSContext createContext(int server) throws NamingException {
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:" + (61617 + server));
      return cf.createContext();
   }

   protected void setupCluster(final MessageLoadBalancingType messageLoadBalancingType) throws Exception {
      setupClusterConnection("cluster0", "queues", messageLoadBalancingType, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", messageLoadBalancingType, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", messageLoadBalancingType, 1, isNetty(), 2, 0, 1);
   }

   protected void setRedistributionDelay(final long delay) {
      AddressSettings as = new AddressSettings().setRedistributionDelay(delay);

      getServer(0).getAddressSettingsRepository().addMatch("queues.*", as);
      getServer(1).getAddressSettingsRepository().addMatch("queues.*", as);
      getServer(2).getAddressSettingsRepository().addMatch("queues.*", as);
   }

   protected void setupServers() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());
      servers[0].getConfiguration().addAddressesSetting("*", new AddressSettings().setAutoCreateAddresses(true) //
         .setAutoCreateQueues(true) //
         .setAutoDeleteAddresses(true) //
         .setAutoDeleteQueues(true) //  --> this causes IllegalStateExceptions
         .setDefaultPurgeOnNoConsumers(true));
   }

   protected void stopServers() throws Exception {
      closeAllConsumers();

      closeAllSessionFactories();

      closeAllServerLocatorsFactories();

      stopServers(0, 1, 2);

      clearServer(0, 1, 2);
   }

}
