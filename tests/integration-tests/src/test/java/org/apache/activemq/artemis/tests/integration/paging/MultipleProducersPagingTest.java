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
package org.apache.activemq.artemis.tests.integration.paging;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MultipleProducersPagingTest extends ActiveMQTestBase {

   private static final int CONSUMER_WAIT_TIME_MS = 250;
   private static final int PRODUCERS = 5;
   private static final long MESSAGES_PER_PRODUCER = 2000;
   private static final long TOTAL_MSG = MESSAGES_PER_PRODUCER * PRODUCERS;
   private ExecutorService executor;
   private CountDownLatch runnersLatch;
   private CyclicBarrier barrierLatch;

   private AtomicLong msgReceived;
   private AtomicLong msgSent;
   private final Set<Connection> connections = new HashSet<>();
   private EmbeddedJMS jmsServer;
   private ConnectionFactory cf;
   private Queue queue;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      executor = Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory());

      AddressSettings addressSettings = new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE).setPageSizeBytes(50000).setMaxSizeBytes(404850);

      Configuration config = createBasicConfig().setPersistenceEnabled(false).setAddressesSettings(Collections.singletonMap("#", addressSettings)).setAcceptorConfigurations(Collections.singleton(new TransportConfiguration(NettyAcceptorFactory.class.getName()))).setConnectorConfigurations(Collections.singletonMap("netty", new TransportConfiguration(NettyConnectorFactory.class.getName())));

      final JMSConfiguration jmsConfig = new JMSConfigurationImpl();
      jmsConfig.getConnectionFactoryConfigurations().add(new ConnectionFactoryConfigurationImpl().setName("cf").setConnectorNames(Arrays.asList("netty")).setBindings("/cf"));
      jmsConfig.getQueueConfigurations().add(new JMSQueueConfigurationImpl().setName("simple").setSelector("").setDurable(false).setBindings("/queue/simple"));

      jmsServer = new EmbeddedJMS();
      jmsServer.setConfiguration(config);
      jmsServer.setJmsConfiguration(jmsConfig);
      jmsServer.start();

      cf = (ConnectionFactory) jmsServer.lookup("/cf");
      queue = (Queue) jmsServer.lookup("/queue/simple");

      barrierLatch = new CyclicBarrier(PRODUCERS + 1);
      runnersLatch = new CountDownLatch(PRODUCERS + 1);
      msgReceived = new AtomicLong(0);
      msgSent = new AtomicLong(0);
   }

   @Test
   public void testQueue() throws InterruptedException {
      executor.execute(new ConsumerRun());
      for (int i = 0; i < PRODUCERS; i++) {
         executor.execute(new ProducerRun());
      }
      Assert.assertTrue("must take less than a minute to run", runnersLatch.await(1, TimeUnit.MINUTES));
      Assert.assertEquals("number sent", TOTAL_MSG, msgSent.longValue());
      Assert.assertEquals("number received", TOTAL_MSG, msgReceived.longValue());
   }

   private synchronized Session createSession() throws JMSException {
      Connection connection = cf.createConnection();
      connections.add(connection);
      connection.start();
      return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
   }

   final class ConsumerRun implements Runnable {

      @Override
      public void run() {
         try {
            Session session = createSession();
            MessageConsumer consumer = session.createConsumer(queue);
            barrierLatch.await();
            while (true) {
               Message msg = consumer.receive(CONSUMER_WAIT_TIME_MS);
               if (msg == null)
                  break;
               msgReceived.incrementAndGet();
            }
         } catch (Exception e) {
            throw new RuntimeException(e);
         } finally {
            runnersLatch.countDown();
         }
      }

   }

   final class ProducerRun implements Runnable {

      @Override
      public void run() {
         try {
            Session session = createSession();
            MessageProducer producer = session.createProducer(queue);
            barrierLatch.await();

            for (int i = 0; i < MESSAGES_PER_PRODUCER; i++) {
               producer.send(session.createTextMessage(this.hashCode() + " counter " + i));
               msgSent.incrementAndGet();
            }
         } catch (Exception cause) {
            throw new RuntimeException(cause);
         } finally {
            runnersLatch.countDown();
         }
      }
   }

   @Override
   @After
   public void tearDown() throws Exception {
      executor.shutdown();
      for (Connection conn : connections) {
         conn.close();
      }
      connections.clear();
      if (jmsServer != null)
         jmsServer.stop();
      super.tearDown();
   }
}
