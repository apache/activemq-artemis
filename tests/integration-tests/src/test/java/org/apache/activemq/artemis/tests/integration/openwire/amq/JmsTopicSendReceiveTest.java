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
package org.apache.activemq.artemis.tests.integration.openwire.amq;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Before;

/**
 * adapted from: JmsTopicSendReceiveTest
 */
public class JmsTopicSendReceiveTest extends JmsSendReceiveTestSupport {

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      if (durable) {
         connection.setClientID(getClass().getName());
      }

      System.out.println("Created connection: " + connection);

      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      consumeSession = createConsumerSession();

      System.out.println("Created session: " + session);
      producer = session.createProducer(null);
      producer.setDeliveryMode(deliveryMode);

      System.out.println("Created producer: " + producer + " delivery mode = " + (deliveryMode == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON_PERSISTENT"));

      if (topic) {
         consumerDestination = createDestination(session, ActiveMQDestination.TOPIC_TYPE, getConsumerSubject());
         producerDestination = createDestination(session, ActiveMQDestination.TOPIC_TYPE, getProducerSubject());
      } else {
         consumerDestination = createDestination(session, ActiveMQDestination.QUEUE_TYPE, getConsumerSubject());
         producerDestination = createDestination(session, ActiveMQDestination.QUEUE_TYPE, getConsumerSubject());
      }

      System.out.println("Created  consumer destination: " + consumerDestination + " of type: " + consumerDestination.getClass());
      System.out.println("Created  producer destination: " + producerDestination + " of type: " + producerDestination.getClass());
      consumer = createConsumer();
      consumer.setMessageListener(this);
      connection.start();

      // log.info("Created connection: " + connection);
   }

   protected String getConsumerSubject() {
      return null;
   }

   protected String getProducerSubject() {
      return null;
   }

   protected MessageConsumer createConsumer() throws JMSException {
      if (durable) {
         System.out.println("Creating durable consumer");
         return session.createDurableSubscriber((Topic) consumerDestination, getName());
      }
      return session.createConsumer(consumerDestination);
   }

   protected Session createConsumerSession() throws JMSException {
      if (useSeparateSession) {
         return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      } else {
         return session;
      }
   }

}
