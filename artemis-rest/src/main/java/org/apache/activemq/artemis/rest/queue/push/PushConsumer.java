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
package org.apache.activemq.artemis.rest.queue.push;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;
import org.apache.activemq.artemis.utils.SelectorTranslator;

public class PushConsumer {

   protected PushRegistration registration;
   protected ClientSessionFactory factory;
   protected List<ClientSession> sessions;
   protected List<ClientConsumer> consumers;
   protected String destination;
   protected String id;
   protected PushStrategy strategy;
   protected PushStore store;

   private ConnectionFactoryOptions jmsOptions;

   public PushConsumer(ClientSessionFactory factory,
                       String destination,
                       String id,
                       PushRegistration registration,
                       PushStore store,
                       ConnectionFactoryOptions jmsOptions) {
      this.factory = factory;
      this.destination = destination;
      this.id = id;
      this.registration = registration;
      this.store = store;
      this.jmsOptions = jmsOptions;
   }

   public PushStrategy getStrategy() {
      return strategy;
   }

   public PushRegistration getRegistration() {
      return registration;
   }

   public String getDestination() {
      return destination;
   }

   public void start() throws Exception {
      if (registration.getTarget().getClassName() != null) {
         Class clazz = Thread.currentThread().getContextClassLoader().loadClass(registration.getTarget().getClassName());
         strategy = (PushStrategy) clazz.newInstance();
      } else if (registration.getTarget().getRelationship() != null) {
         if (registration.getTarget().getRelationship().equals("destination")) {
            strategy = new ActiveMQPushStrategy();
         } else if (registration.getTarget().getRelationship().equals("template")) {
            strategy = new UriTemplateStrategy();
         }
      }
      if (strategy == null) {
         strategy = new UriStrategy();
      }
      strategy.setRegistration(registration);
      strategy.setJmsOptions(jmsOptions);
      strategy.start();

      sessions = new ArrayList<>();
      consumers = new ArrayList<>();

      for (int i = 0; i < registration.getSessionCount(); i++) {
         ClientSession session = factory.createSession(false, false, 0);

         ClientConsumer consumer;

         if (registration.getSelector() != null) {
            consumer = session.createConsumer(destination, SelectorTranslator.convertToActiveMQFilterString(registration.getSelector()));
         } else {
            consumer = session.createConsumer(destination);
         }
         consumer.setMessageHandler(new PushConsumerMessageHandler(this, session));
         session.start();
         ActiveMQRestLogger.LOGGER.startingPushConsumer(registration.getTarget());

         consumers.add(consumer);
         sessions.add(session);
      }
   }

   public void stop() {
      for (ClientSession session : sessions) {
         try {
            if (session != null) {
               session.close();
            }
         } catch (ActiveMQException e) {

         }
      }

      try {
         if (strategy != null) {
            strategy.stop();
         }
      } catch (Exception e) {
      }
   }

   public void disableFromFailure() {
      registration.setEnabled(false);
      try {
         if (registration.isDurable()) {
            store.update(registration);
         }
      } catch (Exception e) {
         ActiveMQRestLogger.LOGGER.errorUpdatingStore(e);
      }
      stop();
   }
}
