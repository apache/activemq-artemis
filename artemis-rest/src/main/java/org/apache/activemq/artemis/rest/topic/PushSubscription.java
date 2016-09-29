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
package org.apache.activemq.artemis.rest.topic;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.push.PushConsumer;
import org.apache.activemq.artemis.rest.queue.push.PushStore;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;

public class PushSubscription extends PushConsumer {

   public PushSubscription(ClientSessionFactory factory,
                           String destination,
                           String id,
                           PushRegistration registration,
                           PushStore store,
                           ConnectionFactoryOptions jmsOptions) {
      super(factory, destination, id, registration, store, jmsOptions);
   }

   @Override
   public void disableFromFailure() {
      super.disableFromFailure();
      if (registration.isDurable())
         deleteSubscriberQueue();
   }

   protected void deleteSubscriberQueue() {
      String subscriptionName = registration.getDestination();
      ClientSession session = null;
      try {
         session = factory.createSession();

         session.deleteQueue(subscriptionName);
      } catch (ActiveMQException e) {
         ActiveMQRestLogger.LOGGER.errorDeletingSubscriberQueue(e);
      } finally {
         try {
            if (session != null)
               session.close();
         } catch (ActiveMQException e) {
         }
      }
   }
}
