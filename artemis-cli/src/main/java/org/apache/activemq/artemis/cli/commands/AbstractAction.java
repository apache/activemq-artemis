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
package org.apache.activemq.artemis.cli.commands;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientRequestor;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.cli.commands.messages.ConnectionAbstract;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public abstract class AbstractAction extends ConnectionAbstract {

   public void performCoreManagement(ManagementCallback<ClientMessage> cb) throws Exception {

      try (ActiveMQConnectionFactory factory = createConnectionFactory();
         ServerLocator locator = factory.getServerLocator();
           ClientSessionFactory sessionFactory = locator.createSessionFactory();
           ClientSession session = sessionFactory.createSession(user, password, false, true, true, false, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE)) {
         session.start();
         ClientRequestor requestor = new ClientRequestor(session, "activemq.management");
         ClientMessage message = session.createMessage(false);

         cb.setUpInvocation(message);

         ClientMessage reply = requestor.request(message);

         if (ManagementHelper.hasOperationSucceeded(reply)) {
            cb.requestSuccessful(reply);
         } else {
            cb.requestFailed(reply);
         }
      }
   }

   public interface ManagementCallback<T> {

      void setUpInvocation(T message) throws Exception;

      void requestSuccessful(T reply) throws Exception;

      void requestFailed(T reply) throws Exception;
   }
}
