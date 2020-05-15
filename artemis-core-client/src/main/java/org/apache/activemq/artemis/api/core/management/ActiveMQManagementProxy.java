/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.api.core.management;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientRequestor;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;

public class ActiveMQManagementProxy implements AutoCloseable {

   private final String username;
   private final String password;
   private final ServerLocator locator;

   private ClientSessionFactory sessionFactory;
   private ClientSession session;
   private ClientRequestor requestor;

   public ActiveMQManagementProxy(final ServerLocator locator, final String username, final String password) {
      this.locator = locator;
      this.username = username;
      this.password = password;
   }

   public void start() throws Exception {
      sessionFactory = locator.createSessionFactory();
      session = sessionFactory.createSession(username, password, false, true, true, false, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE);
      requestor = new ClientRequestor(session, ActiveMQDefaultConfiguration.getDefaultManagementAddress());

      session.start();
   }

   public <T> T invokeOperation(final Class<T> type, final String resourceName, final String operationName, final Object... operationArgs) throws Exception {
      ClientMessage request = session.createMessage(false);

      ManagementHelper.putOperationInvocation(request, resourceName, operationName, operationArgs);

      ClientMessage reply = requestor.request(request);

      if (ManagementHelper.hasOperationSucceeded(reply)) {
         return (T)ManagementHelper.getResult(reply, type);
      } else {
         throw new Exception("Failed to invoke " + resourceName + "." + operationName + ". Reason: " + ManagementHelper.getResult(reply, String.class));
      }
   }


   public void stop() throws ActiveMQException {
      session.stop();
   }

   @Override
   public void close() throws Exception {
      requestor.close();
      session.close();
      sessionFactory.close();
   }
}
