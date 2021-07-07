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
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientRequestor;
import org.apache.activemq.artemis.api.core.client.ClientSession;

public class ActiveMQManagementProxy implements AutoCloseable {

   private final ClientSession session;

   public ActiveMQManagementProxy(final ClientSession session) {
      this.session = session;
   }

   public Object getAttribute(final String resourceName, final String attributeName, final int timeout) throws Exception {
      try (ClientRequestor requestor = new ClientRequestor(session, ActiveMQDefaultConfiguration.getDefaultManagementAddress())) {
         ClientMessage request = session.createMessage(false);

         ManagementHelper.putAttribute(request, resourceName, attributeName);

         ClientMessage reply = requestor.request(request, timeout);

         if (ManagementHelper.hasOperationSucceeded(reply)) {
            return ManagementHelper.getResult(reply);
         } else {
            throw new Exception("Failed to get " + resourceName + "." + attributeName + ". Reason: " + ManagementHelper.getResult(reply, String.class));
         }
      }
   }

   public Object invokeOperation(final String resourceName, final String operationName, final Object[] operationParams, final int timeout) throws Exception {
      try (ClientRequestor requestor = new ClientRequestor(session, ActiveMQDefaultConfiguration.getDefaultManagementAddress())) {
         ClientMessage request = session.createMessage(false);

         ManagementHelper.putOperationInvocation(request, resourceName, operationName, operationParams);

         ClientMessage reply = requestor.request(request, timeout);

         if (ManagementHelper.hasOperationSucceeded(reply)) {
            return ManagementHelper.getResult(reply);
         } else {
            throw new Exception("Failed to invoke " + resourceName + "." + operationName + ". Reason: " + ManagementHelper.getResult(reply, String.class));
         }
      }
   }

   @Override
   public void close() throws Exception {
      session.close();
   }
}
