/**
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
package org.apache.activemq.tests.integration.jms.server.management;

import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.activemq.api.jms.management.JMSManagementHelper;

public class JMSMessagingProxy
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String resourceName;

   private final Session session;

   private final QueueRequestor requestor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSMessagingProxy(final QueueSession session, final Queue managementQueue, final String resourceName) throws Exception
   {
      this.session = session;

      this.resourceName = resourceName;

      requestor = new QueueRequestor(session, managementQueue);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   public Object retrieveAttributeValue(final String attributeName)
   {
      try
      {
         Message m = session.createMessage();
         JMSManagementHelper.putAttribute(m, resourceName, attributeName);
         Message reply = requestor.request(m);
         return JMSManagementHelper.getResult(reply);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public Object invokeOperation(final String operationName, final Object... args) throws Exception
   {
      Message m = session.createMessage();
      JMSManagementHelper.putOperationInvocation(m, resourceName, operationName, args);
      Message reply = requestor.request(m);
      if (JMSManagementHelper.hasOperationSucceeded(reply))
      {
         return JMSManagementHelper.getResult(reply);
      }
      else
      {
         throw new Exception((String)JMSManagementHelper.getResult(reply));
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
