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
package org.apache.activemq.tests.integration.management;

import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientRequestor;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.management.ManagementHelper;

public class CoreMessagingProxy
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String resourceName;

   private final ClientSession session;

   private final ClientRequestor requestor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CoreMessagingProxy(final ClientSession session, final String resourceName) throws Exception
   {
      this.session = session;

      this.resourceName = resourceName;

      requestor = new ClientRequestor(session, ActiveMQDefaultConfiguration.getDefaultManagementAddress());

   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   public Object retrieveAttributeValue(final String attributeName)
   {
      return retrieveAttributeValue(attributeName, null);
   }

   public Object retrieveAttributeValue(final String attributeName, final Class desiredType)
   {
      ClientMessage m = session.createMessage(false);
      ManagementHelper.putAttribute(m, resourceName, attributeName);
      ClientMessage reply;
      try
      {
         reply = requestor.request(m);
         Object result = ManagementHelper.getResult(reply);

         if (desiredType != null && desiredType != result.getClass())
         {
            // Conversions
            if (desiredType == Long.class && result.getClass() == Integer.class)
            {
               Integer in = (Integer)result;

               result = new Long(in.intValue());
            }
         }

         return result;
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public Object invokeOperation(final String operationName, final Object... args) throws Exception
   {
      ClientMessage m = session.createMessage(false);
      ManagementHelper.putOperationInvocation(m, resourceName, operationName, args);
      ClientMessage reply = requestor.request(m);
      if (ManagementHelper.hasOperationSucceeded(reply))
      {
         return ManagementHelper.getResult(reply);
      }
      else
      {
         throw new Exception((String)ManagementHelper.getResult(reply));
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
