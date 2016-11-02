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

package org.apache.activemq.artemis.cli.commands.destination;

import javax.jms.Message;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.cli.commands.ActionContext;

@Command(name = "create", description = "create a queue or topic")
public class CreateDestination extends DestinationAction {

   @Option(name = "--filter", description = "queue's filter string (default null)")
   String filter = null;

   @Option(name = "--address", description = "address of the core queue (default queue's name)")
   String address;

   @Option(name = "--durable", description = "whether the queue is durable or not (default false)")
   boolean durable = false;

   @Option(name = "--bindings", description = "comma separated jndi binding names (default null)")
   String bindings = null;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      if (JMS_QUEUE.equals(destType)) {
         createJmsQueue(context);
      } else if (CORE_QUEUE.equals(destType)) {
         createCoreQueue(context);
      } else if (JMS_TOPIC.equals(destType)) {
         createJmsTopic(context);
      } else {
         throw new IllegalArgumentException("--type can only be one of " + JMS_QUEUE + ", " + JMS_TOPIC + " and " + CORE_QUEUE);
      }
      return null;
   }

   private void createJmsTopic(final ActionContext context) throws Exception {
      performJmsManagement(new ManagementCallback<Message>() {
         @Override
         public void setUpInvocation(Message message) throws Exception {
            JMSManagementHelper.putOperationInvocation(message, "jms.server", "createTopic", getName(), bindings);
         }

         @Override
         public void requestSuccessful(Message reply) throws Exception {
            boolean result = (boolean) JMSManagementHelper.getResult(reply, Boolean.class);
            if (result) {
               context.out.println("Topic " + getName() + " created successfully.");
            } else {
               context.err.println("Failed to create topic " + getName() + ".");
            }
         }

         @Override
         public void requestFailed(Message reply) throws Exception {
            String errorMsg = (String) JMSManagementHelper.getResult(reply, String.class);
            context.err.println("Failed to create topic " + getName() + ". Reason: " + errorMsg);
         }
      });
   }

   public String getAddress() {
      if (address == null || "".equals(address.trim())) {
         address = getName();
      }
      return address.trim();
   }

   private void createCoreQueue(final ActionContext context) throws Exception {
      performCoreManagement(new ManagementCallback<ClientMessage>() {
         @Override
         public void setUpInvocation(ClientMessage message) throws Exception {
            String address = getAddress();
            ManagementHelper.putOperationInvocation(message, "core.server", "createQueue", address, getName(), filter, durable);
         }

         @Override
         public void requestSuccessful(ClientMessage reply) throws Exception {
            context.out.println("Core queue " + getName() + " created successfully.");
         }

         @Override
         public void requestFailed(ClientMessage reply) throws Exception {
            String errMsg = (String) ManagementHelper.getResult(reply, String.class);
            context.err.println("Failed to create queue " + getName() + ". Reason: " + errMsg);
         }
      });
   }

   private void createJmsQueue(final ActionContext context) throws Exception {

      performJmsManagement(new ManagementCallback<Message>() {

         @Override
         public void setUpInvocation(Message message) throws Exception {
            JMSManagementHelper.putOperationInvocation(message, "jms.server", "createQueue", getName(), bindings, filter, durable);
         }

         @Override
         public void requestSuccessful(Message reply) throws Exception {
            boolean result = (boolean) JMSManagementHelper.getResult(reply, Boolean.class);
            if (result) {
               context.out.println("Jms queue " + getName() + " created successfully.");
            } else {
               context.err.println("Failed to create jms queue " + getName() + ".");
            }
         }

         @Override
         public void requestFailed(Message reply) throws Exception {
            String errorMsg = (String) JMSManagementHelper.getResult(reply, String.class);
            context.err.println("Failed to create jms queue " + getName() + ". Reason: " + errorMsg);
         }
      });
   }

   public void setFilter(String filter) {
      this.filter = filter;
   }

   public void setBindings(String bindings) {
      this.bindings = bindings;
   }
}
