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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.cli.commands.ActionContext;

import javax.jms.Message;

@Command(name = "delete", description = "delete a queue or topic")
public class DeleteDestination extends DestinationAction {

   @Option(name = "--removeConsumers", description = "whether deleting destination with consumers or not (default false)")
   boolean removeConsumers = false;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      if (JMS_QUEUE.equals(destType)) {
         deleteJmsQueue(context);
      }
      else if (CORE_QUEUE.equals(destType)) {
         deleteCoreQueue(context);
      }
      else if (JMS_TOPIC.equals(destType)) {
         deleteJmsTopic(context);
      }
      else {
         throw new IllegalArgumentException("--type can only be one of " + JMS_QUEUE + ", " + JMS_TOPIC + " and " + CORE_QUEUE);
      }
      return null;
   }

   private void deleteJmsTopic(final ActionContext context) throws Exception {
      performJmsManagement(brokerURL, user, password, new ManagementCallback<Message>() {
         @Override
         public void setUpInvocation(Message message) throws Exception {
            JMSManagementHelper.putOperationInvocation(message, "jms.server", "destroyTopic", getName(), removeConsumers);
         }

         @Override
         public void requestSuccessful(Message reply) throws Exception {
            boolean result = (boolean) JMSManagementHelper.getResult(reply, Boolean.class);
            if (result) {
               context.out.println("Topic " + getName() + " deleted successfully.");
            }
            else {
               context.err.println("Failed to delete topic " + getName());
            }
         }

         @Override
         public void requestFailed(Message reply) throws Exception {
            String errorMsg = (String) JMSManagementHelper.getResult(reply, String.class);
            context.err.println("Failed to delete topic " + getName() + ". Reason: " + errorMsg);
         }
      });
   }

   private void deleteJmsQueue(final ActionContext context) throws Exception {
      performJmsManagement(brokerURL, user, password, new ManagementCallback<Message>() {
         @Override
         public void setUpInvocation(Message message) throws Exception {
            JMSManagementHelper.putOperationInvocation(message, "jms.server", "destroyQueue", getName(), removeConsumers);
         }

         @Override
         public void requestSuccessful(Message reply) throws Exception {
            boolean result = (boolean) JMSManagementHelper.getResult(reply, Boolean.class);
            if (result) {
               context.out.println("Jms queue " + getName() + " deleted successfully.");
            }
            else {
               context.err.println("Failed to delete queue " + getName());
            }
         }

         @Override
         public void requestFailed(Message reply) throws Exception {
            String errorMsg = (String) JMSManagementHelper.getResult(reply, String.class);
            context.err.println("Failed to create " + getName() + " with reason: " + errorMsg);
         }
      });
   }

   private void deleteCoreQueue(final ActionContext context) throws Exception {
      performCoreManagement(brokerURL, user, password, new ManagementCallback<ClientMessage>() {
         @Override
         public void setUpInvocation(ClientMessage message) throws Exception {
            ManagementHelper.putOperationInvocation(message, "core.server", "destroyQueue", getName());
         }

         @Override
         public void requestSuccessful(ClientMessage reply) throws Exception {
            context.out.println("Queue " + getName() + " deleted successfully.");
         }

         @Override
         public void requestFailed(ClientMessage reply) throws Exception {
            String errMsg = (String) ManagementHelper.getResult(reply, String.class);
            context.err.println("Failed to delete queue " + getName() + ". Reason: " + errMsg);
         }
      });
   }

}
