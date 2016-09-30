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
package org.apache.activemq.artemis.tests.integration.cli;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.destination.CreateDestination;
import org.apache.activemq.artemis.cli.commands.destination.DeleteDestination;
import org.apache.activemq.artemis.cli.commands.destination.DestinationAction;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

public class DestinationCommandTest extends JMSTestBase {

   //the command
   private ByteArrayOutputStream output;
   private ByteArrayOutputStream error;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      this.output = new ByteArrayOutputStream(1024);
      this.error = new ByteArrayOutputStream(1024);
   }

   @Test
   public void testCreateJmsQueue() throws Exception {
      CreateDestination command = new CreateDestination();
      command.setName("jmsQueue1");
      command.setBindings("jmsQueue1Binding");
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionResult(command);
   }

   @Test
   public void testDeleteJmsQueue() throws Exception {
      CreateDestination command = new CreateDestination();
      command.setName("jmsQueue1");
      command.setBindings("jmsQueue1Binding");
      command.execute(new ActionContext());

      DeleteDestination delete = new DeleteDestination();
      delete.setName("jmsQueue1");
      delete.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionResult(delete);
   }

   @Test
   public void testDeleteNonExistJmsQueue() throws Exception {
      DeleteDestination delete = new DeleteDestination();
      delete.setName("jmsQueue1NotExist");
      delete.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionResult(delete);
   }

   @Test
   public void testCreateJmsQueueWithFilter() throws Exception {
      CreateDestination command = new CreateDestination();
      command.setName("jmsQueue2");
      command.setBindings("jmsQueue2Binding");
      command.setFilter("color='red'");
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionResult(command);
      assertTrue(checkBindingExists(command, "color='red'"));
   }

   @Test
   public void testCreateJmsTopic() throws Exception {
      CreateDestination command = new CreateDestination();
      command.setDestType(DestinationAction.JMS_TOPIC);
      command.setName("jmsTopic1");
      command.setBindings("jmsTopic1Binding");
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionResult(command);
   }

   @Test
   public void testDeleteJmsTopic() throws Exception {
      CreateDestination command = new CreateDestination();
      command.setDestType(DestinationAction.JMS_TOPIC);
      command.setName("jmsTopic1");
      command.setBindings("jmsTopic1Binding");
      command.execute(new ActionContext());

      DeleteDestination delete = new DeleteDestination();
      delete.setDestType(DestinationAction.JMS_TOPIC);
      delete.setName("jmsTopic1");
      delete.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionResult(delete);
   }

   @Test
   public void testDeleteJmsTopicNotExist() throws Exception {
      DeleteDestination delete = new DeleteDestination();
      delete.setDestType(DestinationAction.JMS_TOPIC);
      delete.setName("jmsTopic1NotExist");
      delete.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionResult(delete);
   }

   @Test
   public void testCreateCoreQueue() throws Exception {
      CreateDestination command = new CreateDestination();
      command.setDestType(DestinationAction.CORE_QUEUE);
      command.setName("coreQueue1");
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionResult(command);
   }

   @Test
   public void testCreateCoreQueueWithFilter() throws Exception {
      CreateDestination command = new CreateDestination();
      command.setName("coreQueue2");
      command.setDestType(DestinationAction.CORE_QUEUE);
      command.setFilter("color='green'");
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionResult(command);
   }

   @Test
   public void testDeleteCoreQueue() throws Exception {
      CreateDestination command = new CreateDestination();
      command.setName("coreQueue2");
      command.setDestType(DestinationAction.CORE_QUEUE);
      command.setFilter("color='green'");
      command.execute(new ActionContext());

      DeleteDestination delete = new DeleteDestination();
      delete.setName("coreQueue2");
      delete.setDestType(DestinationAction.CORE_QUEUE);
      delete.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionResult(delete);
   }

   @Test
   public void testDeleteCoreQueueNotExist() throws Exception {
      DeleteDestination delete = new DeleteDestination();
      delete.setName("coreQueue2NotExist");
      delete.setDestType(DestinationAction.CORE_QUEUE);
      delete.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionResult(delete);
   }

   private boolean isCreateCommand(DestinationAction command) {
      return command instanceof CreateDestination;
   }

   private boolean isJms(DestinationAction command) {
      String destType = command.getDestType();
      return !DestinationAction.CORE_QUEUE.equals(destType);
   }

   private boolean isTopic(DestinationAction command) {
      String destType = command.getDestType();
      return DestinationAction.JMS_TOPIC.equals(destType);
   }

   private void checkExecutionResult(DestinationAction command) throws Exception {
      if (isCreateCommand(command)) {
         String fullMessage = output.toString();
         System.out.println("output: " + fullMessage);
         assertTrue(fullMessage, fullMessage.contains("successfully"));
         assertTrue(checkBindingExists(command, null));
      } else {
         if (command.getName().equals("jmsQueue1") || command.getName().equals("coreQueue2") || command.getName().equals("jmsTopic1")) {
            String fullMessage = output.toString();
            System.out.println("output: " + fullMessage);
            assertTrue(fullMessage, fullMessage.contains("successfully"));
            assertFalse(checkBindingExists(command, null));
         } else {
            String errorMessage = error.toString();
            System.out.println("error: " + errorMessage);
            assertTrue(errorMessage, errorMessage.contains("Failed to"));
            assertFalse(checkBindingExists(command, null));
         }
      }
   }

   private boolean checkBindingExists(DestinationAction command, String filter) {
      String bindingKey = command.getName();
      if (isJms(command)) {
         if (isTopic(command)) {
            bindingKey = "jms.topic." + bindingKey;
         } else {
            bindingKey = "jms.queue." + bindingKey;
         }
      }
      Map<SimpleString, Binding> bindings = server.getPostOffice().getAllBindings();
      System.out.println("bindings: " + bindings);
      Binding binding = bindings.get(new SimpleString(bindingKey));
      System.out.println("got binding: " + binding);
      if (binding == null) {
         System.out.println("No bindings for " + bindingKey);
         return false;
      }
      if (filter != null) {
         Filter bindingFilter = binding.getFilter();
         assertNotNull(bindingFilter);
         assertEquals(filter, bindingFilter.getFilterString().toString());
      }
      return true;
   }

}
