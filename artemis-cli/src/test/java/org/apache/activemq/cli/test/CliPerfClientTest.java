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
package org.apache.activemq.cli.test;

import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.messages.perf.PerfClientCommand;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;

public class CliPerfClientTest extends CliTestBase {
   private Connection connection;
   private ActiveMQConnectionFactory cf;

   @Before
   @Override
   public void setup() throws Exception {
      setupAuth();
      super.setup();
      startServer();
      cf = getConnectionFactory(61616);
      connection = cf.createConnection("admin", "admin");
   }

   @After
   @Override
   public void tearDown() throws Exception {
      closeConnection(cf, connection);
      super.tearDown();
   }

   private void start(boolean durable) throws Exception {
      PerfClientCommand command = new PerfClientCommand() {
         @Override
         public Object execute(ActionContext context) throws Exception {
            clientID = "perfClientTest";
            durableSubscription = durable;
            messageCount = 1;
            return super.execute(context);
         }
      };
      command.setUser("admin").setPassword("admin").execute(new TestActionContext());
   }

   @Test
   public void testNonDurableStarts() throws Exception {
      start(false);
   }

   @Test
   public void testDurableStarts() throws Exception {
      start(true);
   }
}