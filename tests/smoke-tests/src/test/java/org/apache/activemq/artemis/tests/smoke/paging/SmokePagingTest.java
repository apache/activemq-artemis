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

package org.apache.activemq.artemis.tests.smoke.paging;

import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.messages.Consumer;
import org.apache.activemq.artemis.cli.commands.messages.Producer;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SmokePagingTest extends SmokeTestBase {

   public static final String SERVER_NAME_0 = "paging";

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      startServer(SERVER_NAME_0, 0, 30000);
   }


   @Test
   public void testAMQPOnCLI() throws Exception {

      String protocol = "amqp";
      int NUMBER_OF_MESSAGES = 5000;

      internalReceive(protocol, NUMBER_OF_MESSAGES);

   }

   @Test
   public void testCoreOnCLI() throws Exception {

      String protocol = "core";
      int NUMBER_OF_MESSAGES = 5000;

      internalReceive(protocol, NUMBER_OF_MESSAGES);

   }

   private void internalReceive(String protocol, int NUMBER_OF_MESSAGES) throws Exception {
      Producer producer = (Producer)new Producer().setMessageSize(1000).setMessageCount(NUMBER_OF_MESSAGES).setTxBatchSize(1000);
      producer.setProtocol(protocol);
      producer.setSilentInput(true);
      producer.execute(new ActionContext());

      Consumer consumer = new Consumer();
      consumer.setMessageCount(NUMBER_OF_MESSAGES);
      consumer.setProtocol(protocol);
      consumer.setSilentInput(true);
      consumer.setReceiveTimeout(2000);
      consumer.setBreakOnNull(true);
      int consumed = (int)consumer.execute(new ActionContext());

      Assert.assertEquals(NUMBER_OF_MESSAGES, consumed);
   }

}
