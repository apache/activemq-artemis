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

package org.apache.activemq.artemis.tests.smoke.console;

import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.messages.Consumer;
import org.apache.activemq.artemis.cli.commands.messages.Producer;
import org.apache.activemq.artemis.tests.smoke.console.pages.LoginPage;
import org.apache.activemq.artemis.tests.smoke.console.pages.MessagePage;
import org.apache.activemq.artemis.tests.smoke.console.pages.QueuePage;
import org.apache.activemq.artemis.tests.smoke.console.pages.QueuesPage;
import org.apache.activemq.artemis.tests.smoke.console.pages.StatusPage;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openqa.selenium.MutableCapabilities;

@RunWith(Parameterized.class)
public class QueuesTest extends ConsoleTest {

   public QueuesTest(MutableCapabilities browserOptions) {
      super(browserOptions);
   }

   @Test
   public void testDefaultQueues() throws Exception {
      LoginPage loginPage = new LoginPage(driver);
      StatusPage statusPage = loginPage.loginValidUser(
         SERVER_ADMIN_USERNAME, SERVER_ADMIN_PASSWORD, DEFAULT_TIMEOUT);
      QueuesPage queuesPage = statusPage.getQueuesPage(DEFAULT_TIMEOUT);

      Wait.assertEquals(1, () -> queuesPage.countQueue("DLQ"));
      assertEquals(0, queuesPage.getMessagesCount("DLQ"));

      Wait.assertEquals(1, () -> queuesPage.countQueue("ExpiryQueue"));
      assertEquals(0, queuesPage.getMessagesCount("ExpiryQueue"));
   }

   @Test
   public void testAutoCreatedQueue() throws Exception {
      final int messages = 1;
      final String queueName = "TEST";
      final String messageText = "TEST";

      LoginPage loginPage = new LoginPage(driver);
      StatusPage statusPage = loginPage.loginValidUser(
         SERVER_ADMIN_USERNAME, SERVER_ADMIN_PASSWORD, DEFAULT_TIMEOUT);
      QueuesPage beforeQueuesPage = statusPage.getQueuesPage(DEFAULT_TIMEOUT);
      Wait.assertEquals(1, () -> beforeQueuesPage.countQueue("DLQ"));
      Wait.assertEquals(0, () -> beforeQueuesPage.countQueue(queueName));

      Producer producer = new Producer();
      producer.setUser(SERVER_ADMIN_USERNAME);
      producer.setPassword(SERVER_ADMIN_PASSWORD);
      producer.setDestination(queueName);
      producer.setMessageCount(messages);
      producer.setMessage(messageText);
      producer.setSilentInput(true);
      producer.execute(new ActionContext());

      beforeQueuesPage.refresh(DEFAULT_TIMEOUT);
      Wait.assertEquals(1, () -> beforeQueuesPage.countQueue("DLQ"));
      Wait.assertEquals(1, () -> beforeQueuesPage.countQueue(queueName));
      assertEquals(messages, beforeQueuesPage.getMessagesCount(queueName));

      QueuePage queuePage = beforeQueuesPage.getQueuePage(queueName, DEFAULT_TIMEOUT);
      MessagePage messagePage = queuePage.getMessagePage(0, DEFAULT_TIMEOUT);
      assertEquals(messageText, messagePage.getMessageText());

      Consumer consumer = new Consumer();
      consumer.setUser(SERVER_ADMIN_USERNAME);
      consumer.setPassword(SERVER_ADMIN_PASSWORD);
      consumer.setDestination(queueName);
      consumer.setMessageCount(messages);
      consumer.setSilentInput(true);
      consumer.setReceiveTimeout(2000);
      consumer.setBreakOnNull(true);
      int consumed = (int)consumer.execute(new ActionContext());

      assertEquals(messages, consumed);

      QueuesPage afterQueuesPage = messagePage.getQueuesPage(DEFAULT_TIMEOUT);
      Wait.assertEquals(1, () -> afterQueuesPage.countQueue("DLQ"));
      Wait.assertEquals(0, () -> afterQueuesPage.countQueue(queueName));
   }
}
