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
package org.apache.activemq.artemis.core.server;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler.LogLevel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ServerLogBundlesTest {

   private static final String SERVER_LOGGER = ActiveMQServerLogger.class.getPackage().getName();
   private static final String QUEUE_LOGGER =  Queue.class.getName();
   private static LogLevel origServerLoggerLevel;
   private static LogLevel origQueueLoggerLevel;

   @BeforeAll
   public static void setLogLevel() {
      origServerLoggerLevel = AssertionLoggerHandler.setLevel(SERVER_LOGGER, LogLevel.INFO);
      origQueueLoggerLevel = AssertionLoggerHandler.setLevel(QUEUE_LOGGER, LogLevel.INFO);
   }

   @AfterAll
   public static void restoreLogLevel() throws Exception {
      AssertionLoggerHandler.setLevel(SERVER_LOGGER, origServerLoggerLevel);
      AssertionLoggerHandler.setLevel(QUEUE_LOGGER, origQueueLoggerLevel);
   }

   @Test
   public void testActiveMQServerLogger() throws Exception {
      try (AssertionLoggerHandler loggerHandler = new AssertionLoggerHandler(true)) {
         ActiveMQServerLogger.LOGGER.autoRemoveAddress("addressBreadCrumb");

         assertTrue(loggerHandler.findText("AMQ224113", "addressBreadCrumb"));
      }
   }

   @Test
   public void testActiveMQMessageBundle() throws Exception {
      ActiveMQIllegalStateException e = ActiveMQMessageBundle.BUNDLE.bindingAlreadyExists("nameBreadCrumb", "bindingBreadCrumb");

      String message = e.getMessage();
      assertNotNull(message);
      assertTrue(message.startsWith("AMQ229235"), "unexpected message: " + message);
      assertTrue(message.contains("nameBreadCrumb"), "unexpected message: " + message);
      assertTrue(message.contains("bindingBreadCrumb"), "unexpected message: " + message);
   }

   @Test
   public void testActiveMQQueueLogger() throws Exception {
      int messageCount = 1003;
      int messageBytes = 70004;
      int maxMessages = 1000;
      int maxMessagesBytes = 60001;

      try (AssertionLoggerHandler loggerHandler = new AssertionLoggerHandler(true)) {
         ActiveMQQueueLogger.LOGGER.warnPageFlowControl("addressBreadCrumb", "queueBreadCrumb", messageCount, messageBytes, maxMessages, maxMessagesBytes);

         assertTrue(loggerHandler.findText("AMQ224127", "addressBreadCrumb", "queueBreadCrumb", String.valueOf(messageCount), String.valueOf(messageBytes), String.valueOf(maxMessages), String.valueOf(maxMessagesBytes)));
      }
   }
}
