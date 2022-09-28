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

package org.apache.activemq.artemis.tests.unit.logging;

import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.protocol.amqp.broker.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolLogger;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This will validate the AssertionLoggerHandler is working as expected.
 * Even though the class belongs to artemis-unit-test-support, this test has to be done here as we
 * are validating the log4j2-tests-config.properties files and the classloading of everything.
 */
public class AssertionLoggerTest {

   @Before
   public void prepare() {
      AssertionLoggerHandler.startCapture(true);
   }

   @After
   public void cleanup() {
      AssertionLoggerHandler.stopCapture();
   }

   @Test
   public void testHandlingOnAMQP() throws Exception {
      validateLogging(ActiveMQProtonRemotingConnection.class);
   }

   @Test
   public void testHandlingOnClientCore() throws Exception {
      validateLogging(ServerLocatorImpl.class);
   }

   @Test
   public void testInfoAMQP() throws Exception {
      ActiveMQAMQPProtocolLogger.LOGGER.retryConnection("test", "test", 1, 1);
      Assert.assertTrue(AssertionLoggerHandler.findText("AMQ111002"));
   }

   private void validateLogging(Class<?> clazz) {
      String randomLogging = RandomUtil.randomString();
      Logger logging = LoggerFactory.getLogger(clazz);
      logging.warn(randomLogging);
      Assert.assertTrue(AssertionLoggerHandler.findText(randomLogging));

      AssertionLoggerHandler.clear();

      for (int i = 0; i < 10; i++) {
         logging.warn(randomLogging);
      }
      Assert.assertEquals(10, AssertionLoggerHandler.countText(randomLogging));

      AssertionLoggerHandler.clear();

      for (int i = 0; i < 10; i++) {
         logging.info(randomLogging);
      }

      Assert.assertEquals(10, AssertionLoggerHandler.countText(randomLogging));
   }
}
