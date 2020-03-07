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

package org.apache.activemq.artemis.tests.extras.byteman;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.plugin.impl.LoggingActiveMQServerPlugin;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * NOTE: this test should be run at log level INFO
 *
 * This test checks the LoggingActiveMQServerPlugin is logging expected data with specific property configurations
 * when client using AMQP
 */
@RunWith(BMUnitRunner.class)
public class LoggingActiveMQServerPluginAMQPTest extends LoggingActiveMQServerPluginTest {

   /**
    * Aim: test all events are logged when plugin configured with
    * LOG_ALL_EVENTS
    *
    * Overridden as behaviour slightly different for AMQP - consumer closed plugin point called twice
    *
    * @throws Exception
    */
   @Override
   @Test
   @BMRules(rules = {@BMRule(name = "test logAll EVENT",
      targetClass = "org.jboss.logging.Logger",
      targetMethod = "logv",
      targetLocation = "ENTRY",
      action = "org.apache.activemq.artemis.tests.extras.byteman.LoggingActiveMQServerPluginTest.infoLog($2, $4, $0)")})
   public void testLogAll() throws Exception {

      //initial plugin
      ActiveMQServer activeMQServer = createServerWithLoggingPlugin(LoggingActiveMQServerPlugin.LOG_ALL_EVENTS);
      activeMQServer.start();

      try {

         sendAndReceive(true, true, "log ALL Message_1", 0);
         sendAndReceive(true, true, "log ALL Message_2", 0);

         Thread.sleep(500);

         assertEquals("created connections", 2, createdConnectionLogs.size());
         assertEquals("destroyed connections", 2, destroyedConnectionLogs.size());
         assertEquals("created consumer", 2, createdConsumerLogs.size());
         assertEquals("closed consumer", 4, closedConsumerLogs.size());
         assertEquals("delivered message", 2, deliveredLogs.size());
         assertEquals("acked message", 2, ackedLogs.size());
         assertEquals("sending message", 2, sentLogs.size());
         assertEquals("routing message", 2, routedLogs.size());
         assertEquals("queue created", 2, createdQueueLogs.size());
         assertEquals("queue destroyed", 2, destroyedQueueLogs.size());
         assertEquals("expired message", 0, messageExpiredLogs.size());

      } finally {
         activeMQServer.stop();
         //reset the logs lists
         clearLogLists();
      }
   }

   /**
    * Aim: test the consumer create/close events are logged when plugin configured with
    * LOG_CONSUMER_EVENTS
    *
    * Overridden as behaviour slightly different for AMQP - consumer closed plugin point seems to be called twice
    * @throws Exception
    */
   @Override
   @Test
   @BMRules(rules = {@BMRule(name = "test LOG_CONSUMER_EVENTS",
      targetClass = "org.jboss.logging.Logger",
      targetMethod = "logv",
      targetLocation = "ENTRY",
      action = "org.apache.activemq.artemis.tests.extras.byteman.LoggingActiveMQServerPluginTest.infoLog($2, $4, $0)")})
   public void testLogConsumerEvents() throws Exception {

      ActiveMQServer activeMQServer = createServerWithLoggingPlugin(LoggingActiveMQServerPlugin.LOG_CONSUMER_EVENTS);
      activeMQServer.start();

      try {

         sendAndReceive(true, true, "txtMessage", 0);

         assertEquals("created connections", 0, createdConnectionLogs.size());
         assertEquals("destroyed connections", 0, destroyedConnectionLogs.size());
         assertEquals("created sessions", 0, createdSessionLogs.size());
         assertEquals("closed sessions", 0, createdSessionLogs.size());
         assertEquals("created consumer", 1, createdConsumerLogs.size());
         assertEquals("closed consumer", 2, closedConsumerLogs.size());
         assertEquals("delivered message", 0, deliveredLogs.size());
         assertEquals("acked message", 0, ackedLogs.size());
         assertEquals("sending message", 0, sentLogs.size());
         assertEquals("routing message", 0, routedLogs.size());
         assertEquals("queue created", 0, createdQueueLogs.size());
         assertEquals("queue destroyed", 0, destroyedQueueLogs.size());
         assertEquals("expired message", 0, messageExpiredLogs.size());
         assertEquals("unexpected logs", 0, unexpectedLogs.size());

      } finally {
         activeMQServer.stop();
         //reset the logs lists
         clearLogLists();
      }
   }

   /**
    *  Aim: test the session create/close events are logged when plugin configured with
    * LOG_SESSION_EVENTS
    *
    * Overriden as addedMetaData does not seem to be invoked for AMQP
    *
    * @throws Exception
    */
   @Override
   @Test
   @BMRules(rules = {@BMRule(name = "test LOG_SESSION_EVENTS",
      targetClass = "org.jboss.logging.Logger",
      targetMethod = "logv",
      targetLocation = "ENTRY",
      action = "org.apache.activemq.artemis.tests.extras.byteman.LoggingActiveMQServerPluginTest.infoLog($2, $4, $0)")})

   public void testLogSessionEvents() throws Exception {

      ActiveMQServer activeMQServer  = createServerWithLoggingPlugin(LoggingActiveMQServerPlugin.LOG_SESSION_EVENTS);
      activeMQServer.start();

      try {

         sendAndReceive(false,false,"test_message",0);
         Thread.sleep(500);

         assertEquals("created connections", 0, createdConnectionLogs.size());
         assertEquals("destroyed connections", 0, destroyedConnectionLogs.size());
         assertEquals("created sessions", 2, createdSessionLogs.size());
         assertEquals("closed sessions", 2, closedSessionLogs.size());
         assertEquals("unexpected logs", 0, unexpectedLogs.size());


      } finally {
         activeMQServer.stop();
         //reset the logs lists
         clearLogLists();
      }
   }

   @Override
   protected Connection createActiveMQConnection() throws JMSException {
      JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:61616");
      return factory.createConnection();
   }
}