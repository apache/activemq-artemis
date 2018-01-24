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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.plugin.impl.LoggingActiveMQServerPlugin;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.jboss.logging.Logger.Level;

/**
 * NOTE: this test should be run at log level INFO
 *
 * This test checks the LoggingActiveMQServerPlugin is logging expected data with specific property configurations
 * when using CORE protocol
 *
 * Byteman is used to intercept the logged message and uses a helper method to put the messages into seperate lists
 * based on its content. The test subsequently checks to ensure correct number of logs in each list.
 */
@RunWith(BMUnitRunner.class)
public class LoggingActiveMQServerPluginTest extends ActiveMQTestBase {


   //buckets for logging
   protected static List<String> createdConnectionLogs = new ArrayList<>();
   protected static List<String> destroyedConnectionLogs = new ArrayList<>();
   protected static List<String> createdSessionLogs = new ArrayList<>();
   protected static List<String> closedSessionLogs = new ArrayList<>();
   protected static List<String> createdConsumerLogs = new ArrayList<>();
   protected static List<String> closedConsumerLogs = new ArrayList<>();
   protected static List<String> deliveredLogs = new ArrayList<>();
   protected static List<String> ackedLogs = new ArrayList<>();
   protected static List<String> sentLogs = new ArrayList<>();
   protected static List<String> routedLogs = new ArrayList<>();
   protected static List<String> createdQueueLogs = new ArrayList<>();
   protected static List<String> destroyedQueueLogs = new ArrayList<>();
   protected static List<String> messageExpiredLogs = new ArrayList<>();
   protected static List<String> unexpectedLogs = new ArrayList<>();
   protected static List<String> addedSessionMetaData = new ArrayList<>();

   /**
    * Aim: verify connection creation/destroy is logged when LOG_CONNECTION_EVENTS is set
    *
    * @throws Exception
    */

   @Test
   @BMRules(rules = {@BMRule(name = "test LOG_CONNECTION_EVENTS",
      targetClass = "org.jboss.logging.Logger",
      targetMethod = "logv",
      targetLocation = "ENTRY",
      action = "org.apache.activemq.artemis.tests.extras.byteman.LoggingActiveMQServerPluginTest.infoLog($2, $4, $0)")})
   public void testLogConnectEvents() throws Exception {

      ActiveMQServer activeMQServer = createServerWithLoggingPlugin(LoggingActiveMQServerPlugin.LOG_CONNECTION_EVENTS);
      activeMQServer.start();

      try {

         Connection connection = createActiveMQConnection();
         connection.start();
         Thread.sleep(100);
         connection.close();

         //ensure logs are collected.
         Thread.sleep(500);

         assertEquals("created connections", 1, createdConnectionLogs.size());
         assertEquals("destroyed connections", 1, destroyedConnectionLogs.size());
         assertEquals("created sessions", 0, createdSessionLogs.size());
         assertEquals("closed sessions", 0, createdSessionLogs.size());
         assertEquals("created consumer", 0, createdConsumerLogs.size());
         assertEquals("closed consumer", 0, closedConsumerLogs.size());
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
    * NOTE: Session plugin points seem to be invoked twice - did not expect that.
    *
    * @throws Exception
    */
   @Test
   @BMRules(rules = {@BMRule(name = "test LOG_SESSION_EVENTS",
      targetClass = "org.jboss.logging.Logger",
      targetMethod = "logv",
      targetLocation = "ENTRY",
      action = "org.apache.activemq.artemis.tests.extras.byteman.LoggingActiveMQServerPluginTest.infoLog($2, $4, $0)")})
   public void testLogSessionEvents() throws Exception {

      ActiveMQServer activeMQServer = createServerWithLoggingPlugin(LoggingActiveMQServerPlugin.LOG_SESSION_EVENTS);
      activeMQServer.start();

      try {

         sendAndReceive(false, false, "test_message", 0);
         Thread.sleep(500);

         assertEquals("created connections", 0, createdConnectionLogs.size());
         assertEquals("destroyed connections", 0, destroyedConnectionLogs.size());
         assertEquals("created sessions", 2, createdSessionLogs.size());
         assertEquals("closed sessions", 2, closedSessionLogs.size());
         assertEquals("added Metadata logs", 2, addedSessionMetaData.size());
         assertEquals("unexpected logs", 0, unexpectedLogs.size());

      } finally {
         activeMQServer.stop();
         //reset the logs lists
         clearLogLists();
      }
   }

   /**
    * Aim: test the consumer create/close events are logged when plugin configured with
    * LOG_CONSUMER_EVENTS
    * @throws Exception
    */

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
         assertEquals("closed consumer", 1, closedConsumerLogs.size());
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
    * Aim: test delivering events are logged when plugin configured with
    * LOG_DELIVERING_EVENTS
    * @throws Exception
    */
   @Test
   @BMRules(rules = {@BMRule(name = "test LOG_DELIVERING_EVENTS",
      targetClass = "org.jboss.logging.Logger",
      targetMethod = "logv",
      targetLocation = "ENTRY",
      action = "org.apache.activemq.artemis.tests.extras.byteman.LoggingActiveMQServerPluginTest.infoLog($2, $4, $0)")})
   public void testLogDeliveringEvents() throws Exception {

      ActiveMQServer activeMQServer = createServerWithLoggingPlugin(LoggingActiveMQServerPlugin.LOG_DELIVERING_EVENTS);
      activeMQServer.start();

      try {

         sendAndReceive(true, true, "txtMessage 1", 0);

         assertEquals("created connections", 0, createdConnectionLogs.size());
         assertEquals("destroyed connections", 0, destroyedConnectionLogs.size());
         assertEquals("created sessions", 0, createdSessionLogs.size());
         assertEquals("closed sessions", 0, createdSessionLogs.size());
         assertEquals("created consumer", 0, createdConsumerLogs.size());
         assertEquals("closed consumer", 0, closedConsumerLogs.size());
         assertEquals("delivered message", 1, deliveredLogs.size());
         assertEquals("acked message", 1, ackedLogs.size());
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
    * Aim: test sending events are logged when plugin configured with
    * LOG_SENDING_EVENTS
    * @throws Exception
    */
   @Test
   @BMRules(rules = {@BMRule(name = "test LOG_SENDING_EVENTS",
      targetClass = "org.jboss.logging.Logger",
      targetMethod = "logv",
      targetLocation = "ENTRY",
      action = "org.apache.activemq.artemis.tests.extras.byteman.LoggingActiveMQServerPluginTest.infoLog($2, $4, $0)")})
   public void testLogSendingEvents() throws Exception {

      //initial plugin
      ActiveMQServer activeMQServer = createServerWithLoggingPlugin(LoggingActiveMQServerPlugin.LOG_SENDING_EVENTS);
      activeMQServer.start();

      try {

         sendAndReceive(true, true, "txtMessage 1", 0);
         sendAndReceive(true, true, "txtMessage 2", 0);

         assertEquals("created connections", 0, createdConnectionLogs.size());
         assertEquals("destroyed connections", 0, destroyedConnectionLogs.size());
         assertEquals("created sessions", 0, createdSessionLogs.size());
         assertEquals("closed sessions", 0, createdSessionLogs.size());
         assertEquals("created consumer", 0, createdConsumerLogs.size());
         assertEquals("closed consumer", 0, closedConsumerLogs.size());
         assertEquals("delivered message", 0, deliveredLogs.size());
         assertEquals("acked message", 0, ackedLogs.size());
         assertEquals("sending message", 2, sentLogs.size());
         assertEquals("routing message", 2, routedLogs.size());
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
    * Aim: test queue creation/destroy are logged when plugin configured with
    * LOG_INTERNAL_EVENTS
    * @throws Exception
    */
   @Test
   @BMRules(rules = {@BMRule(name = "test queue creation log",
      targetClass = "org.jboss.logging.Logger",
      targetMethod = "logv",
      targetLocation = "ENTRY",
      action = "org.apache.activemq.artemis.tests.extras.byteman.LoggingActiveMQServerPluginTest.infoLog($2, $4, $0)")})
   public void testQueueCreationLog() throws Exception {

      //initial plugin
      ActiveMQServer activeMQServer = createServerWithLoggingPlugin(LoggingActiveMQServerPlugin.LOG_INTERNAL_EVENTS);
      activeMQServer.start();

      try {

         sendAndReceive(false, true, "NO_MESSAGE", 0);

         assertEquals("created connections", 0, createdConnectionLogs.size());
         assertEquals("destroyed connections", 0, destroyedConnectionLogs.size());
         assertEquals("created sessions", 0, createdSessionLogs.size());
         assertEquals("closed sessions", 0, createdSessionLogs.size());
         assertEquals("created consumer", 0, createdConsumerLogs.size());
         assertEquals("closed consumer", 0, closedConsumerLogs.size());
         assertEquals("delivered message", 0, deliveredLogs.size());
         assertEquals("acked message", 0, ackedLogs.size());
         assertEquals("sending message", 0, sentLogs.size());
         assertEquals("routing message", 0, routedLogs.size());
         assertEquals("queue created", 1, createdQueueLogs.size());
         assertEquals("queue destroyed", 1, destroyedQueueLogs.size());
         assertEquals("expired message", 0, messageExpiredLogs.size());
         assertEquals("unexpected logs", 0, unexpectedLogs.size());

      } finally {
         activeMQServer.stop();
         //reset the logs lists
         clearLogLists();
      }
   }

   /**
    * Aim: test message expiry is logged when plugin configured with
    * LOG_INTERNAL_EVENTS
    * @throws Exception
    */
   @Test
   @BMRules(rules = {@BMRule(name = "test queue creation log",
      targetClass = "org.jboss.logging.Logger",
      targetMethod = "logv",
      targetLocation = "ENTRY",
      action = "org.apache.activemq.artemis.tests.extras.byteman.LoggingActiveMQServerPluginTest.infoLog($2, $4, $0)")})
   public void testExpireMessageLog() throws Exception {

      //initial plugin
      ActiveMQServer activeMQServer = createServerWithLoggingPlugin(LoggingActiveMQServerPlugin.LOG_INTERNAL_EVENTS);
      activeMQServer.start();

      try {

         sendAndReceive(true, false, "message to expiry", 100);
         Thread.sleep(1000);
         sendAndReceive(false, true, "NO_MESSAGE", 0);

         assertEquals("created connections", 0, createdConnectionLogs.size());
         assertEquals("destroyed connections", 0, destroyedConnectionLogs.size());
         assertEquals("created sessions", 0, createdSessionLogs.size());
         assertEquals("closed sessions", 0, createdSessionLogs.size());
         assertEquals("created consumer", 0, createdConsumerLogs.size());
         assertEquals("closed consumer", 0, closedConsumerLogs.size());
         assertEquals("delivered message", 0, deliveredLogs.size());
         assertEquals("acked message", 0, ackedLogs.size());
         assertEquals("sending message", 0, sentLogs.size());
         assertEquals("routing message", 0, routedLogs.size());
         assertEquals("queue created", 1, createdQueueLogs.size());
         assertEquals("queue destroyed", 1, destroyedQueueLogs.size());
         assertEquals("expired message", 1, messageExpiredLogs.size());
         assertEquals("unexpected logs", 0, unexpectedLogs.size());

      } finally {
         activeMQServer.stop();
         //reset the logs lists
         clearLogLists();
      }
   }

   /**
    * Aim: test all events are logged when plugin configured with
    * LOG_ALL_EVENTS
    * @throws Exception
    */
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
         assertEquals("closed consumer", 2, closedConsumerLogs.size());
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

   //collect the log invocation from LoggingActiveMQServerPlugin
   public static void infoLog(Level level, Object message, org.jboss.logging.Logger logger) {

      //only interested in log level INFO
      if (!level.equals(Level.INFO)) {
         return;
      }

      //only interested in one logger
      if (!logger.getName().startsWith(LoggingActiveMQServerPlugin.class.getPackage().getName())) {
         return;
      }

      String stringMessage = (String) message;

      if (stringMessage.startsWith("AMQ841000")) {
         createdConnectionLogs.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841001")) {
         destroyedConnectionLogs.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841002")) {
         createdSessionLogs.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841003")) {
         closedSessionLogs.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841004")) {
         addedSessionMetaData.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841005")) {
         createdConsumerLogs.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841006")) {
         closedConsumerLogs.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841012")) {
         deliveredLogs.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841014")) {
         ackedLogs.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841009")) {
         sentLogs.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841010")) {
         routedLogs.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841007")) {
         createdQueueLogs.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841008")) {
         destroyedQueueLogs.add(stringMessage);
      } else if (stringMessage.startsWith("AMQ841013")) {
         messageExpiredLogs.add(stringMessage);
      } else {
         unexpectedLogs.add(stringMessage);
      }

   }

   //----- helper methods ---

   protected void clearLogLists() {
      createdConnectionLogs.clear();
      destroyedConnectionLogs.clear();
      createdSessionLogs.clear();
      closedSessionLogs.clear();
      createdConsumerLogs.clear();
      closedConsumerLogs.clear();
      deliveredLogs.clear();
      ackedLogs.clear();
      sentLogs.clear();
      routedLogs.clear();
      createdQueueLogs.clear();
      destroyedQueueLogs.clear();
      messageExpiredLogs.clear();
      unexpectedLogs.clear();
      addedSessionMetaData.clear();
   }

   protected ActiveMQServer createServerWithLoggingPlugin(String loggingPluginEventType) throws Exception {
      //initial plugin
      LoggingActiveMQServerPlugin loggingActiveMQServerPlugin = new LoggingActiveMQServerPlugin();
      Map<String, String> properties = new HashMap<>();
      properties.put(loggingPluginEventType, "true");
      loggingActiveMQServerPlugin.init(properties);

      //register
      Configuration config = createDefaultConfig(true);
      config.registerBrokerPlugin(loggingActiveMQServerPlugin);

      return createServer(false, config);
   }

   protected void sendAndReceive(boolean send,
                                 boolean receive,
                                 String txtMessage,
                                 long expiry) throws JMSException, InterruptedException {
      Connection connection = createActiveMQConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();
      Queue queue = session.createQueue("TEST.QUEUE");
      MessageConsumer messageConsumer = null;

      if (receive) {
         messageConsumer = session.createConsumer(queue);
         Thread.sleep(1000);
      }
      if (send) {
         MessageProducer messageProducer = session.createProducer(queue);
         if (expiry > 0) {
            messageProducer.setTimeToLive(expiry);
         }
         messageProducer.send(session.createTextMessage(txtMessage));
      }
      if (receive) {
         messageConsumer.receive(100);
         messageConsumer.close();
      }

      session.close();
      connection.close();
   }

   protected Connection createActiveMQConnection() throws JMSException {
      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
      return (ActiveMQConnection) factory.createConnection();
   }
}