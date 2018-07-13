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
package org.apache.activemq.artemis.jms.tests;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.TopicConnection;
import javax.jms.XAConnection;
import javax.naming.InitialContext;
import java.util.ArrayList;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQQueueConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQTopicConnectionFactory;
import org.junit.After;
import org.junit.Before;

/**
 * @deprecated this infrastructure should not be used for new code. New tests should go into
 * org.apache.activemq.tests.integration.jms at the integration-tests project.
 */
@Deprecated
public class JMSTestCase extends ActiveMQServerTestCase {

   protected static final ArrayList<String> NETTY_CONNECTOR = new ArrayList<>();

   static {
      NETTY_CONNECTOR.add("netty");
   }

   protected ActiveMQJMSConnectionFactory cf;

   protected ActiveMQQueueConnectionFactory queueCf;

   protected ActiveMQTopicConnectionFactory topicCf;

   protected InitialContext ic;

   protected static final String defaultConf = "all";

   protected static String conf;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      ic = getInitialContext();

      // All jms tests should use a specific cg which has blockOnAcknowledge = true and
      // both np and p messages are sent synchronously

      getJmsServerManager().createConnectionFactory("testsuitecf", false, JMSFactoryType.CF, NETTY_CONNECTOR, null, ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD, ActiveMQClient.DEFAULT_CONNECTION_TTL, ActiveMQClient.DEFAULT_CALL_TIMEOUT, ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT, ActiveMQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, ActiveMQClient.DEFAULT_COMPRESS_LARGE_MESSAGES, ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE, ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE, ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE, ActiveMQClient.DEFAULT_PRODUCER_WINDOW_SIZE, ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE, true, true, true, ActiveMQClient.DEFAULT_AUTO_GROUP, ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE, ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE, ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS, ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE, ActiveMQClient.DEFAULT_RETRY_INTERVAL, ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER, ActiveMQClient.DEFAULT_MAX_RETRY_INTERVAL, ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS, ActiveMQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION, null, "/testsuitecf");

      getJmsServerManager().createConnectionFactory("testsuitecf_queue", false, JMSFactoryType.QUEUE_CF, NETTY_CONNECTOR, null, ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD, ActiveMQClient.DEFAULT_CONNECTION_TTL, ActiveMQClient.DEFAULT_CALL_TIMEOUT, ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT, ActiveMQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, ActiveMQClient.DEFAULT_COMPRESS_LARGE_MESSAGES, ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE, ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE, ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE, ActiveMQClient.DEFAULT_PRODUCER_WINDOW_SIZE, ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE, true, true, true, ActiveMQClient.DEFAULT_AUTO_GROUP, ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE, ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE, ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS, ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE, ActiveMQClient.DEFAULT_RETRY_INTERVAL, ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER, ActiveMQClient.DEFAULT_MAX_RETRY_INTERVAL, ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS, ActiveMQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION, null, "/testsuitecf_queue");

      getJmsServerManager().createConnectionFactory("testsuitecf_topic", false, JMSFactoryType.TOPIC_CF, NETTY_CONNECTOR, null, ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD, ActiveMQClient.DEFAULT_CONNECTION_TTL, ActiveMQClient.DEFAULT_CALL_TIMEOUT, ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT, ActiveMQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, ActiveMQClient.DEFAULT_COMPRESS_LARGE_MESSAGES, ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE, ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE, ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE, ActiveMQClient.DEFAULT_PRODUCER_WINDOW_SIZE, ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE, true, true, true, ActiveMQClient.DEFAULT_AUTO_GROUP, ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE, ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE, ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS, ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE, ActiveMQClient.DEFAULT_RETRY_INTERVAL, ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER, ActiveMQClient.DEFAULT_MAX_RETRY_INTERVAL, ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS, ActiveMQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION, null, "/testsuitecf_topic");

      cf = (ActiveMQJMSConnectionFactory) getInitialContext().lookup("/testsuitecf");
      queueCf = (ActiveMQQueueConnectionFactory) getInitialContext().lookup("/testsuitecf_queue");
      topicCf = (ActiveMQTopicConnectionFactory) getInitialContext().lookup("/testsuitecf_topic");

      assertRemainingMessages(0);
   }

   protected final JMSContext createContext() {
      return addContext(cf.createContext());
   }

   protected final Connection createConnection() throws JMSException {
      Connection c = cf.createConnection();
      return addConnection(c);
   }

   protected final TopicConnection createTopicConnection() throws JMSException {
      TopicConnection c = cf.createTopicConnection();
      addConnection(c);
      return c;
   }

   protected final QueueConnection createQueueConnection() throws JMSException {
      QueueConnection c = cf.createQueueConnection();
      addConnection(c);
      return c;
   }

   protected final XAConnection createXAConnection() throws JMSException {
      XAConnection c = cf.createXAConnection();
      addConnection(c);
      return c;
   }

   protected final Connection createConnection(String user, String password) throws JMSException {
      Connection c = cf.createConnection(user, password);
      addConnection(c);
      return c;
   }

   @Override
   @After
   public void tearDown() throws Exception {
      super.tearDown();
      getJmsServerManager().destroyConnectionFactory("testsuitecf");
      if (cf != null) {
         cf.close();
      }

      cf = null;

      assertRemainingMessages(0);
   }

   protected Connection createConnection(ConnectionFactory cf1) throws JMSException {
      return addConnection(cf1.createConnection());
   }
}
