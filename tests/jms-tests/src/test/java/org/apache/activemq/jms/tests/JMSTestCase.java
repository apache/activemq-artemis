/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.jms.tests;
import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.TopicConnection;
import javax.jms.XAConnection;
import javax.naming.InitialContext;

import org.apache.activemq6.api.core.client.HornetQClient;
import org.apache.activemq6.api.jms.JMSFactoryType;
import org.apache.activemq6.jms.client.HornetQJMSConnectionFactory;
import org.apache.activemq6.jms.client.HornetQQueueConnectionFactory;
import org.apache.activemq6.jms.client.HornetQTopicConnectionFactory;
import org.junit.After;
import org.junit.Before;

/**
 * @deprecated this infrastructure should not be used for new code. New tests should go into
 *             org.apache.activemq6.tests.integration.jms at the integration-tests project.
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
@Deprecated
public class JMSTestCase extends HornetQServerTestCase
{

   protected static final ArrayList<String> NETTY_CONNECTOR = new ArrayList<String>();

   static
   {
      NETTY_CONNECTOR.add("netty");
   }

   protected HornetQJMSConnectionFactory cf;

   protected HornetQQueueConnectionFactory queueCf;

   protected HornetQTopicConnectionFactory topicCf;

   protected InitialContext ic;

   protected static final String defaultConf = "all";

   protected static String conf;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      ic = getInitialContext();

      // All jms tests should use a specific cg which has blockOnAcknowledge = true and
      // both np and p messages are sent synchronously


      getJmsServerManager().createConnectionFactory("testsuitecf",
                                                    false,
                                                    JMSFactoryType.CF,
                                                    NETTY_CONNECTOR,
                                                    null,
                                                    HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                    HornetQClient.DEFAULT_CONNECTION_TTL,
                                                    HornetQClient.DEFAULT_CALL_TIMEOUT,
                                                    HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                                                    HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                                    HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                    HornetQClient.DEFAULT_COMPRESS_LARGE_MESSAGES,
                                                    HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                                                    HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                                                    true,
                                                    true,
                                                    true,
                                                    HornetQClient.DEFAULT_AUTO_GROUP,
                                                    HornetQClient.DEFAULT_PRE_ACKNOWLEDGE,
                                                    HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                    HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                                    HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                                    HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                                                    HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                                    HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                                                    HornetQClient.DEFAULT_RETRY_INTERVAL,
                                                    HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                    HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                    HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                                                    HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
                                                    null,
                                                    "/testsuitecf");

      getJmsServerManager().createConnectionFactory("testsuitecf_queue",
                                                    false,
                                                    JMSFactoryType.QUEUE_CF,
                                                    NETTY_CONNECTOR,
                                                    null,
                                                    HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                    HornetQClient.DEFAULT_CONNECTION_TTL,
                                                    HornetQClient.DEFAULT_CALL_TIMEOUT,
                                                    HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                                                    HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                                    HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                    HornetQClient.DEFAULT_COMPRESS_LARGE_MESSAGES,
                                                    HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                                                    HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                                                    true,
                                                    true,
                                                    true,
                                                    HornetQClient.DEFAULT_AUTO_GROUP,
                                                    HornetQClient.DEFAULT_PRE_ACKNOWLEDGE,
                                                    HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                    HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                                    HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                                    HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                                                    HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                                    HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                                                    HornetQClient.DEFAULT_RETRY_INTERVAL,
                                                    HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                    HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                    HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                                                    HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
                                                    null,
                                                    "/testsuitecf_queue");

      getJmsServerManager().createConnectionFactory("testsuitecf_topic",
                                                    false,
                                                    JMSFactoryType.TOPIC_CF,
                                                    NETTY_CONNECTOR,
                                                    null,
                                                    HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                    HornetQClient.DEFAULT_CONNECTION_TTL,
                                                    HornetQClient.DEFAULT_CALL_TIMEOUT,
                                                    HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                                                    HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                                    HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                    HornetQClient.DEFAULT_COMPRESS_LARGE_MESSAGES,
                                                    HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                                                    HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                                                    true,
                                                    true,
                                                    true,
                                                    HornetQClient.DEFAULT_AUTO_GROUP,
                                                    HornetQClient.DEFAULT_PRE_ACKNOWLEDGE,
                                                    HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                    HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                                    HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                                    HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                                                    HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                                    HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                                                    HornetQClient.DEFAULT_RETRY_INTERVAL,
                                                    HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                    HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                    HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                                                    HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
                                                    null,
                                                    "/testsuitecf_topic");

      cf = (HornetQJMSConnectionFactory)getInitialContext().lookup("/testsuitecf");
      queueCf = (HornetQQueueConnectionFactory)getInitialContext().lookup("/testsuitecf_queue");
      topicCf = (HornetQTopicConnectionFactory)getInitialContext().lookup("/testsuitecf_topic");

      assertRemainingMessages(0);
   }

   protected final JMSContext createContext()
   {
      return addContext(cf.createContext());
   }


   protected final Connection createConnection() throws JMSException
   {
      Connection c = cf.createConnection();
      return addConnection(c);
   }

   protected final TopicConnection createTopicConnection() throws JMSException
   {
      TopicConnection c = cf.createTopicConnection();
      addConnection(c);
      return c;
   }

   protected final QueueConnection createQueueConnection() throws JMSException
   {
      QueueConnection c = cf.createQueueConnection();
      addConnection(c);
      return c;
   }

   protected final XAConnection createXAConnection() throws JMSException
   {
      XAConnection c = cf.createXAConnection();
      addConnection(c);
      return c;
   }


   protected final Connection createConnection(String user, String password) throws JMSException
   {
      Connection c = cf.createConnection(user, password);
      addConnection(c);
      return c;
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
      getJmsServerManager().destroyConnectionFactory("testsuitecf");
      if (cf != null)
      {
         cf.close();
      }

      cf = null;

      assertRemainingMessages(0);
   }

   protected Connection createConnection(ConnectionFactory cf1) throws JMSException
   {
      return addConnection(cf1.createConnection());
   }
}
