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
package org.apache.activemq.tests.integration.twitter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.core.config.CoreQueueConfiguration;
import org.apache.activemq.core.server.ConnectorService;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.integration.twitter.TwitterConstants;
import org.apache.activemq.integration.twitter.TwitterIncomingConnectorServiceFactory;
import org.apache.activemq.integration.twitter.TwitterOutgoingConnectorServiceFactory;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.http.AccessToken;

/**
 * A TwitterTest
 *
 * @author tm.igarashi@gmail.com
 */
public class TwitterTest extends ServiceTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;
   private static final String KEY_CONNECTOR_NAME = "connector.name";
   private static final String KEY_CONSUMER_KEY = "consumerKey";
   private static final String KEY_CONSUMER_SECRET = "consumerSecret";
   private static final String KEY_ACCESS_TOKEN = "accessToken";
   private static final String KEY_ACCESS_TOKEN_SECRET = "accessTokenSecret";
   private static final String KEY_QUEUE_NAME = "queue.name";

   private static final String TWITTER_CONSUMER_KEY = System.getProperty("twitter.consumerKey");
   private static final String TWITTER_CONSUMER_SECRET = System.getProperty("twitter.consumerSecret");
   private static final String TWITTER_ACCESS_TOKEN = System.getProperty("twitter.accessToken");
   private static final String TWITTER_ACCESS_TOKEN_SECRET = System.getProperty("twitter.accessTokenSecret");

   // incoming

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
   }

   @BeforeClass
   public static void hasCredentials()
   {
      Assume.assumeNotNull(TWITTER_CONSUMER_KEY);
      Assume.assumeFalse("null".equals(TWITTER_CONSUMER_KEY));
   }

   @Test
   public void testSimpleIncoming() throws Exception
   {
      internalTestIncoming(true, false);
   }

   @Test
   public void testIncomingNoQueue() throws Exception
   {
      internalTestIncoming(false, false);
   }

   @Test
   public void testIncomingWithRestart() throws Exception
   {
      internalTestIncoming(true, true);
   }

   @Test
   public void testIncomingWithEmptyConnectorName() throws Exception
   {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put(KEY_CONNECTOR_NAME, "");
      internalTestIncomingFailedToInitialize(params);
   }

   @Test
   public void testIncomingWithEmptyQueueName() throws Exception
   {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put(KEY_QUEUE_NAME, "");
      internalTestIncomingFailedToInitialize(params);
   }

   @Test
   public void testIncomingWithInvalidCredentials() throws Exception
   {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put(KEY_CONSUMER_KEY, "invalidConsumerKey");
      params.put(KEY_CONSUMER_SECRET, "invalidConsumerSecret");
      params.put(KEY_ACCESS_TOKEN, "invalidAccessToken");
      params.put(KEY_ACCESS_TOKEN_SECRET, "invalidAcccessTokenSecret");
      internalTestIncomingFailedToInitialize(params);
   }

   //outgoing

   @Test
   public void testSimpleOutgoing() throws Exception
   {
      internalTestOutgoing(true, false);
   }

   @Test
   public void testOutgoingNoQueue() throws Exception
   {
      internalTestOutgoing(false, false);
   }

   @Test
   public void testOutgoingWithRestart() throws Exception
   {
      internalTestOutgoing(true, true);
   }

   @Test
   public void testOutgoingWithEmptyConnectorName() throws Exception
   {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put(KEY_CONNECTOR_NAME, "");
      internalTestOutgoingFailedToInitialize(params);
   }

   @Test
   public void testOutgoingWithEmptyQueueName() throws Exception
   {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put(KEY_QUEUE_NAME, "");
      internalTestOutgoingFailedToInitialize(params);
   }

   @Test
   public void testOutgoingWithInvalidCredentials() throws Exception
   {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put(KEY_CONSUMER_KEY, "invalidConsumerKey");
      params.put(KEY_CONSUMER_SECRET, "invalidConsumerSecret");
      params.put(KEY_ACCESS_TOKEN, "invalidAccessToken");
      params.put(KEY_ACCESS_TOKEN_SECRET, "invalidAcccessTokenSecret");
      internalTestOutgoingFailedToInitialize(params);
   }

   @Test
   public void testOutgoingWithInReplyTo() throws Exception
   {
      internalTestOutgoingWithInReplyTo();
   }

   protected void internalTestIncoming(boolean createQueue, boolean restart) throws Exception
   {
      ActiveMQServer server0 = null;
      ClientSession session = null;
      ServerLocator locator = null;
      String queue = "TwitterTestQueue";
      int interval = 5;
      Twitter twitter = new TwitterFactory().getOAuthAuthorizedInstance(TWITTER_CONSUMER_KEY,
                                                                        TWITTER_CONSUMER_SECRET,
                                                                        new AccessToken(TWITTER_ACCESS_TOKEN,
                                                                                        TWITTER_ACCESS_TOKEN_SECRET));
      String testMessage = "TwitterTest/incoming: " + System.currentTimeMillis();
      log.debug("test incoming: " + testMessage);

      try
      {
         HashMap<String, Object> config = new HashMap<String, Object>();
         config.put(TwitterConstants.INCOMING_INTERVAL, interval);
         config.put(TwitterConstants.QUEUE_NAME, queue);
         config.put(TwitterConstants.CONSUMER_KEY, TWITTER_CONSUMER_KEY);
         config.put(TwitterConstants.CONSUMER_SECRET, TWITTER_CONSUMER_SECRET);
         config.put(TwitterConstants.ACCESS_TOKEN, TWITTER_ACCESS_TOKEN);
         config.put(TwitterConstants.ACCESS_TOKEN_SECRET, TWITTER_ACCESS_TOKEN_SECRET);
         ConnectorServiceConfiguration inconf = new ConnectorServiceConfiguration()
            .setFactoryClassName(TwitterIncomingConnectorServiceFactory.class.getName())
            .setParams(config)
            .setName("test-incoming-connector");

         Configuration configuration = createDefaultConfig(false)
            .addConnectorServiceConfiguration(inconf);

         if (createQueue)
         {
            CoreQueueConfiguration qc = new CoreQueueConfiguration()
               .setAddress(queue)
               .setName(queue);
            configuration.getQueueConfigurations().add(qc);
         }

         server0 = createServer(false, configuration);
         server0.start();

         if (restart)
         {
            server0.getConnectorsService().stop();
            server0.getConnectorsService().start();
         }

         assertEquals(1, server0.getConnectorsService().getConnectors().size());
         Iterator<ConnectorService> connectorServiceIterator = server0.getConnectorsService().getConnectors().iterator();
         if (createQueue)
         {
            Assert.assertTrue(connectorServiceIterator.next().isStarted());
         }
         else
         {
            Assert.assertFalse(connectorServiceIterator.next().isStarted());
            return;
         }

         twitter.updateStatus(testMessage);

         TransportConfiguration tpconf = new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY);
         locator = ActiveMQClient.createServerLocatorWithoutHA(tpconf);
         ClientSessionFactory sf = createSessionFactory(locator);
         session = sf.createSession(false, true, true);
         ClientConsumer consumer = session.createConsumer(queue);
         session.start();
         ClientMessage msg = consumer.receive(60 * 1000);

         Assert.assertNotNull(msg);
         Assert.assertEquals(testMessage, msg.getBodyBuffer().readString());

         msg.acknowledge();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable t)
         {
         }

         try
         {
            locator.close();
         }
         catch (Throwable ignored)
         {
         }

         try
         {
            server0.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   protected void internalTestIncomingFailedToInitialize(HashMap<String, String> params) throws Exception
   {
      ActiveMQServer server0 = null;
      String connectorName = "test-incoming-connector";
      String queue = "TwitterTestQueue";
      String consumerKey = "invalidConsumerKey";
      String consumerSecret = "invalidConsumerSecret";
      String accessToken = "invalidAccessToken";
      String accessTokenSecret = "invalidAccessTokenSecret";
      int interval = 5;

      if (params.containsKey(KEY_CONNECTOR_NAME))
      {
         connectorName = params.get(KEY_CONNECTOR_NAME);
      }
      if (params.containsKey(KEY_CONSUMER_KEY))
      {
         consumerKey = params.get(KEY_CONSUMER_KEY);
      }
      if (params.containsKey(KEY_CONSUMER_SECRET))
      {
         consumerSecret = params.get(KEY_CONSUMER_SECRET);
      }
      if (params.containsKey(KEY_ACCESS_TOKEN))
      {
         accessToken = params.get(KEY_ACCESS_TOKEN);
      }
      if (params.containsKey(KEY_ACCESS_TOKEN_SECRET))
      {
         accessTokenSecret = params.get(KEY_ACCESS_TOKEN_SECRET);
      }
      if (params.containsKey(KEY_QUEUE_NAME))
      {
         queue = params.get(KEY_QUEUE_NAME);
      }

      try
      {
         HashMap<String, Object> config = new HashMap<String, Object>();
         config.put(TwitterConstants.INCOMING_INTERVAL, interval);
         config.put(TwitterConstants.QUEUE_NAME, queue);
         config.put(TwitterConstants.CONSUMER_KEY, consumerKey);
         config.put(TwitterConstants.CONSUMER_SECRET, consumerSecret);
         config.put(TwitterConstants.ACCESS_TOKEN, accessToken);
         config.put(TwitterConstants.ACCESS_TOKEN_SECRET, accessTokenSecret);

         ConnectorServiceConfiguration inconf = new ConnectorServiceConfiguration()
            .setFactoryClassName(TwitterIncomingConnectorServiceFactory.class.getName())
            .setParams(config)
            .setName(connectorName);

         CoreQueueConfiguration qc = new CoreQueueConfiguration()
            .setAddress(queue)
            .setName(queue);

         Configuration configuration = createDefaultConfig(false)
            .addConnectorServiceConfiguration(inconf)
            .addQueueConfiguration(qc);

         server0 = createServer(false, configuration);
         server0.start();

         Set<ConnectorService> conns = server0.getConnectorsService().getConnectors();
         Assert.assertEquals(1, conns.size());
         Iterator<ConnectorService> it = conns.iterator();
         Assert.assertFalse(it.next().isStarted());
      }
      finally
      {
         try
         {
            server0.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   protected void internalTestOutgoing(boolean createQueue, boolean restart) throws Exception
   {
      ActiveMQServer server0 = null;
      ServerLocator locator = null;
      ClientSession session = null;
      String queue = "TwitterTestQueue";
      Twitter twitter = new TwitterFactory().getOAuthAuthorizedInstance(TWITTER_CONSUMER_KEY,
                                                                        TWITTER_CONSUMER_SECRET,
                                                                        new AccessToken(TWITTER_ACCESS_TOKEN,
                                                                                        TWITTER_ACCESS_TOKEN_SECRET));
      String testMessage = "TwitterTest/outgoing: " + System.currentTimeMillis();
      log.debug("test outgoing: " + testMessage);

      try
      {
         HashMap<String, Object> config = new HashMap<String, Object>();
         config.put(TwitterConstants.QUEUE_NAME, queue);
         config.put(TwitterConstants.CONSUMER_KEY, TWITTER_CONSUMER_KEY);
         config.put(TwitterConstants.CONSUMER_SECRET, TWITTER_CONSUMER_SECRET);
         config.put(TwitterConstants.ACCESS_TOKEN, TWITTER_ACCESS_TOKEN);
         config.put(TwitterConstants.ACCESS_TOKEN_SECRET, TWITTER_ACCESS_TOKEN_SECRET);
         ConnectorServiceConfiguration outconf = new ConnectorServiceConfiguration()
            .setFactoryClassName(TwitterOutgoingConnectorServiceFactory.class.getName())
            .setParams(config)
            .setName("test-outgoing-connector");

         Configuration configuration = createDefaultConfig(false)
            .addConnectorServiceConfiguration(outconf);

         if (createQueue)
         {
            CoreQueueConfiguration qc = new CoreQueueConfiguration()
               .setAddress(queue)
               .setName(queue)
               .setDurable(false);
            configuration.getQueueConfigurations().add(qc);
         }

         server0 = createServer(false, configuration);
         server0.start();

         if (restart)
         {
            server0.getConnectorsService().stop();
            server0.getConnectorsService().start();
         }

         assertEquals(1, server0.getConnectorsService().getConnectors().size());
         Iterator<ConnectorService> connectorServiceIterator = server0.getConnectorsService().getConnectors().iterator();
         if (createQueue)
         {
            Assert.assertTrue(connectorServiceIterator.next().isStarted());
         }
         else
         {
            Assert.assertFalse(connectorServiceIterator.next().isStarted());
            return;
         }

         TransportConfiguration tpconf = new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY);
         locator = ActiveMQClient.createServerLocatorWithoutHA(tpconf);
         ClientSessionFactory sf = createSessionFactory(locator);
         session = sf.createSession(false, true, true);
         ClientProducer producer = session.createProducer(queue);
         ClientMessage msg = session.createMessage(false);
         msg.getBodyBuffer().writeString(testMessage);
         session.start();
         producer.send(msg);

         Thread.sleep(3000);

         Paging page = new Paging();
         page.setCount(1);
         ResponseList res = twitter.getHomeTimeline(page);

         Assert.assertEquals(testMessage, ((Status) (res.get(0))).getText());
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable t)
         {
         }

         try
         {
            locator.close();
         }
         catch (Throwable t)
         {
         }

         try
         {
            server0.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   protected void internalTestOutgoingFailedToInitialize(HashMap<String, String> params) throws Exception
   {
      ActiveMQServer server0 = null;
      String connectorName = "test-outgoing-connector";
      String queue = "TwitterTestQueue";
      String consumerKey = TWITTER_CONSUMER_KEY;
      String consumerSecret = TWITTER_CONSUMER_SECRET;
      String accessToken = TWITTER_ACCESS_TOKEN;
      String accessTokenSecret = TWITTER_ACCESS_TOKEN_SECRET;

      if (params.containsKey(KEY_CONNECTOR_NAME))
      {
         connectorName = params.get(KEY_CONNECTOR_NAME);
      }
      if (params.containsKey(KEY_CONSUMER_KEY))
      {
         consumerKey = params.get(KEY_CONSUMER_KEY);
      }
      if (params.containsKey(KEY_CONSUMER_SECRET))
      {
         consumerSecret = params.get(KEY_CONSUMER_SECRET);
      }
      if (params.containsKey(KEY_ACCESS_TOKEN))
      {
         accessToken = params.get(KEY_ACCESS_TOKEN);
      }
      if (params.containsKey(KEY_ACCESS_TOKEN_SECRET))
      {
         accessTokenSecret = params.get(KEY_ACCESS_TOKEN_SECRET);
      }
      if (params.containsKey(KEY_QUEUE_NAME))
      {
         queue = params.get(KEY_QUEUE_NAME);
      }

      try
      {
         HashMap<String, Object> config = new HashMap<String, Object>();
         config.put(TwitterConstants.QUEUE_NAME, queue);
         config.put(TwitterConstants.CONSUMER_KEY, consumerKey);
         config.put(TwitterConstants.CONSUMER_SECRET, consumerSecret);
         config.put(TwitterConstants.ACCESS_TOKEN, accessToken);
         config.put(TwitterConstants.ACCESS_TOKEN_SECRET, accessTokenSecret);

         ConnectorServiceConfiguration outconf = new ConnectorServiceConfiguration()
            .setFactoryClassName(TwitterOutgoingConnectorServiceFactory.class.getName())
            .setParams(config)
            .setName(connectorName);

         CoreQueueConfiguration qc = new CoreQueueConfiguration()
            .setAddress(queue)
            .setName(queue)
            .setDurable(false);

         Configuration configuration = createDefaultConfig(false)
            .addConnectorServiceConfiguration(outconf)
            .addQueueConfiguration(qc);

         server0 = createServer(false, configuration);
         server0.start();

      }
      finally
      {
         try
         {
            server0.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   protected void internalTestOutgoingWithInReplyTo() throws Exception
   {
      ActiveMQServer server0 = null;
      ClientSession session = null;
      ServerLocator locator = null;
      String queue = "TwitterTestQueue";
      Twitter twitter = new TwitterFactory().getOAuthAuthorizedInstance(TWITTER_CONSUMER_KEY,
                                                                        TWITTER_CONSUMER_SECRET,
                                                                        new AccessToken(TWITTER_ACCESS_TOKEN,
                                                                                        TWITTER_ACCESS_TOKEN_SECRET));
      String testMessage = "TwitterTest/outgoing with in_reply_to: " + System.currentTimeMillis();
      String replyMessage = "@" + twitter.getScreenName() + " TwitterTest/outgoing reply: " + System.currentTimeMillis();
      try
      {
         HashMap<String, Object> config = new HashMap<String, Object>();
         config.put(TwitterConstants.QUEUE_NAME, queue);
         config.put(TwitterConstants.CONSUMER_KEY, TWITTER_CONSUMER_KEY);
         config.put(TwitterConstants.CONSUMER_SECRET, TWITTER_CONSUMER_SECRET);
         config.put(TwitterConstants.ACCESS_TOKEN, TWITTER_ACCESS_TOKEN);
         config.put(TwitterConstants.ACCESS_TOKEN_SECRET, TWITTER_ACCESS_TOKEN_SECRET);

         ConnectorServiceConfiguration outconf = new ConnectorServiceConfiguration()
            .setFactoryClassName(TwitterOutgoingConnectorServiceFactory.class.getName())
            .setParams(config)
            .setName("test-outgoing-with-in-reply-to");

         CoreQueueConfiguration qc = new CoreQueueConfiguration()
            .setAddress(queue)
            .setName(queue)
            .setDurable(false);

         Configuration configuration = createDefaultConfig(false)
            .addConnectorServiceConfiguration(outconf)
            .addQueueConfiguration(qc);

         Status s = twitter.updateStatus(testMessage);

         server0 = createServer(false, configuration);
         server0.start();

         TransportConfiguration tpconf = new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY);
         locator = ActiveMQClient.createServerLocatorWithoutHA(tpconf);

         ClientSessionFactory sf = createSessionFactory(locator);
         session = sf.createSession(false, true, true);
         ClientProducer producer = session.createProducer(queue);
         ClientMessage msg = session.createMessage(false);
         msg.getBodyBuffer().writeString(replyMessage);
         msg.putLongProperty(TwitterConstants.KEY_IN_REPLY_TO_STATUS_ID, s.getId());
         session.start();
         producer.send(msg);

         Thread.sleep(3000);

         Paging page = new Paging();
         page.setCount(2);
         ResponseList res = twitter.getHomeTimeline(page);

         Assert.assertEquals(testMessage, ((Status) (res.get(1))).getText());
         Assert.assertEquals(-1, ((Status) (res.get(1))).getInReplyToStatusId());
         Assert.assertEquals(replyMessage, ((Status) (res.get(0))).getText());
         Assert.assertEquals(s.getId(), ((Status) (res.get(0))).getInReplyToStatusId());
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable t)
         {
         }
         try
         {
            locator.close();
         }
         catch (Throwable t)
         {
         }
         try
         {
            server0.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }
}
