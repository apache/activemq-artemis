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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;
import javax.jms.XAConnectionFactory;
import javax.naming.InitialContext;
import javax.transaction.TransactionManager;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.core.postoffice.Binding;
import org.apache.activemq6.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq6.core.security.Role;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.jms.server.JMSServerManager;
import org.apache.activemq6.jms.tests.tools.ServerManagement;
import org.apache.activemq6.jms.tests.tools.container.Server;
import org.apache.activemq6.jms.tests.util.ProxyAssertSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.org">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @deprecated this infrastructure should not be used for new code. New tests should go into
 * org.apache.activemq6.tests.integration.jms at the integration-tests project.
 */
@Deprecated
public abstract class HornetQServerTestCase
{
   public static final int MAX_TIMEOUT = 1000 * 10 /* seconds */;

   public static final int MIN_TIMEOUT = 1000 * 1 /* seconds */;

   private static final int DRAIN_WAIT_TIME = 250;
   protected final JmsTestLogger log = JmsTestLogger.LOGGER;

   /**
    * Some testcases are time sensitive, and we need to make sure a GC would happen before certain scenarios
    */
   public static void forceGC()
   {
      WeakReference<Object> dumbReference = new WeakReference<Object>(new Object());
      // A loop that will wait GC, using the minimal time as possible
      while (dumbReference.get() != null)
      {
         System.gc();
         try
         {
            Thread.sleep(500);
         }
         catch (InterruptedException e)
         {
         }
      }
   }

   protected static List<Server> servers = new ArrayList<Server>();

   protected static Topic topic1;
   protected static Topic topic2;
   protected static Topic topic3;

   protected Queue queue1;
   protected Queue queue2;
   protected Queue queue3;
   protected Queue queue4;

   private final Set<Connection> connectionsSet = new HashSet<Connection>();
   private final Set<JMSContext> contextSet = new HashSet<JMSContext>();

   @Rule
   public TestRule watcher = new TestWatcher()
   {
      @Override
      protected void starting(Description description)
      {
         log.info(String.format("#*#*# Starting test: %s()...", description.getMethodName()));
      }

      @Override
      protected void finished(Description description)
      {
         log.info(String.format("#*#*# Finished test: %s()...", description.getMethodName()));
      }

      @Override
      protected void failed(Throwable e, Description description)
      {
         HornetQServerTestCase.tearDownAllServers();
      }
   };

   @Before
   public void setUp() throws Exception
   {
      System.setProperty("java.naming.factory.initial", getContextFactory());

      try
      {
         // create any new server we need
         HornetQServerTestCase.servers.add(ServerManagement.create());

         // start the servers if needed
         if (!HornetQServerTestCase.servers.get(0).isStarted())
         {
            HornetQServerTestCase.servers.get(0).start(getConfiguration(), true);
         }
         // deploy the objects for this test
         deployAdministeredObjects();
         lookUp();
      }
      catch (Exception e)
      {
         // if we get here we need to clean up for the next test
         e.printStackTrace();
         HornetQServerTestCase.servers.get(0).stop();
         throw e;
      }
      // empty the queues
      checkEmpty(queue1);
      checkEmpty(queue2);
      checkEmpty(queue3);
      checkEmpty(queue4);

      // Check no subscriptions left lying around

      checkNoSubscriptions(topic1);
      checkNoSubscriptions(topic2);
      checkNoSubscriptions(topic3);
   }

   @After
   public void tearDown() throws Exception
   {
      for (JMSContext context : contextSet)
      {
         context.close();
      }
      contextSet.clear();

      for (Connection localConn : connectionsSet)
      {
         localConn.close();
      }
      connectionsSet.clear();
   }

   public void stop() throws Exception
   {
      HornetQServerTestCase.servers.get(0).stop();
   }

   public String getContextFactory()
   {
      return org.apache.activemq6.jms.tests.tools.container.InVMInitialContextFactory.class.getCanonicalName();
   }

   public void start() throws Exception
   {
      System.setProperty("java.naming.factory.initial", getContextFactory());
      HornetQServerTestCase.servers.get(0).start(getConfiguration(), false);
   }

   public void startNoDelete() throws Exception
   {
      System.setProperty("java.naming.factory.initial", getContextFactory());
      HornetQServerTestCase.servers.get(0).start(getConfiguration(), false);
   }

   public void stopServerPeer() throws Exception
   {
      HornetQServerTestCase.servers.get(0).stopServerPeer();
   }

   public void startServerPeer() throws Exception
   {
      System.setProperty("java.naming.factory.initial", getContextFactory());
      HornetQServerTestCase.servers.get(0).startServerPeer();
   }

   protected HashMap<String, Object> getConfiguration()
   {
      return new HashMap<String, Object>();
   }

   protected void deployAndLookupAdministeredObjects() throws Exception
   {
      createTopic("Topic1");
      createTopic("Topic2");
      createTopic("Topic3");
      createQueue("Queue1");
      createQueue("Queue2");
      createQueue("Queue3");
      createQueue("Queue4");

      lookUp();
   }

   protected void deployAdministeredObjects() throws Exception
   {
      createTopic("Topic1");
      createTopic("Topic2");
      createTopic("Topic3");
      createQueue("Queue1");
      createQueue("Queue2");
      createQueue("Queue3");
      createQueue("Queue4");
   }

   private void lookUp() throws Exception
   {
      InitialContext ic = getInitialContext();
      HornetQServerTestCase.topic1 = (Topic) ic.lookup("/topic/Topic1");
      HornetQServerTestCase.topic2 = (Topic) ic.lookup("/topic/Topic2");
      HornetQServerTestCase.topic3 = (Topic) ic.lookup("/topic/Topic3");
      queue1 = (Queue) ic.lookup("/queue/Queue1");
      queue2 = (Queue) ic.lookup("/queue/Queue2");
      queue3 = (Queue) ic.lookup("/queue/Queue3");
      queue4 = (Queue) ic.lookup("/queue/Queue4");
   }

   protected void undeployAdministeredObjects() throws Exception
   {
      removeAllMessages("Topic1", false);
      removeAllMessages("Topic2", false);
      removeAllMessages("Topic3", false);
      removeAllMessages("Queue1", true);
      removeAllMessages("Queue2", true);
      removeAllMessages("Queue3", true);
      removeAllMessages("Queue4", true);

      destroyTopic("Topic1");
      destroyTopic("Topic2");
      destroyTopic("Topic3");
      destroyQueue("Queue1");
      destroyQueue("Queue2");
      destroyQueue("Queue3");
      destroyQueue("Queue4");
   }

   @AfterClass
   public static final void tearDownAllServers()
   {
      for (Server s : servers)
      {
         try
         {
            s.stop();
         }
         catch (Exception cause)
         {
            // ignore
         }
      }
      servers.clear();
   }

   protected HornetQServer getJmsServer() throws Exception
   {
      return HornetQServerTestCase.servers.get(0).getHornetQServer();
   }

   protected JMSServerManager getJmsServerManager() throws Exception
   {
      return HornetQServerTestCase.servers.get(0).getJMSServerManager();
   }

   protected void checkNoSubscriptions(final Topic topic) throws Exception
   {

   }

   protected void drainDestination(final ConnectionFactory cf, final Destination dest) throws JMSException
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection();
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(dest);
         Message m = null;
         conn.start();
         log.trace("Draining messages from " + dest);
         while (true)
         {
            m = cons.receive(DRAIN_WAIT_TIME);
            if (m == null)
            {
               break;
            }
            log.trace("Drained message");
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public InitialContext getInitialContext() throws Exception
   {
      return new InitialContext(ServerManagement.getJNDIEnvironment(0));
   }

   public ConnectionFactory getConnectionFactory() throws Exception
   {
      return (ConnectionFactory) getInitialContext().lookup("/ConnectionFactory");
   }

   public TopicConnectionFactory getTopicConnectionFactory() throws Exception
   {
      return (TopicConnectionFactory) getInitialContext().lookup("/CF_TOPIC");
   }

   public XAConnectionFactory getXAConnectionFactory() throws Exception
   {
      return (XAConnectionFactory) getInitialContext().lookup("/CF_XA_TRUE");
   }

   public InitialContext getInitialContext(final int serverid) throws Exception
   {
      return new InitialContext(ServerManagement.getJNDIEnvironment(serverid));
   }

   protected TransactionManager getTransactionManager()
   {
      return new TransactionManagerImple();
   }

   public void configureSecurityForDestination(final String destName, final boolean isQueue, final HashSet<Role> roles) throws Exception
   {
      HornetQServerTestCase.servers.get(0).configureSecurityForDestination(destName, isQueue, roles);
   }

   public void createQueue(final String name) throws Exception
   {
      HornetQServerTestCase.servers.get(0).createQueue(name, null);
   }

   public void createTopic(final String name) throws Exception
   {
      HornetQServerTestCase.servers.get(0).createTopic(name, null);
   }

   public void destroyQueue(final String name) throws Exception
   {
      HornetQServerTestCase.servers.get(0).destroyQueue(name, null);
   }

   public void destroyTopic(final String name) throws Exception
   {
      HornetQServerTestCase.servers.get(0).destroyTopic(name, null);
   }

   public void createQueue(final String name, final int i) throws Exception
   {
      HornetQServerTestCase.servers.get(i).createQueue(name, null);
   }

   public void createTopic(final String name, final int i) throws Exception
   {
      HornetQServerTestCase.servers.get(i).createTopic(name, null);
   }

   public void destroyQueue(final String name, final int i) throws Exception
   {
      HornetQServerTestCase.servers.get(i).destroyQueue(name, null);
   }

   public boolean checkNoMessageData()
   {
      return false;
   }

   public boolean checkEmpty(final Queue queue) throws Exception
   {
      Long messageCount = HornetQServerTestCase.servers.get(0).getMessageCountForQueue(queue.getQueueName());
      if (messageCount > 0)
      {
         removeAllMessages(queue.getQueueName(), true);
      }
      return true;
   }

   public boolean checkEmpty(final Queue queue, final int i)
   {
      return true;
   }

   public boolean checkEmpty(final Topic topic)
   {
      return true;
   }

   protected void removeAllMessages(final String destName, final boolean isQueue) throws Exception
   {
      HornetQServerTestCase.servers.get(0).removeAllMessages(destName, isQueue);
   }

   protected boolean assertRemainingMessages(final int expected) throws Exception
   {
      String queueName = "Queue1";
      Binding binding = servers.get(0).getHornetQServer().getPostOffice().getBinding(SimpleString.toSimpleString("jms.queue." + queueName));
      if (binding != null && binding instanceof LocalQueueBinding)
      {
         ((LocalQueueBinding)binding).getQueue().flushExecutor();
      }
      Long messageCount = HornetQServerTestCase.servers.get(0).getMessageCountForQueue(queueName);

      ProxyAssertSupport.assertEquals(expected, messageCount.intValue());
      return expected == messageCount.intValue();
   }

   protected static void assertActiveConnectionsOnTheServer(final int expectedSize) throws Exception
   {
      ProxyAssertSupport.assertEquals(expectedSize, HornetQServerTestCase.servers.get(0)
         .getHornetQServer()
         .getHornetQServerControl()
         .getConnectionCount());
   }

   public static void deployConnectionFactory(final String clientId,
                                              final String objectName,
                                              final String... jndiBindings) throws Exception
   {
      HornetQServerTestCase.servers.get(0).deployConnectionFactory(clientId, objectName, jndiBindings);
   }

   public static void deployConnectionFactory(final String objectName,
                                              final int prefetchSize,
                                              final String... jndiBindings) throws Exception
   {
      HornetQServerTestCase.servers.get(0).deployConnectionFactory(objectName, prefetchSize, jndiBindings);
   }


   public static void deployConnectionFactory(final int server,
                                              final String objectName,
                                              final int prefetchSize,
                                              final String... jndiBindings) throws Exception
   {
      HornetQServerTestCase.servers.get(server).deployConnectionFactory(objectName, prefetchSize, jndiBindings);
   }

   public static void deployConnectionFactory(final int server, final String objectName, final String... jndiBindings) throws Exception
   {
      HornetQServerTestCase.servers.get(server).deployConnectionFactory(objectName, jndiBindings);
   }

   public void deployConnectionFactory(final String clientId,
                                       final String objectName,
                                       final int prefetchSize,
                                       final int defaultTempQueueFullSize,
                                       final int defaultTempQueuePageSize,
                                       final int defaultTempQueueDownCacheSize,
                                       final boolean supportsFailover,
                                       final boolean supportsLoadBalancing,
                                       final int dupsOkBatchSize,
                                       final boolean blockOnAcknowledge,
                                       final String... jndiBindings) throws Exception
   {
      HornetQServerTestCase.servers.get(0).deployConnectionFactory(clientId,
                                                                   objectName,
                                                                   prefetchSize,
                                                                   defaultTempQueueFullSize,
                                                                   defaultTempQueuePageSize,
                                                                   defaultTempQueueDownCacheSize,
                                                                   supportsFailover,
                                                                   supportsLoadBalancing,
                                                                   dupsOkBatchSize,
                                                                   blockOnAcknowledge,
                                                                   jndiBindings);
   }

   public static void deployConnectionFactory(final String objectName,
                                              final int prefetchSize,
                                              final int defaultTempQueueFullSize,
                                              final int defaultTempQueuePageSize,
                                              final int defaultTempQueueDownCacheSize,
                                              final String... jndiBindings) throws Exception
   {
      HornetQServerTestCase.servers.get(0).deployConnectionFactory(objectName,
                                                                   prefetchSize,
                                                                   defaultTempQueueFullSize,
                                                                   defaultTempQueuePageSize,
                                                                   defaultTempQueueDownCacheSize,
                                                                   jndiBindings);
   }

   public static void undeployConnectionFactory(final String objectName) throws Exception
   {
      HornetQServerTestCase.servers.get(0).undeployConnectionFactory(objectName);
   }

   protected List<String> listAllSubscribersForTopic(final String s) throws Exception
   {
      return HornetQServerTestCase.servers.get(0).listAllSubscribersForTopic(s);
   }

   protected Long getMessageCountForQueue(final String s) throws Exception
   {
      return HornetQServerTestCase.servers.get(0).getMessageCountForQueue(s);
   }

   protected Set<Role> getSecurityConfig() throws Exception
   {
      return HornetQServerTestCase.servers.get(0).getSecurityConfig();
   }

   protected void setSecurityConfig(final Set<Role> defConfig) throws Exception
   {
      HornetQServerTestCase.servers.get(0).setSecurityConfig(defConfig);
   }

   protected void setSecurityConfigOnManager(final String destination, final boolean isQueue, final Set<Role> roles) throws Exception
   {
      HornetQServerTestCase.servers.get(0).configureSecurityForDestination(destination, isQueue, roles);
   }

   protected final JMSContext addContext(JMSContext createContext)
   {
      contextSet.add(createContext);
      return createContext;
   }

   protected final Connection addConnection(Connection conn)
   {
      connectionsSet.add(conn);
      return conn;
   }
}
