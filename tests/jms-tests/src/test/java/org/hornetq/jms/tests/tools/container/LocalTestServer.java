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
package org.hornetq.jms.tests.tools.container;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.api.jms.management.JMSQueueControl;
import org.hornetq.api.jms.management.TopicControl;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.jms.tests.JmsTestLogger;
import org.hornetq.spi.core.security.HornetQSecurityManagerImpl;
import org.jnp.server.Main;
import org.jnp.server.NamingBeanImpl;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.1</tt>
 *          <p/>
 *          LocalTestServer.java,v 1.1 2006/02/21 08:25:32 timfox Exp
 */
public class LocalTestServer implements Server, Runnable
{
   // Constants ------------------------------------------------------------------------------------

   private boolean started = false;

   private final HashMap<String, List<String>> allBindings = new HashMap<String, List<String>>();
   private Main jndiServer;
   private JMSServerManagerImpl jmsServerManager;

   // Static ---------------------------------------------------------------------------------------

   public static void setEnvironmentServerIndex(final int serverIndex)
   {
      System.setProperty(Constants.SERVER_INDEX_PROPERTY_NAME, Integer.toString(serverIndex));
   }

   public static void clearEnvironmentServerIndex()
   {
      System.getProperty(Constants.SERVER_INDEX_PROPERTY_NAME, null);
   }

   // Attributes -----------------------------------------------------------------------------------

   private final int serverIndex;

   // Constructors ---------------------------------------------------------------------------------

   public LocalTestServer()
   {
      super();

      serverIndex = 0;
   }

   // Server implementation ------------------------------------------------------------------------

   public int getServerID()
   {
      return serverIndex;
   }

   public synchronized void start(final HashMap<String, Object> configuration,
                                  final boolean clearJournal) throws Exception
   {
      if (isStarted())
      {
         return;
      }

      if (clearJournal)
      {
         // Delete the Journal environment

         File dir = new File("target/data");

         boolean deleted = LocalTestServer.deleteDirectory(dir);

         JmsTestLogger.LOGGER.info("Deleted dir: " + dir.getAbsolutePath() + " deleted: " + deleted);
      }

      org.jnp.server.NamingBeanImpl namingBean = new NamingBeanImpl();
      jndiServer = new Main();
      jndiServer.setNamingInfo(namingBean);
      jndiServer.setPort(1099);
      jndiServer.setBindAddress("localhost");
      jndiServer.setRmiPort(1098);
      jndiServer.setRmiBindAddress("localhost");

      javax.management.MBeanServer beanServer = java.lang.management.ManagementFactory.getPlatformMBeanServer();
      FileConfiguration fileConfiguration = new FileConfiguration();
      HornetQSecurityManagerImpl securityManager = new HornetQSecurityManagerImpl();
      HornetQServerImpl hornetQServer = new HornetQServerImpl(fileConfiguration, beanServer, securityManager);
      jmsServerManager = new JMSServerManagerImpl(hornetQServer);
      System.setProperty(Constants.SERVER_INDEX_PROPERTY_NAME, "" + getServerID());

      namingBean.start();
      jndiServer.start();
      fileConfiguration.start();
      jmsServerManager.start();
      started = true;

   }

   private static boolean deleteDirectory(final File directory)
   {
      if (directory.isDirectory())
      {
         String[] files = directory.list();

         for (int j = 0; j < files.length; j++)
         {
            if (!LocalTestServer.deleteDirectory(new File(directory, files[j])))
            {
               return false;
            }
         }
      }

      return directory.delete();
   }

   public synchronized boolean stop() throws Exception
   {
      jmsServerManager.stop();
      jndiServer.stop();
      started = false;
      unbindAll();
      jmsServerManager = null;
      jndiServer.stop();
      return true;
   }

   public void ping() throws Exception
   {
      if (!isStarted())
      {
         throw new RuntimeException("ok");
      }
   }

   public synchronized void kill() throws Exception
   {
      stop();
   }

   public synchronized boolean isStarted() throws Exception
   {
      return started;
   }

   public synchronized void startServerPeer() throws Exception
   {
      System.setProperty(Constants.SERVER_INDEX_PROPERTY_NAME, "" + getServerID());
      getHornetQServer().start();
   }

   public synchronized void stopServerPeer() throws Exception
   {
      System.setProperty(Constants.SERVER_INDEX_PROPERTY_NAME, "" + getServerID());
      getHornetQServer().stop();
      // also unbind everything
      unbindAll();
   }

   private void unbindAll() throws Exception
   {
      Collection<List<String>> bindings = allBindings.values();
      for (List<String> binding : bindings)
      {
         for (String s : binding)
         {
            getInitialContext().unbind(s);
         }
      }
   }

   /**
    * Only for in-VM use!
    */
   public HornetQServer getServerPeer()
   {
      return getHornetQServer();
   }

   public void destroyQueue(final String name, final String jndiName) throws Exception
   {
      getJMSServerManager().destroyQueue(name);
   }

   public void destroyTopic(final String name, final String jndiName) throws Exception
   {
      getJMSServerManager().destroyTopic(name);
   }

   public void createQueue(final String name, final String jndiName) throws Exception
   {
      getJMSServerManager().createQueue(true, name, null, true, "/queue/" + (jndiName != null ? jndiName : name));
   }

   public void createTopic(final String name, final String jndiName) throws Exception
   {
      getJMSServerManager().createTopic(true, name, "/topic/" + (jndiName != null ? jndiName : name));
   }

   public void deployConnectionFactory(final String clientId, final String objectName, final String ... jndiBindings) throws Exception
   {
      deployConnectionFactory(clientId, objectName, -1, -1, -1, -1, false, false, -1, false, jndiBindings);
   }

   public void deployConnectionFactory(final String objectName,
                                       final int consumerWindowSize,
                                       final String ... jndiBindings) throws Exception
   {
      deployConnectionFactory(null, objectName, consumerWindowSize, -1, -1, -1, false, false, -1, false, jndiBindings);
   }

   public void deployConnectionFactory(final String objectName, final String ... jndiBindings) throws Exception
   {
      deployConnectionFactory(null, objectName, -1, -1, -1, -1, false, false, -1, false, jndiBindings);
   }

   public void deployConnectionFactory(final String objectName,
                                       final int prefetchSize,
                                       final int defaultTempQueueFullSize,
                                       final int defaultTempQueuePageSize,
                                       final int defaultTempQueueDownCacheSize,
                                       final String ... jndiBindings) throws Exception
   {
      this.deployConnectionFactory(null,
                                   objectName,
                                   prefetchSize,
                                   defaultTempQueueFullSize,
                                   defaultTempQueuePageSize,
                                   defaultTempQueueDownCacheSize,
                                   false,
                                   false,
                                   -1,
                                   false,
                                   jndiBindings);
   }

   public void deployConnectionFactory(final String objectName,
                                       final boolean supportsFailover,
                                       final boolean supportsLoadBalancing,
                                       final String ... jndiBindings) throws Exception
   {
      this.deployConnectionFactory(null,
                                   objectName,
                                   -1,
                                   -1,
                                   -1,
                                   -1,
                                   supportsFailover,
                                   supportsLoadBalancing,
                                   -1,
                                   false,
                                   jndiBindings);
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
                                       final String ... jndiBindings) throws Exception
   {
      List<TransportConfiguration> connectorConfigs = new ArrayList<TransportConfiguration>();
      connectorConfigs.add(new TransportConfiguration(NettyConnectorFactory.class.getName()));

      ArrayList<String> connectors = new ArrayList<String>();
      connectors.add("netty");

      getJMSServerManager().createConnectionFactory(objectName,
                                                    false,
                                                    JMSFactoryType.CF,
                                                    connectors,
                                                    clientId,
                                                    HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                    HornetQClient.DEFAULT_CONNECTION_TTL,
                                                    HornetQClient.DEFAULT_CALL_TIMEOUT,
                                                    HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                                                    HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                                    HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                    HornetQClient.DEFAULT_COMPRESS_LARGE_MESSAGES,
                                                    prefetchSize,
                                                    HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                                                    HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
                                                    HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                                                    blockOnAcknowledge,
                                                    true,
                                                    true,
                                                    HornetQClient.DEFAULT_AUTO_GROUP,
                                                    HornetQClient.DEFAULT_PRE_ACKNOWLEDGE,
                                                    HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                    HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                                    dupsOkBatchSize,
                                                    HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                                                    HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                                    HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                                                    HornetQClient.DEFAULT_RETRY_INTERVAL,
                                                    HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                    HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                    HornetQClient.DEFAULT_RECONNECT_ATTEMPTS,
                                                    HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
                                                    null,
                                                    jndiBindings);
   }

   public void undeployConnectionFactory(final String objectName) throws Exception
   {
      getJMSServerManager().destroyConnectionFactory(objectName);
   }

   public void configureSecurityForDestination(final String destName, final boolean isQueue, final Set<Role> roles) throws Exception
   {
      String destination = (isQueue ? "jms.queue." : "jms.topic.") + destName;
      if (roles != null)
      {
         getHornetQServer().getSecurityRepository().addMatch(destination, roles);
      }
      else
      {
         getHornetQServer().getSecurityRepository().removeMatch(destination);
      }
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   public HornetQServer getHornetQServer()
   {
      return jmsServerManager.getHornetQServer();
   }

   public JMSServerManager getJMSServerManager()
   {
      return jmsServerManager;
   }

   public InitialContext getInitialContext() throws Exception
   {
      Properties props = new Properties();
      props.setProperty("java.naming.factory.initial",
                        "org.hornetq.jms.tests.tools.container.InVMInitialContextFactory");
      props.setProperty(Constants.SERVER_INDEX_PROPERTY_NAME, "" + getServerID());
      // props.setProperty("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
      return new InitialContext(props);
   }

   public void run()
   {
    //  bootstrap.run();

      started = true;

      synchronized (this)
      {
         notify();
         try
         {
            wait();
         }
         catch (InterruptedException e)
         {
            // e.printStackTrace();
         }
      }

   }

   @Override
   public Long getMessageCountForQueue(final String queueName) throws Exception
   {
      JMSQueueControl queue = (JMSQueueControl)getHornetQServer().getManagementService()
                                                                 .getResource(ResourceNames.JMS_QUEUE + queueName);
      if (queue != null)
      {
         queue.flushExecutor();
         return queue.getMessageCount();
      }
      else
      {
         return -1L;
      }
   }

   public void removeAllMessages(final String destination, final boolean isQueue) throws Exception
   {
      if (isQueue)
      {
         JMSQueueControl queue = (JMSQueueControl)getHornetQServer().getManagementService()
                                                                    .getResource(ResourceNames.JMS_QUEUE + destination);
         queue.removeMessages(null);
      }
      else
      {
         TopicControl topic = (TopicControl)getHornetQServer().getManagementService()
                                                              .getResource(ResourceNames.JMS_TOPIC + destination);
         topic.removeMessages(null);
      }
   }

   public List<String> listAllSubscribersForTopic(final String s) throws Exception
   {
      ObjectName objectName = ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(s);
      TopicControl topic = MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                         objectName,
                                                                         TopicControl.class,
                                                                         false);
      Object[] subInfos = topic.listAllSubscriptions();
      List<String> subs = new ArrayList<String>();
      for (Object o : subInfos)
      {
         Object[] data = (Object[])o;
         subs.add((String)data[2]);
      }
      return subs;
   }

   public Set<Role> getSecurityConfig() throws Exception
   {
      return getHornetQServer().getSecurityRepository().getMatch("*");
   }

   public void setSecurityConfig(final Set<Role> defConfig) throws Exception
   {
      getHornetQServer().getSecurityRepository().removeMatch("#");
      getHornetQServer().getSecurityRepository().addMatch("#", defConfig);
   }

   // Inner classes --------------------------------------------------------------------------------

}
