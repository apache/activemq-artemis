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
package org.apache.activemq.artemis.tests.util;

import javax.naming.Context;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.asyncio.impl.AsynchronousFileImpl;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.client.impl.Topology;
import org.apache.activemq.artemis.core.client.impl.TopologyMemberImpl;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.journal.impl.JournalReaderCallback;
import org.apache.activemq.artemis.core.journal.impl.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMRegistry;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.core.server.cluster.RemoteQueueBinding;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.impl.Activation;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.core.server.impl.SharedNothingBackupActivation;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl;
import org.apache.activemq.artemis.utils.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Base class with basic utilities on starting up a basic server
 */
public abstract class ActiveMQTestBase extends Assert
{
   public static final String TARGET_TMP = "./target/tmp";
   public static final String INVM_ACCEPTOR_FACTORY = InVMAcceptorFactory.class.getCanonicalName();
   public static final String INVM_CONNECTOR_FACTORY = InVMConnectorFactory.class.getCanonicalName();
   public static final String NETTY_ACCEPTOR_FACTORY = NettyAcceptorFactory.class.getCanonicalName();
   public static final String NETTY_CONNECTOR_FACTORY = NettyConnectorFactory.class.getCanonicalName();
   public static final String CLUSTER_PASSWORD = "UnitTestsClusterPassword";

   /**
    * Add a "sendCallNumber" property to messages sent using helper classes. Meant to help in
    * debugging.
    */
   private static final String SEND_CALL_NUMBER = "sendCallNumber";
   private static final String OS_TYPE = System.getProperty("os.name").toLowerCase();
   private static final int DEFAULT_UDP_PORT;
   private static final ActiveMQServerLogger log = ActiveMQServerLogger.LOGGER;

   protected static final long WAIT_TIMEOUT = 30000;

   // There is a verification about thread leakages. We only fail a single thread when this happens
   private static Set<Thread> alreadyFailedThread = new HashSet<Thread>();

   private final Collection<ActiveMQServer> servers = new ArrayList<ActiveMQServer>();
   private final Collection<ServerLocator> locators = new ArrayList<ServerLocator>();
   private final Collection<ClientSessionFactory> sessionFactories = new ArrayList<ClientSessionFactory>();
   private final Collection<ClientSession> clientSessions = new HashSet<ClientSession>();
   private final Collection<ClientConsumer> clientConsumers = new HashSet<ClientConsumer>();
   private final Collection<ClientProducer> clientProducers = new HashSet<ClientProducer>();
   private final Collection<ActiveMQComponent> otherComponents = new HashSet<ActiveMQComponent>();
   private final Set<ExecutorService> executorSet = new HashSet<ExecutorService>();

   private boolean checkThread = true;
   private String testDir;
   private int sendMsgCount = 0;
   private Map<Thread, StackTraceElement[]> previousThreads;

   @Rule
   public TestName name = new TestName();

   @Rule
   public TemporaryFolder temporaryFolder;

   @Rule
   // This Custom rule will remove any files under ./target/tmp
   // including anything created previously by TemporaryFolder
   public RemoveFolder folder = new RemoveFolder(TARGET_TMP);

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
   };

   static
   {
      Random random = new Random();
      DEFAULT_UDP_PORT = 6000 + random.nextInt(1000);
   }

   public ActiveMQTestBase()
   {
      File parent = new File(TARGET_TMP);
      parent.mkdirs();
      temporaryFolder = new TemporaryFolder(parent);
   }

   @After
   public void tearDown() throws Exception
   {
      for (ExecutorService s : executorSet)
      {
         s.shutdown();
      }
      closeAllSessionFactories();
      closeAllServerLocatorsFactories();

      try
      {
         assertAllExecutorsFinished();
         assertAllClientConsumersAreClosed();
         assertAllClientProducersAreClosed();
         assertAllClientSessionsAreClosed();
      }
      finally
      {
         synchronized (servers)
         {
            for (ActiveMQServer server : servers)
            {
               if (server == null)
               {
                  continue;
               }
               try
               {
                  final ClusterManager clusterManager = server.getClusterManager();
                  if (clusterManager != null)
                  {
                     for (ClusterConnection cc : clusterManager.getClusterConnections())
                     {
                        stopComponent(cc);
                     }
                  }
               }
               catch (Exception e)
               {
                  // no-op
               }
               stopComponentOutputExceptions(server);
            }
            servers.clear();
         }

         closeAllOtherComponents();

         ArrayList<Exception> exceptions;
         try
         {
            exceptions = checkCsfStopped();
         }
         finally
         {
            cleanupPools();
         }
         //clean up pools before failing
         if (!exceptions.isEmpty())
         {
            for (Exception exception : exceptions)
            {
               exception.printStackTrace();
            }
            fail("Client Session Factories still trying to reconnect, see above to see where created");
         }
         Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
         for (Thread thread : threadMap.keySet())
         {
            StackTraceElement[] stack = threadMap.get(thread);
            for (StackTraceElement stackTraceElement : stack)
            {
               if (stackTraceElement.getMethodName().contains("getConnectionWithRetry") && !alreadyFailedThread.contains(thread))
               {
                  alreadyFailedThread.add(thread);
                  System.out.println(threadDump(this.getName() + " has left threads running. Look at thread " +
                                                        thread.getName() +
                                                        " id = " +
                                                        thread.getId() +
                                                        " has running locators on test " +
                                                        this.getName() +
                                                        " on this following dump"));
                  fail("test '" + getName() + "' left serverlocator running, this could effect other tests");
               }
               else if (stackTraceElement.getMethodName().contains("BroadcastGroupImpl.run") && !alreadyFailedThread.contains(thread))
               {
                  alreadyFailedThread.add(thread);
                  System.out.println(threadDump(this.getName() + " has left threads running. Look at thread " +
                                                        thread.getName() +
                                                        " id = " +
                                                        thread.getId() +
                                                        " is still broadcasting " +
                                                        this.getName() +
                                                        " on this following dump"));
                  fail("test left broadcastgroupimpl running, this could effect other tests");
               }
            }
         }

         if (checkThread)
         {
            StringBuffer buffer = null;

            boolean failed = true;


            long timeout = System.currentTimeMillis() + 60000;
            while (failed && timeout > System.currentTimeMillis())
            {
               buffer = new StringBuffer();

               failed = checkThread(buffer);

               if (failed)
               {
                  forceGC();
                  Thread.sleep(500);
                  log.info("There are still threads running, trying again");
                  System.out.println(buffer);
               }
            }

            if (failed)
            {
               logAndSystemOut("Thread leaked on test " + this.getClass().getName() + "::" + this.getName() + "\n" +
                                       buffer);
               logAndSystemOut("Thread leakage");

               fail("Thread leaked");
            }

         }
         else
         {
            checkThread = true;
         }

         if (Thread.currentThread().getContextClassLoader() == null)
         {
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
            fail("Thread Context ClassLoader was set to null at some point before this test. We will set to this.getClass().getClassLoader(), but you are supposed to fix your tests");
         }

         checkFilesUsage();
      }

      if (InVMRegistry.instance.size() > 0)
      {
         fail("InVMREgistry size > 0");
      }
   }

   @Before
   public void setUp() throws Exception
   {
      sendMsgCount = 0;
      testDir = temporaryFolder.getRoot().getAbsolutePath();
      clearDataRecreateServerDirs();
      OperationContextImpl.clearContext();

      InVMRegistry.instance.clear();

      // checkFreePort(TransportConstants.DEFAULT_PORT);

      previousThreads = Thread.getAllStackTraces();

      logAndSystemOut("#test " + getName());
   }

   public static void assertEqualsByteArrays(final byte[] expected, final byte[] actual)
   {
      for (int i = 0; i < expected.length; i++)
      {
         Assert.assertEquals("byte at index " + i, expected[i], actual[i]);
      }
   }

   /**
    *
    * @param str
    * @param sub
    * @return
    */
   public static int countOccurrencesOf(String str, String sub)
   {
      if (str == null || sub == null || str.length() == 0 || sub.length() == 0)
      {
         return 0;
      }
      int count = 0;
      int pos = 0;
      int idx;
      while ((idx = str.indexOf(sub, pos)) != -1)
      {
         ++count;
         pos = idx + sub.length();
      }
      return count;
   }

   protected void disableCheckThread()
   {
      checkThread = false;
   }

   protected String getName()
   {
      return name.getMethodName();
   }

   protected boolean isWindows()
   {
      return (OS_TYPE.indexOf("win") >= 0);
   }

   protected Configuration createDefaultInVMConfig() throws Exception
   {
      return createDefaultConfig(0, false);
   }

   protected Configuration createDefaultInVMConfig(final int serverID) throws Exception
   {
      return createDefaultConfig(serverID, false);
   }

   protected Configuration createDefaultNettyConfig() throws Exception
   {
      return createDefaultConfig(0, true);
   }

   protected Configuration createDefaultConfig(final boolean netty) throws Exception
   {
      return createDefaultConfig(0, netty);
   }

   protected Configuration createDefaultConfig(final int serverID, final boolean netty) throws Exception
   {
      ConfigurationImpl configuration = createBasicConfig(serverID)
              .setJMXManagementEnabled(false)
              .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, generateInVMParams(serverID)));

      if (netty)
      {
         configuration.addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY));
      }

      return configuration;
   }

   private Configuration createDefaultConfig(final int index,
                                               final Map<String, Object> params,
                                               final String... acceptors)
   {
      Configuration configuration = createBasicConfig(index);

      for (String acceptor : acceptors)
      {
         TransportConfiguration transportConfig = new TransportConfiguration(acceptor, params);
         configuration.getAcceptorConfigurations().add(transportConfig);
      }

      return configuration;
   }

   protected ConfigurationImpl createBasicConfig() throws Exception
   {
      return createBasicConfig(-1);
   }

   /**
    * @param serverID
    * @return
    * @throws Exception
    */
   protected ConfigurationImpl createBasicConfig(final int serverID)
   {
      ConfigurationImpl configuration = new ConfigurationImpl()
              .setSecurityEnabled(false)
              .setJournalMinFiles(2)
              .setJournalFileSize(100 * 1024)
              .setJournalType(getDefaultJournalType())
              .setJournalDirectory(getJournalDir(serverID, false))
              .setBindingsDirectory(getBindingsDir(serverID, false))
              .setPagingDirectory(getPageDir(serverID, false))
              .setLargeMessagesDirectory(getLargeMessagesDir(serverID, false))
              .setJournalCompactMinFiles(0)
              .setJournalCompactPercentage(0)
              .setClusterPassword(CLUSTER_PASSWORD);

      return configuration;
   }

   protected Map<String, Object> generateInVMParams(final int node)
   {
      Map<String, Object> params = new HashMap<String, Object>();

      params.put(org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, node);

      return params;
   }

   protected static final ClusterConnectionConfiguration basicClusterConnectionConfig(String connectorName, String... connectors)
   {
      ArrayList<String> connectors0 = new ArrayList<>();
      for (String c : connectors)
      {
         connectors0.add(c);
      }
      ClusterConnectionConfiguration clusterConnectionConfiguration = new ClusterConnectionConfiguration()
              .setName("cluster1")
              .setAddress("jms")
              .setConnectorName(connectorName)
              .setRetryInterval(1000)
              .setDuplicateDetection(false)
              .setMaxHops(1)
              .setConfirmationWindowSize(1)
              .setMessageLoadBalancingType(MessageLoadBalancingType.STRICT)
              .setStaticConnectors(connectors0);

      return clusterConnectionConfiguration;
   }

   protected final OrderedExecutorFactory getOrderedExecutor()
   {
      final ExecutorService executor = Executors.newCachedThreadPool();
      executorSet.add(executor);
      return new OrderedExecutorFactory(executor);
   }

   protected static String getUDPDiscoveryAddress()
   {
      return System.getProperty("TEST-UDP-ADDRESS", "230.1.2.3");
   }

   protected static String getUDPDiscoveryAddress(final int variant)
   {
      String value = getUDPDiscoveryAddress();

      int posPoint = value.lastIndexOf('.');

      int last = Integer.valueOf(value.substring(posPoint + 1));

      return value.substring(0, posPoint + 1) + (last + variant);
   }

   public static int getUDPDiscoveryPort()
   {
      String port = System.getProperty("TEST-UDP-PORT");
      if (port != null)
      {
         return Integer.parseInt(port);
      }
      return DEFAULT_UDP_PORT;
   }

   public static int getUDPDiscoveryPort(final int variant)
   {
      return getUDPDiscoveryPort() + variant;
   }

   public static JournalType getDefaultJournalType()
   {
      if (AsynchronousFileImpl.isLoaded())
      {
         return JournalType.ASYNCIO;
      }
      else
      {
         return JournalType.NIO;
      }
   }

   public static void forceGC()
   {
      log.info("#test forceGC");
      WeakReference<Object> dumbReference = new WeakReference<Object>(new Object());
      // A loop that will wait GC, using the minimal time as possible
      while (dumbReference.get() != null)
      {
         System.gc();
         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
         }
      }
      log.info("#test forceGC Done");
   }

   public static void forceGC(final Reference<?> ref, final long timeout)
   {
      long waitUntil = System.currentTimeMillis() + timeout;
      // A loop that will wait GC, using the minimal time as possible
      while (ref.get() != null && System.currentTimeMillis() < waitUntil)
      {
         ArrayList<String> list = new ArrayList<String>();
         for (int i = 0; i < 1000; i++)
         {
            list.add("Some string with garbage with concatenation " + i);
         }
         list.clear();
         list = null;
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

   /**
    * Verifies whether weak references are released after a few GCs.
    *
    * @param references
    * @throws InterruptedException
    */
   public static void checkWeakReferences(final WeakReference<?>... references)
   {
      int i = 0;
      boolean hasValue = false;

      do
      {
         hasValue = false;

         if (i > 0)
         {
            forceGC();
         }

         for (WeakReference<?> ref : references)
         {
            if (ref.get() != null)
            {
               hasValue = true;
               break;
            }
         }
      }
      while (i++ <= 30 && hasValue);

      for (WeakReference<?> ref : references)
      {
         Assert.assertNull(ref.get());
      }
   }

   public static String threadDump(final String msg)
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      Map<Thread, StackTraceElement[]> stackTrace = Thread.getAllStackTraces();

      out.println("*******************************************************************************");
      out.println("Complete Thread dump " + msg);

      for (Map.Entry<Thread, StackTraceElement[]> el : stackTrace.entrySet())
      {
         out.println("===============================================================================");
         out.println("Thread " + el.getKey() +
                             " name = " +
                             el.getKey().getName() +
                             " id = " +
                             el.getKey().getId() +
                             " group = " +
                             el.getKey().getThreadGroup());
         out.println();
         for (StackTraceElement traceEl : el.getValue())
         {
            out.println(traceEl);
         }
      }

      out.println("===============================================================================");
      out.println("End Thread dump " + msg);
      out.println("*******************************************************************************");

      return str.toString();
   }

   /**
    * Sends the message to both logger and System.out (for unit report)
    */
   public void logAndSystemOut(String message, Exception e)
   {
      ActiveMQServerLogger log0 = ActiveMQServerLogger.LOGGER;
      log0.info(message, e);
      System.out.println(message);
      if (e != null)
      {
         e.printStackTrace(System.out);
      }
   }

   /**
    * Sends the message to both logger and System.out (for unit report)
    */
   public void logAndSystemOut(String message)
   {
      logAndSystemOut(message, null);
   }

   public static String dumpBytes(final byte[] bytes)
   {
      StringBuffer buff = new StringBuffer();

      buff.append(System.identityHashCode(bytes) + ", size: " + bytes.length + " [");

      for (int i = 0; i < bytes.length; i++)
      {
         buff.append(bytes[i]);

         if (i != bytes.length - 1)
         {
            buff.append(", ");
         }
      }

      buff.append("]");

      return buff.toString();
   }

   public static String dumpBytesHex(final byte[] buffer, final int bytesPerLine)
   {

      StringBuffer buff = new StringBuffer();

      buff.append("[");

      for (int i = 0; i < buffer.length; i++)
      {
         buff.append(String.format("%1$2X", buffer[i]));
         if (i + 1 < buffer.length)
         {
            buff.append(", ");
         }
         if ((i + 1) % bytesPerLine == 0)
         {
            buff.append("\n ");
         }
      }
      buff.append("]");

      return buff.toString();
   }

   public static void assertEqualsTransportConfigurations(final TransportConfiguration[] expected,
                                                          final TransportConfiguration[] actual)
   {
      assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++)
      {
         Assert.assertEquals("TransportConfiguration at index " + i, expected[i], actual[i]);
      }
   }

   public static void assertEqualsBuffers(final int size, final ActiveMQBuffer expected, final ActiveMQBuffer actual)
   {
      // assertEquals(expected.length, actual.length);
      expected.readerIndex(0);
      actual.readerIndex(0);

      for (int i = 0; i < size; i++)
      {
         byte b1 = expected.readByte();
         byte b2 = actual.readByte();
         Assert.assertEquals("byte at index " + i, b1, b2);
      }
      expected.resetReaderIndex();
      actual.resetReaderIndex();
   }

   public static void assertEqualsByteArrays(final int length, final byte[] expected, final byte[] actual)
   {
      // we check only for the given length (the arrays might be
      // larger)
      Assert.assertTrue(expected.length >= length);
      Assert.assertTrue(actual.length >= length);
      for (int i = 0; i < length; i++)
      {
         Assert.assertEquals("byte at index " + i, expected[i], actual[i]);
      }
   }

   public static void assertSameXids(final List<Xid> expected, final List<Xid> actual)
   {
      Assert.assertNotNull(expected);
      Assert.assertNotNull(actual);
      Assert.assertEquals(expected.size(), actual.size());

      for (int i = 0; i < expected.size(); i++)
      {
         Xid expectedXid = expected.get(i);
         Xid actualXid = actual.get(i);
         assertEqualsByteArrays(expectedXid.getBranchQualifier(), actualXid.getBranchQualifier());
         Assert.assertEquals(expectedXid.getFormatId(), actualXid.getFormatId());
         assertEqualsByteArrays(expectedXid.getGlobalTransactionId(), actualXid.getGlobalTransactionId());
      }
   }

   protected static void checkNoBinding(final Context context, final String binding)
   {
      try
      {
         context.lookup(binding);
         Assert.fail("there must be no resource to look up for " + binding);
      }
      catch (Exception e)
      {
      }
   }

   protected static Object checkBinding(final Context context, final String binding) throws Exception
   {
      Object o = context.lookup(binding);
      Assert.assertNotNull(o);
      return o;
   }

   /**
    * @param connectorConfigs
    * @return
    */
   protected ArrayList<String> registerConnectors(final ActiveMQServer server,
                                                  final List<TransportConfiguration> connectorConfigs)
   {
      // The connectors need to be pre-configured at main config object but this method is taking
      // TransportConfigurations directly
      // So this will first register them at the config and then generate a list of objects
      ArrayList<String> connectors = new ArrayList<String>();
      for (TransportConfiguration tnsp : connectorConfigs)
      {
         String name1 = RandomUtil.randomString();

         server.getConfiguration().getConnectorConfigurations().put(name1, tnsp);

         connectors.add(name1);
      }
      return connectors;
   }

   protected static final void checkFreePort(final int... ports)
   {
      for (int port : ports)
      {
         ServerSocket ssocket = null;
         try
         {
            ssocket = new ServerSocket(port);
         }
         catch (Exception e)
         {
            throw new IllegalStateException("port " + port + " is bound", e);
         }
         finally
         {
            if (ssocket != null)
            {
               try
               {
                  ssocket.close();
               }
               catch (IOException e)
               {
               }
            }
         }
      }
   }

   /**
    * @return the testDir
    */
   protected final String getTestDir()
   {
      return testDir;
   }

   protected final void setTestDir(String testDir)
   {
      this.testDir = testDir;
   }

   protected final void clearDataRecreateServerDirs()
   {
      clearDataRecreateServerDirs(getTestDir());
   }

   protected void clearDataRecreateServerDirs(final String testDir1)
   {
      // Need to delete the root

      File file = new File(testDir1);
      deleteDirectory(file);
      file.mkdirs();

      recreateDirectory(getJournalDir(testDir1));
      recreateDirectory(getBindingsDir(testDir1));
      recreateDirectory(getPageDir(testDir1));
      recreateDirectory(getLargeMessagesDir(testDir1));
      recreateDirectory(getClientLargeMessagesDir(testDir1));
      recreateDirectory(getTemporaryDir(testDir1));
   }

   /**
    * @return the journalDir
    */
   public String getJournalDir()
   {
      return getJournalDir(getTestDir());
   }

   protected static String getJournalDir(final String testDir1)
   {
      return testDir1 + "/journal";
   }

   protected String getJournalDir(final int index, final boolean backup)
   {
      return getJournalDir(getTestDir(), index, backup);
   }

   public static String getJournalDir(final String testDir, final int index, final boolean backup)
   {
      return getJournalDir(testDir) + directoryNameSuffix(index, backup);
   }

   /**
    * @return the bindingsDir
    */
   protected String getBindingsDir()
   {
      return getBindingsDir(getTestDir());
   }

   /**
    * @return the bindingsDir
    */
   protected static String getBindingsDir(final String testDir1)
   {
      return testDir1 + "/bindings";
   }

   /**
    * @return the bindingsDir
    */
   protected String getBindingsDir(final int index, final boolean backup)
   {
      return getBindingsDir(getTestDir(), index, backup);
   }

   public static String getBindingsDir(final String testDir, final int index, final boolean backup)
   {
      return getBindingsDir(testDir) + directoryNameSuffix(index, backup);
   }

   /**
    * @return the pageDir
    */
   protected String getPageDir()
   {
      return getPageDir(getTestDir());
   }

   /**
    * @return the pageDir
    */
   protected static String getPageDir(final String testDir1)
   {
      return testDir1 + "/page";
   }

   protected String getPageDir(final int index, final boolean backup)
   {
      return getPageDir(getTestDir(), index, backup);
   }

   public static String getPageDir(final String testDir, final int index, final boolean backup)
   {
      return getPageDir(testDir) + directoryNameSuffix(index, backup);
   }

   /**
    * @return the largeMessagesDir
    */
   protected String getLargeMessagesDir()
   {
      return getLargeMessagesDir(getTestDir());
   }

   /**
    * @return the largeMessagesDir
    */
   protected static String getLargeMessagesDir(final String testDir1)
   {
      return testDir1 + "/large-msg";
   }

   protected String getLargeMessagesDir(final int index, final boolean backup)
   {
      return getLargeMessagesDir(getTestDir(), index, backup);
   }

   public static String getLargeMessagesDir(final String testDir, final int index, final boolean backup)
   {
      return getLargeMessagesDir(testDir) + directoryNameSuffix(index, backup);
   }

   private static String directoryNameSuffix(int index, boolean backup)
   {
      if (index == -1)
         return "";
      return index + "-" + (backup ? "B" : "L");
   }

   /**
    * @return the clientLargeMessagesDir
    */
   protected String getClientLargeMessagesDir()
   {
      return getClientLargeMessagesDir(getTestDir());
   }

   /**
    * @return the clientLargeMessagesDir
    */
   protected String getClientLargeMessagesDir(final String testDir1)
   {
      return testDir1 + "/client-large-msg";
   }

   /**
    * @return the temporaryDir
    */
   protected final String getTemporaryDir()
   {
      return getTemporaryDir(getTestDir());
   }

   /**
    * @return the temporaryDir
    */
   protected String getTemporaryDir(final String testDir1)
   {
      return testDir1 + "/temp";
   }

   protected static void expectActiveMQException(final String message, final ActiveMQExceptionType errorCode, final ActiveMQAction action)
   {
      try
      {
         action.run();
         Assert.fail(message);
      }
      catch (Exception e)
      {
         Assert.assertTrue(e instanceof ActiveMQException);
         Assert.assertEquals(errorCode, ((ActiveMQException) e).getType());
      }
   }

   protected static void expectActiveMQException(final ActiveMQExceptionType errorCode, final ActiveMQAction action)
   {
      expectActiveMQException("must throw a ActiveMQException with the expected errorCode: " + errorCode,
                              errorCode,
                              action);
   }

   protected static void expectXAException(final int errorCode, final ActiveMQAction action)
   {
      try
      {
         action.run();
         Assert.fail("must throw a XAException with the expected errorCode: " + errorCode);
      }
      catch (Exception e)
      {
         Assert.assertTrue(e instanceof XAException);
         Assert.assertEquals(errorCode, ((XAException) e).errorCode);
      }
   }

   public static byte getSamplebyte(final long position)
   {
      return (byte) ('a' + position % ('z' - 'a' + 1));
   }

   // Creates a Fake LargeStream without using a real file
   public static InputStream createFakeLargeStream(final long size) throws Exception
   {
      return new InputStream()
      {
         private long count;

         private boolean closed = false;

         @Override
         public void close() throws IOException
         {
            super.close();
            closed = true;
         }

         @Override
         public int read() throws IOException
         {
            if (closed)
            {
               throw new IOException("Stream was closed");
            }
            if (count++ < size)
            {
               return getSamplebyte(count - 1);
            }
            else
            {
               return -1;
            }
         }
      };

   }

   /**
    * It validates a Bean (POJO) using simple setters and getters with random values.
    * You can pass a list of properties to be ignored, as some properties will have a pre-defined domain (not being possible to use random-values on them)
    */
   protected void validateGettersAndSetters(final Object pojo, final String... ignoredProperties) throws Exception
   {
      HashSet<String> ignoreSet = new HashSet<String>();

      for (String ignore : ignoredProperties)
      {
         ignoreSet.add(ignore);
      }

      BeanInfo info = Introspector.getBeanInfo(pojo.getClass());

      PropertyDescriptor[] properties = info.getPropertyDescriptors();

      for (PropertyDescriptor prop : properties)
      {
         Object value;

         if (prop.getPropertyType() == String.class)
         {
            value = RandomUtil.randomString();
         }
         else if (prop.getPropertyType() == Integer.class || prop.getPropertyType() == Integer.TYPE)
         {
            value = RandomUtil.randomInt();
         }
         else if (prop.getPropertyType() == Long.class || prop.getPropertyType() == Long.TYPE)
         {
            value = RandomUtil.randomLong();
         }
         else if (prop.getPropertyType() == Boolean.class || prop.getPropertyType() == Boolean.TYPE)
         {
            value = RandomUtil.randomBoolean();
         }
         else if (prop.getPropertyType() == Double.class || prop.getPropertyType() == Double.TYPE)
         {
            value = RandomUtil.randomDouble();
         }
         else
         {
            System.out.println("Can't validate property of type " + prop.getPropertyType() + " on " + prop.getName());
            value = null;
         }

         if (value != null && prop.getWriteMethod() != null && prop.getReadMethod() == null)
         {
            System.out.println("WriteOnly property " + prop.getName() + " on " + pojo.getClass());
         }
         else if (value != null & prop.getWriteMethod() != null &&
                 prop.getReadMethod() != null &&
                 !ignoreSet.contains(prop.getName()))
         {
            System.out.println("Validating " + prop.getName() + " type = " + prop.getPropertyType());
            prop.getWriteMethod().invoke(pojo, value);

            Assert.assertEquals("Property " + prop.getName(), value, prop.getReadMethod().invoke(pojo));
         }
      }
   }

   /**
    * @param queue
    * @throws InterruptedException
    */
   protected void waitForNotPaging(Queue queue) throws InterruptedException
   {
      waitForNotPaging(queue.getPageSubscription().getPagingStore());
   }

   protected void waitForNotPaging(PagingStore store) throws InterruptedException
   {
      long timeout = System.currentTimeMillis() + 10000;
      while (timeout > System.currentTimeMillis() && store.isPaging())
      {
         Thread.sleep(100);
      }
      assertFalse(store.isPaging());
   }

   protected Topology waitForTopology(final ActiveMQServer server, final int nodes) throws Exception
   {
      return waitForTopology(server, nodes, -1, WAIT_TIMEOUT);
   }

   protected Topology waitForTopology(final ActiveMQServer server, final int nodes, final int backups) throws Exception
   {
      return waitForTopology(server, nodes, backups, WAIT_TIMEOUT);
   }

   protected Topology waitForTopology(final ActiveMQServer server, final int liveNodes, final int backupNodes, final long timeout) throws Exception
   {
      ActiveMQServerLogger.LOGGER.debug("waiting for " + liveNodes + " on the topology for server = " + server);

      long start = System.currentTimeMillis();

      Set<ClusterConnection> ccs = server.getClusterManager().getClusterConnections();

      if (ccs.size() != 1)
      {
         throw new IllegalStateException("You need a single cluster connection on this version of waitForTopology on ServiceTestBase");
      }

      Topology topology = server.getClusterManager().getDefaultConnection(null).getTopology();

      int liveNodesCount = 0;

      int backupNodesCount = 0;


      do
      {
         liveNodesCount = 0;
         backupNodesCount = 0;

         for (TopologyMemberImpl member : topology.getMembers())
         {
            if (member.getLive() != null)
            {
               liveNodesCount++;
            }
            if (member.getBackup() != null)
            {
               backupNodesCount++;
            }
         }

         if ((liveNodes == -1 || liveNodes == liveNodesCount) && (backupNodes == -1 || backupNodes == backupNodesCount))
         {
            return topology;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < timeout);

      String msg = "Timed out waiting for cluster topology of live=" + liveNodes + ",backup=" + backupNodes +
         " (received live=" + liveNodesCount + ", backup=" + backupNodesCount +
         ") topology = " +
         topology.describe() +
         ")";

      ActiveMQServerLogger.LOGGER.error(msg);

      throw new Exception(msg);
   }


   protected void waitForTopology(final ActiveMQServer server, String clusterConnectionName, final int nodes, final long timeout) throws Exception
   {
      ActiveMQServerLogger.LOGGER.debug("waiting for " + nodes + " on the topology for server = " + server);

      long start = System.currentTimeMillis();

      ClusterConnection clusterConnection = server.getClusterManager().getClusterConnection(clusterConnectionName);


      Topology topology = clusterConnection.getTopology();

      do
      {
         if (nodes == topology.getMembers().size())
         {
            return;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < timeout);

      String msg = "Timed out waiting for cluster topology of " + nodes +
         " (received " +
         topology.getMembers().size() +
         ") topology = " +
         topology +
         ")";

      ActiveMQServerLogger.LOGGER.error(msg);

      throw new Exception(msg);
   }

   protected static final void waitForComponent(final ActiveMQComponent component, final long seconds) throws InterruptedException
   {
      long time = System.currentTimeMillis();
      long toWait = seconds * 1000;
      while (!component.isStarted())
      {
         Thread.sleep(50);
         if (System.currentTimeMillis() > (time + toWait))
         {
            fail("component did not start within timeout of " + seconds);
         }
      }
   }

   protected static final Map<String, Object> generateParams(final int node, final boolean netty)
   {
      Map<String, Object> params = new HashMap<String, Object>();

      if (netty)
      {
         params.put(org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                    org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + node);
      }
      else
      {
         params.put(org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, node);
      }

      return params;
   }

   protected static final TransportConfiguration getNettyAcceptorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration(NETTY_ACCEPTOR_FACTORY);
      }

      Map<String, Object> server1Params = new HashMap<String, Object>();

      server1Params.put(org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                        org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);

      return new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, server1Params);
   }

   protected static final TransportConfiguration getNettyConnectorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
      }

      Map<String, Object> server1Params = new HashMap<String, Object>();

      server1Params.put(org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                        org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);
      return new TransportConfiguration(NETTY_CONNECTOR_FACTORY, server1Params);
   }

   protected static final TransportConfiguration createTransportConfiguration(boolean netty, boolean acceptor,
                                                                              Map<String, Object> params)
   {
      String className;
      if (netty)
      {
         if (acceptor)
         {
            className = NETTY_ACCEPTOR_FACTORY;
         }
         else
         {
            className = NETTY_CONNECTOR_FACTORY;
         }
      }
      else
      {
         if (acceptor)
         {
            className = INVM_ACCEPTOR_FACTORY;
         }
         else
         {
            className = INVM_CONNECTOR_FACTORY;
         }
      }
      if (params == null)
         params = new HashMap<String, Object>();
      return new TransportConfiguration(className, params);
   }


   protected void waitForServerToStart(ActiveMQServer server) throws InterruptedException
   {
      if (server == null)
         return;
      final long wait = 5000;
      long timetowait = System.currentTimeMillis() + wait;
      while (!server.isStarted() && System.currentTimeMillis() < timetowait)
      {
         Thread.sleep(50);
      }

      if (!server.isStarted())
      {
         log.info(threadDump("Server didn't start"));
         fail("server didn't start: " + server);
      }

      if (!server.getHAPolicy().isBackup())
      {
         if (!server.waitForActivation(wait, TimeUnit.MILLISECONDS))
            fail("Server didn't initialize: " + server);
      }
   }

   protected void waitForServerToStop(ActiveMQServer server) throws InterruptedException
   {
      if (server == null)
         return;
      final long wait = 5000;
      long timetowait = System.currentTimeMillis() + wait;
      while (server.isStarted() && System.currentTimeMillis() < timetowait)
      {
         Thread.sleep(50);
      }

      if (server.isStarted())
      {
         log.info(threadDump("Server didn't start"));
         fail("server didnt start: " + server);
      }
   }

   /**
    * @param backup
    */
   public static final void waitForRemoteBackupSynchronization(final ActiveMQServer backup)
   {
      waitForRemoteBackup(null, 10, true, backup);
   }

   /**
    * @param sessionFactoryP
    * @param seconds
    * @param waitForSync
    * @param backup
    */
   public static final void waitForRemoteBackup(ClientSessionFactory sessionFactoryP, int seconds,
                                                boolean waitForSync, final ActiveMQServer backup)
   {
      ClientSessionFactoryInternal sessionFactory = (ClientSessionFactoryInternal)sessionFactoryP;
      final ActiveMQServerImpl actualServer = (ActiveMQServerImpl) backup;
      final long toWait = seconds * 1000;
      final long time = System.currentTimeMillis();
      int loop = 0;
      while (true)
      {
         Activation activation = actualServer.getActivation();
         boolean isReplicated = !backup.getHAPolicy().isSharedStore();
         boolean isRemoteUpToDate = true;
         if (isReplicated)
         {
            if (activation instanceof SharedNothingBackupActivation)
            {
               isRemoteUpToDate = ((SharedNothingBackupActivation) activation).isRemoteBackupUpToDate();
            }
            else
            {
               //we may have already failed over and changed the Activation
               if (actualServer.isStarted())
               {
                  //let it fail a few time to have time to start stopping in the case of waiting to failback
                  isRemoteUpToDate = loop++ > 10;
               }
               //we could be waiting to failback or restart if the server is stopping
               else
               {
                  isRemoteUpToDate = false;
               }
            }
         }
         if ((sessionFactory == null || sessionFactory.getBackupConnector() != null) &&
               (isRemoteUpToDate || !waitForSync) &&
            (!waitForSync || actualServer.getBackupManager() != null && actualServer.getBackupManager().isBackupAnnounced()))
         {
            break;
         }
         if (System.currentTimeMillis() > (time + toWait))
         {
            fail("backup started? (" + actualServer.isStarted() + "). Finished synchronizing (" +
                  (activation) + "). SessionFactory!=null ? " + (sessionFactory != null) +
                    " || sessionFactory.getBackupConnector()==" +
                    (sessionFactory != null ? sessionFactory.getBackupConnector() : "not-applicable"));
         }
         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
            fail(e.getMessage());
         }
      }
   }

   public static final void waitForRemoteBackup(ClientSessionFactory sessionFactory, int seconds)
   {
      ClientSessionFactoryInternal factoryInternal = (ClientSessionFactoryInternal) sessionFactory;
      final long toWait = seconds * 1000;
      final long time = System.currentTimeMillis();
      while (true)
      {
         if (factoryInternal.getBackupConnector() != null)
         {
            break;
         }
         if (System.currentTimeMillis() > (time + toWait))
         {
            fail("Backup wasn't located");
         }
         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
            fail(e.getMessage());
         }
      }
   }

   protected final ActiveMQServer createServer(final boolean realFiles,
                                              final Configuration configuration,
                                              final long pageSize,
                                              final long maxAddressSize,
                                              final Map<String, AddressSettings> settings)
   {
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(configuration, realFiles));

      if (settings != null)
      {
         for (Map.Entry<String, AddressSettings> setting : settings.entrySet())
         {
            server.getAddressSettingsRepository().addMatch(setting.getKey(), setting.getValue());
         }
      }

      AddressSettings defaultSetting = new AddressSettings()
              .setPageSizeBytes(pageSize)
              .setMaxSizeBytes(maxAddressSize)
              .setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   protected final ActiveMQServer createServer(final boolean realFiles) throws Exception
   {
      return createServer(realFiles, false);
   }

   protected final ActiveMQServer createServer(final boolean realFiles, final boolean netty) throws Exception
   {
      return createServer(realFiles, createDefaultConfig(netty), AddressSettings.DEFAULT_PAGE_SIZE, AddressSettings.DEFAULT_MAX_SIZE_BYTES, new HashMap<String, AddressSettings>());
   }

   protected ActiveMQServer createServer(final boolean realFiles, final Configuration configuration)
   {
      return createServer(realFiles, configuration, AddressSettings.DEFAULT_PAGE_SIZE, AddressSettings.DEFAULT_MAX_SIZE_BYTES, new HashMap<String, AddressSettings>());
   }

   protected final ActiveMQServer createServer(final Configuration configuration)
   {
      return createServer(configuration.isPersistenceEnabled(), configuration, AddressSettings.DEFAULT_PAGE_SIZE, AddressSettings.DEFAULT_MAX_SIZE_BYTES, new HashMap<String, AddressSettings>());
   }

   protected ActiveMQServer createInVMFailoverServer(final boolean realFiles,
                                                    final Configuration configuration,
                                                    final NodeManager nodeManager,
                                                    final int id)
   {
      return createInVMFailoverServer(realFiles,
                                      configuration,
                                      -1,
                                      -1,
                                      new HashMap<String, AddressSettings>(),
                                      nodeManager,
                                      id);
   }

   protected ActiveMQServer createInVMFailoverServer(final boolean realFiles,
                                                    final Configuration configuration,
                                                    final int pageSize,
                                                    final int maxAddressSize,
                                                    final Map<String, AddressSettings> settings,
                                                    NodeManager nodeManager,
                                                    final int id)
   {
      ActiveMQServer server;
      ActiveMQSecurityManager securityManager = new ActiveMQSecurityManagerImpl();
      configuration.setPersistenceEnabled(realFiles);
      server = addServer(new InVMNodeManagerServer(configuration,
                                         ManagementFactory.getPlatformMBeanServer(),
                                         securityManager,
                                         nodeManager));

      try
      {
         server.setIdentity("Server " + id);

         for (Map.Entry<String, AddressSettings> setting : settings.entrySet())
         {
            server.getAddressSettingsRepository().addMatch(setting.getKey(), setting.getValue());
         }

         AddressSettings defaultSetting = new AddressSettings();
         defaultSetting.setPageSizeBytes(pageSize);
         defaultSetting.setMaxSizeBytes(maxAddressSize);

         server.getAddressSettingsRepository().addMatch("#", defaultSetting);

         return server;
      }
      finally
      {
         addServer(server);
      }
   }

   protected ActiveMQServer createColocatedInVMFailoverServer(final boolean realFiles,
                                                    final Configuration configuration,
                                                    NodeManager liveNodeManager,
                                                    NodeManager backupNodeManager,
                                                    final int id)
   {
      return createColocatedInVMFailoverServer(realFiles,
            configuration,
            -1,
            -1,
            new HashMap<String, AddressSettings>(),
            liveNodeManager,
            backupNodeManager,
            id);
   }

   protected ActiveMQServer createColocatedInVMFailoverServer(final boolean realFiles,
                                                    final Configuration configuration,
                                                    final int pageSize,
                                                    final int maxAddressSize,
                                                    final Map<String, AddressSettings> settings,
                                                    NodeManager liveNodeManager,
                                                    NodeManager backupNodeManager,
                                                    final int id)
   {
      ActiveMQServer server;
      ActiveMQSecurityManager securityManager = new ActiveMQSecurityManagerImpl();
      configuration.setPersistenceEnabled(realFiles);
      server = new ColocatedActiveMQServer(configuration,
            ManagementFactory.getPlatformMBeanServer(),
            securityManager,
            liveNodeManager,
            backupNodeManager);

      try
      {
         server.setIdentity("Server " + id);

         for (Map.Entry<String, AddressSettings> setting : settings.entrySet())
         {
            server.getAddressSettingsRepository().addMatch(setting.getKey(), setting.getValue());
         }

         AddressSettings defaultSetting = new AddressSettings();
         defaultSetting.setPageSizeBytes(pageSize);
         defaultSetting.setMaxSizeBytes(maxAddressSize);

         server.getAddressSettingsRepository().addMatch("#", defaultSetting);

         return server;
      }
      finally
      {
         addServer(server);
      }
   }

   protected ActiveMQServer createClusteredServerWithParams(final boolean isNetty,
                                                           final int index,
                                                           final boolean realFiles,
                                                           final Map<String, Object> params) throws Exception
   {
      String acceptor = isNetty ? NETTY_ACCEPTOR_FACTORY : INVM_ACCEPTOR_FACTORY;
      return createServer(realFiles, createDefaultConfig(index, params, acceptor), -1, -1,
                          new HashMap<String, AddressSettings>());
   }

   protected ActiveMQServer createClusteredServerWithParams(final boolean isNetty,
                                                           final int index,
                                                           final boolean realFiles,
                                                           final int pageSize,
                                                           final int maxAddressSize,
                                                           final Map<String, Object> params) throws Exception
   {
      return createServer(realFiles, createDefaultConfig(index, params, (isNetty ? NETTY_ACCEPTOR_FACTORY : INVM_ACCEPTOR_FACTORY)),
                          pageSize,
                          maxAddressSize,
                          new HashMap<String, AddressSettings>());
   }

   protected ServerLocator createFactory(final boolean isNetty) throws Exception
   {
      if (isNetty)
      {
         return createNettyNonHALocator();
      }
      else
      {
         return createInVMNonHALocator();
      }
   }

   protected void createQueue(final String address, final String queue) throws Exception
   {
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = locator.createSessionFactory();
      ClientSession session = sf.createSession();
      try
      {
         session.createQueue(address, queue);
      }
      finally
      {
         session.close();
         closeSessionFactory(sf);
         closeServerLocator(locator);
      }
   }

   protected final ServerLocator createInVMLocator(final int serverID)
   {
      TransportConfiguration tnspConfig = createInVMTransportConnectorConfig(serverID, UUIDGenerator.getInstance().generateStringUUID());

      ServerLocator locator = ActiveMQClient.createServerLocatorWithHA(tnspConfig);
      return addServerLocator(locator);
   }

   /**
    * @param serverID
    * @return
    */
   protected final TransportConfiguration createInVMTransportConnectorConfig(final int serverID, String name1)
   {
      Map<String, Object> server1Params = new HashMap<String, Object>();

      if (serverID != 0)
      {
         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, serverID);
      }

      TransportConfiguration tnspConfig = new TransportConfiguration(INVM_CONNECTOR_FACTORY, server1Params, name1);
      return tnspConfig;
   }

   public String getTextMessage(final ClientMessage m)
   {
      m.getBodyBuffer().resetReaderIndex();
      return m.getBodyBuffer().readString();
   }

   protected ClientMessage createBytesMessage(final ClientSession session,
                                              final byte type,
                                              final byte[] b,
                                              final boolean durable)
   {
      ClientMessage message = session.createMessage(type, durable, 0, System.currentTimeMillis(), (byte) 1);
      message.getBodyBuffer().writeBytes(b);
      return message;
   }

   /**
    * @param i
    * @param message
    * @throws Exception
    */
   protected void setBody(final int i, final ClientMessage message)
   {
      message.getBodyBuffer().writeString("message" + i);
   }

   /**
    * @param i
    * @param message
    */
   protected void assertMessageBody(final int i, final ClientMessage message)
   {
      Assert.assertEquals(message.toString(), "message" + i, message.getBodyBuffer().readString());
   }

   /**
    * Send durable messages with pre-specified body.
    *
    * @param session
    * @param producer
    * @param numMessages
    * @throws Exception
    */
   public final void sendMessages(ClientSession session, ClientProducer producer, int numMessages) throws ActiveMQException
   {
      for (int i = 0; i < numMessages; i++)
      {
         producer.send(createMessage(session, i, true));
      }
   }

   protected final ClientMessage createMessage(ClientSession session, int counter, boolean durable) throws ActiveMQException
   {
      ClientMessage message = session.createMessage(durable);
      setBody(counter, message);
      message.putIntProperty("counter", counter);
      message.putIntProperty(SEND_CALL_NUMBER, sendMsgCount++);
      return message;
   }

   protected final void receiveMessages(ClientConsumer consumer, final int start, final int msgCount, final boolean ack) throws ActiveMQException
   {
      for (int i = start; i < msgCount; i++)
      {
         ClientMessage message = consumer.receive(1000);
         Assert.assertNotNull("Expecting a message " + i, message);
         // sendCallNumber is just a debugging measure.
         Object prop = message.getObjectProperty(SEND_CALL_NUMBER);
         if (prop == null)
            prop = Integer.valueOf(-1);
         final int actual = message.getIntProperty("counter").intValue();
         Assert.assertEquals("expected=" + i + ". Got: property['counter']=" + actual + " sendNumber=" + prop, i,
                             actual);
         assertMessageBody(i, message);
         if (ack)
            message.acknowledge();
      }
   }

   /**
    * Reads a journal system and returns a Map<Integer,AtomicInteger> of recordTypes and the number of records per type,
    * independent of being deleted or not
    *
    * @param config
    * @return
    * @throws Exception
    */
   protected Pair<List<RecordInfo>, List<PreparedTransactionInfo>> loadMessageJournal(Configuration config) throws Exception
   {
      JournalImpl messagesJournal = null;
      try
      {
         SequentialFileFactory messagesFF = new NIOSequentialFileFactory(getJournalDir(), null);

         messagesJournal = new JournalImpl(config.getJournalFileSize(),
                                           config.getJournalMinFiles(),
                                           0,
                                           0,
                                           messagesFF,
                                           "activemq-data",
                                           "amq",
                                           1);
         final List<RecordInfo> committedRecords = new LinkedList<RecordInfo>();
         final List<PreparedTransactionInfo> preparedTransactions = new LinkedList<PreparedTransactionInfo>();

         messagesJournal.start();

         messagesJournal.load(committedRecords, preparedTransactions, null, false);

         return new Pair<List<RecordInfo>, List<PreparedTransactionInfo>>(committedRecords, preparedTransactions);
      }
      finally
      {
         try
         {
            if (messagesJournal != null)
            {
               messagesJournal.stop();
            }
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   /**
    * Reads a journal system and returns a Map<Integer,AtomicInteger> of recordTypes and the number of records per type,
    * independent of being deleted or not
    *
    * @param config
    * @return
    * @throws Exception
    */
   protected HashMap<Integer, AtomicInteger> countJournal(Configuration config) throws Exception
   {
      final HashMap<Integer, AtomicInteger> recordsType = new HashMap<Integer, AtomicInteger>();
      SequentialFileFactory messagesFF = new NIOSequentialFileFactory(config.getJournalDirectory(), null);

      JournalImpl messagesJournal = new JournalImpl(config.getJournalFileSize(),
                                                    config.getJournalMinFiles(),
                                                    0,
                                                    0,
                                                    messagesFF,
                                                    "activemq-data",
                                                    "amq",
                                                    1);
      List<JournalFile> filesToRead = messagesJournal.orderFiles();

      for (JournalFile file : filesToRead)
      {
         JournalImpl.readJournalFile(messagesFF, file, new RecordTypeCounter(recordsType));
      }
      return recordsType;
   }

   /**
    * This method will load a journal and count the living records
    *
    * @param config
    * @return
    * @throws Exception
    */
   protected HashMap<Integer, AtomicInteger> countJournalLivingRecords(Configuration config) throws Exception
   {
      return internalCountJournalLivingRecords(config, true);
   }

   /**
    * This method will load a journal and count the living records
    *
    * @param config
    * @param messageJournal if true -> MessageJournal, false -> BindingsJournal
    * @return
    * @throws Exception
    */
   protected HashMap<Integer, AtomicInteger> internalCountJournalLivingRecords(Configuration config, boolean messageJournal) throws Exception
   {
      final HashMap<Integer, AtomicInteger> recordsType = new HashMap<Integer, AtomicInteger>();
      SequentialFileFactory ff;

      JournalImpl journal;

      if (messageJournal)
      {
         ff = new NIOSequentialFileFactory(config.getJournalDirectory(), null);
         journal = new JournalImpl(config.getJournalFileSize(),
                                   config.getJournalMinFiles(),
                                   0,
                                   0,
                                   ff,
                                   "activemq-data",
                                   "amq",
                                   1);
      }
      else
      {
         ff = new NIOSequentialFileFactory(config.getBindingsDirectory(), null);
         journal = new JournalImpl(1024 * 1024,
                                   2,
                                   config.getJournalCompactMinFiles(),
                                   config.getJournalCompactPercentage(),
                                   ff,
                                   "activemq-bindings",
                                   "bindings",
                                   1);
      }
      journal.start();


      final List<RecordInfo> committedRecords = new LinkedList<RecordInfo>();
      final List<PreparedTransactionInfo> preparedTransactions = new LinkedList<PreparedTransactionInfo>();


      journal.load(committedRecords, preparedTransactions, null, false);

      for (RecordInfo info : committedRecords)
      {
         Integer ikey = new Integer(info.getUserRecordType());
         AtomicInteger value = recordsType.get(ikey);
         if (value == null)
         {
            value = new AtomicInteger();
            recordsType.put(ikey, value);
         }
         value.incrementAndGet();

      }

      journal.stop();
      return recordsType;
   }

   private static final class RecordTypeCounter implements JournalReaderCallback
   {
      private final HashMap<Integer, AtomicInteger> recordsType;

      /**
       * @param recordsType
       */
      public RecordTypeCounter(HashMap<Integer, AtomicInteger> recordsType)
      {
         this.recordsType = recordsType;
      }

      AtomicInteger getType(byte key)
      {
         if (key == 0)
         {
            System.out.println("huh?");
         }
         Integer ikey = new Integer(key);
         AtomicInteger value = recordsType.get(ikey);
         if (value == null)
         {
            value = new AtomicInteger();
            recordsType.put(ikey, value);
         }
         return value;
      }

      public void onReadUpdateRecordTX(long transactionID, RecordInfo recordInfo) throws Exception
      {
         getType(recordInfo.getUserRecordType()).incrementAndGet();
      }

      public void onReadUpdateRecord(RecordInfo recordInfo) throws Exception
      {
         getType(recordInfo.getUserRecordType()).incrementAndGet();
      }

      public void onReadAddRecordTX(long transactionID, RecordInfo recordInfo) throws Exception
      {
         getType(recordInfo.getUserRecordType()).incrementAndGet();
      }

      public void onReadAddRecord(RecordInfo recordInfo) throws Exception
      {
         getType(recordInfo.getUserRecordType()).incrementAndGet();
      }

      public void onReadRollbackRecord(long transactionID) throws Exception
      {
      }

      public void onReadPrepareRecord(long transactionID, byte[] extraData, int numberOfRecords) throws Exception
      {
      }

      public void onReadDeleteRecordTX(long transactionID, RecordInfo recordInfo) throws Exception
      {
      }

      public void onReadDeleteRecord(long recordID) throws Exception
      {
      }

      public void onReadCommitRecord(long transactionID, int numberOfRecords) throws Exception
      {
      }

      public void markAsDataFile(JournalFile file0)
      {
      }
   }

   /**
    * @param server                the server where's being checked
    * @param address               the name of the address being checked
    * @param local                 if true we are looking for local bindings, false we are looking for remoting servers
    * @param expectedBindingCount  the expected number of counts
    * @param expectedConsumerCount the expected number of consumers
    * @param timeout               the timeout used on the check
    * @return
    * @throws Exception
    * @throws InterruptedException
    */
   protected boolean waitForBindings(final ActiveMQServer server,
                                     final String address,
                                     final boolean local,
                                     final int expectedBindingCount,
                                     final int expectedConsumerCount,
                                     long timeout) throws Exception
   {
      final PostOffice po = server.getPostOffice();

      long start = System.currentTimeMillis();

      int bindingCount = 0;

      int totConsumers = 0;

      do
      {
         bindingCount = 0;

         totConsumers = 0;

         Bindings bindings = po.getBindingsForAddress(new SimpleString(address));

         for (Binding binding : bindings.getBindings())
         {
            if (binding.isConnected() && (binding instanceof LocalQueueBinding && local || binding instanceof RemoteQueueBinding && !local))
            {
               QueueBinding qBinding = (QueueBinding) binding;

               bindingCount++;

               totConsumers += qBinding.consumerCount();
            }
         }

         if (bindingCount == expectedBindingCount && totConsumers == expectedConsumerCount)
         {
            return true;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < timeout);

      String msg = "Timed out waiting for bindings (bindingCount = " + bindingCount +
         " (expecting " +
         expectedBindingCount +
         ") " +
         ", totConsumers = " +
         totConsumers +
         " (expecting " +
         expectedConsumerCount +
         ")" +
         ")";

      log.error(msg);
      return false;
   }

   /**
    * Deleting a file on LargeDir is an asynchronous process. We need to keep looking for a while if
    * the file hasn't been deleted yet.
    */
   protected void validateNoFilesOnLargeDir(final String directory, final int expect) throws Exception
   {
      File largeMessagesFileDir = new File(directory);

      // Deleting the file is async... we keep looking for a period of the time until the file is really gone
      long timeout = System.currentTimeMillis() + 5000;
      while (timeout > System.currentTimeMillis() && largeMessagesFileDir.listFiles().length != expect)
      {
         Thread.sleep(100);
      }


      if (expect != largeMessagesFileDir.listFiles().length)
      {
         for (File file : largeMessagesFileDir.listFiles())
         {
            System.out.println("File " + file + " still on ");
         }
      }

      Assert.assertEquals(expect, largeMessagesFileDir.listFiles().length);
   }

   /**
    * Deleting a file on LargeDire is an asynchronous process. Wee need to keep looking for a while
    * if the file hasn't been deleted yet
    */
   protected void validateNoFilesOnLargeDir() throws Exception
   {
      validateNoFilesOnLargeDir(getLargeMessagesDir(), 0);
   }

   public void printBindings(ActiveMQServer server, String address) throws Exception
   {
      PostOffice po = server.getPostOffice();
      Bindings bindings = po.getBindingsForAddress(new SimpleString(address));

      System.err.println("=======================================================================");
      System.err.println("Binding information for address = " + address + " for server " + server);

      for (Binding binding : bindings.getBindings())
      {
         QueueBinding qBinding = (QueueBinding) binding;
         System.err.println("Binding = " + qBinding + ", queue=" + qBinding.getQueue());
      }

   }

   private void assertAllExecutorsFinished() throws InterruptedException
   {
      for (ExecutorService s : executorSet)
      {
         Assert.assertTrue(s.awaitTermination(5, TimeUnit.SECONDS));
      }
   }

   private ArrayList<Exception> checkCsfStopped()
   {
      long time = System.currentTimeMillis();
      long waitUntil = time + 5000;
      while (!ClientSessionFactoryImpl.CLOSE_RUNNABLES.isEmpty() && time < waitUntil)
      {
         try
         {
            Thread.sleep(50);
         }
         catch (InterruptedException e)
         {
            //ignore
         }
         time = System.currentTimeMillis();
      }
      List<ClientSessionFactoryImpl.CloseRunnable> closeRunnables = new ArrayList<>(ClientSessionFactoryImpl.CLOSE_RUNNABLES);
      ArrayList<Exception> exceptions = new ArrayList<>();

      if (!closeRunnables.isEmpty())
      {
         for (ClientSessionFactoryImpl.CloseRunnable closeRunnable : closeRunnables)
         {
            if (closeRunnable != null)
            {
               exceptions.add(closeRunnable.stop().createTrace);
            }
         }
      }

      return exceptions;
   }

   private void assertAllClientProducersAreClosed()
   {
      synchronized (clientProducers)
      {
         for (ClientProducer p : clientProducers)
         {
            assertTrue(p + " should be closed", p.isClosed());
         }
         clientProducers.clear();
      }
   }

   /**
    *
    */
   private void closeAllOtherComponents()
   {
      synchronized (otherComponents)
      {
         for (ActiveMQComponent c : otherComponents)
         {
            stopComponent(c);
         }
         otherComponents.clear();
      }
   }

   /**
    * @param buffer
    * @return
    */
   private boolean checkThread(StringBuffer buffer)
   {
      boolean failedThread = false;

      Map<Thread, StackTraceElement[]> postThreads = Thread.getAllStackTraces();

      if (postThreads != null && previousThreads != null && postThreads.size() > previousThreads.size())
      {

         buffer.append("*********************************************************************************\n");
         buffer.append("LEAKING THREADS\n");

         for (Thread aliveThread : postThreads.keySet())
         {
            if (!isExpectedThread(aliveThread) && !previousThreads.containsKey(aliveThread))
            {
               failedThread = true;
               buffer.append("=============================================================================\n");
               buffer.append("Thread " + aliveThread + " is still alive with the following stackTrace:\n");
               StackTraceElement[] elements = postThreads.get(aliveThread);
               for (StackTraceElement el : elements)
               {
                  buffer.append(el + "\n");
               }
            }

         }
         buffer.append("*********************************************************************************\n");

      }
      return failedThread;
   }

   /**
    * if it's an expected thread... we will just move along ignoring it
    *
    * @param thread
    * @return
    */
   private boolean isExpectedThread(Thread thread)
   {
      final String threadName = thread.getName();
      final ThreadGroup group = thread.getThreadGroup();
      final boolean isSystemThread = group != null && "system".equals(group.getName());
      final String javaVendor = System.getProperty("java.vendor");

      if (threadName.contains("SunPKCS11"))
      {
         return true;
      }
      else if (threadName.contains("Attach Listener"))
      {
         return true;
      }
      else if ((javaVendor.contains("IBM") || isSystemThread) && threadName.equals("process reaper"))
      {
         return true;
      }
      else if (javaVendor.contains("IBM") && threadName.equals("MemoryPoolMXBean notification dispatcher"))
      {
         return true;
      }
      else if (threadName.contains("globalEventExecutor"))
      {
         return true;
      }
      else if (threadName.contains("threadDeathWatcher"))
      {
         return true;
      }
      else if (threadName.contains("netty-threads"))
      {
         // This is ok as we use EventLoopGroup.shutdownGracefully() which will shutdown things with a bit of delay
         // if the EventLoop's are still busy.
         return true;
      }
      else if (threadName.contains("threadDeathWatcher"))
      {
         //another netty thread
         return true;
      }
      else
      {
         for (StackTraceElement element : thread.getStackTrace())
         {
            if (element.getClassName().contains("org.jboss.byteman.agent.TransformListener"))
            {
               return true;
            }
         }
         return false;
      }
   }

   private void checkFilesUsage()
   {


      long timeout = System.currentTimeMillis() + 15000;

      while (AsynchronousFileImpl.getTotalMaxIO() != 0 && System.currentTimeMillis() > timeout)
      {
         try
         {
            Thread.sleep(100);
         }
         catch (Exception ignored)
         {
         }
      }

      int invmSize = InVMRegistry.instance.size();
      if (invmSize > 0)
      {
         InVMRegistry.instance.clear();
         log.info(threadDump("Thread dump"));
         fail("invm registry still had acceptors registered");
      }

      final int totalMaxIO = AsynchronousFileImpl.getTotalMaxIO();
      if (totalMaxIO != 0)
      {
         AsynchronousFileImpl.resetMaxAIO();
         Assert.fail("test did not close all its files " + totalMaxIO);
      }
   }

   private void cleanupPools()
   {
      OperationContextImpl.clearContext();

      // We shutdown the global pools to give a better isolation between tests
      try
      {
         ServerLocatorImpl.clearThreadPools();
      }
      catch (Throwable e)
      {
         log.info(threadDump(e.getMessage()));
         System.err.println(threadDump(e.getMessage()));
      }

      try
      {
         NettyConnector.clearThreadPools();
      }
      catch (Exception e)
      {
         log.info(threadDump(e.getMessage()));
         System.err.println(threadDump(e.getMessage()));
      }
   }

   protected static final void recreateDirectory(final String directory)
   {
      File file = new File(directory);
      deleteDirectory(file);
      file.mkdirs();
   }

   protected static final boolean deleteDirectory(final File directory)
   {
      if (directory.isDirectory())
      {
         String[] files = directory.list();
         int num = 5;
         int attempts = 0;
         while (files == null && (attempts < num))
         {
            try
            {
               Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
            }
            files = directory.list();
            attempts++;
         }

         for (String file : files)
         {
            File f = new File(directory, file);
            if (!deleteDirectory(f))
            {
               log.warn("Failed to clean up file: " + f.getAbsolutePath());
            }
         }
      }

      return directory.delete();
   }

   protected static final void copyRecursive(final File from, final File to) throws Exception
   {
      if (from.isDirectory())
      {
         if (!to.exists())
         {
            to.mkdir();
         }

         String[] subs = from.list();

         for (String sub : subs)
         {
            copyRecursive(new File(from, sub), new File(to, sub));
         }
      }
      else
      {
         InputStream in = null;

         OutputStream out = null;

         try
         {
            in = new BufferedInputStream(new FileInputStream(from));

            out = new BufferedOutputStream(new FileOutputStream(to));

            int b;

            while ((b = in.read()) != -1)
            {
               out.write(b);
            }
         }
         finally
         {
            if (in != null)
            {
               in.close();
            }

            if (out != null)
            {
               out.close();
            }
         }
      }
   }

   protected void assertRefListsIdenticalRefs(final List<MessageReference> l1, final List<MessageReference> l2)
   {
      if (l1.size() != l2.size())
      {
         Assert.fail("Lists different sizes: " + l1.size() + ", " + l2.size());
      }

      Iterator<MessageReference> iter1 = l1.iterator();
      Iterator<MessageReference> iter2 = l2.iterator();

      while (iter1.hasNext())
      {
         MessageReference o1 = iter1.next();
         MessageReference o2 = iter2.next();

         Assert.assertTrue("expected " + o1 + " but was " + o2, o1 == o2);
      }
   }

   protected ServerMessage generateMessage(final long id)
   {
      ServerMessage message = new ServerMessageImpl(id, 1000);

      message.setMessageID(id);

      message.getBodyBuffer().writeString(UUID.randomUUID().toString());

      message.setAddress(new SimpleString("foo"));

      return message;
   }

   protected MessageReference generateReference(final Queue queue, final long id)
   {
      ServerMessage message = generateMessage(id);

      return message.createReference(queue);
   }

   protected int calculateRecordSize(final int size, final int alignment)
   {
      return (size / alignment + (size % alignment != 0 ? 1 : 0)) * alignment;
   }

   protected ClientMessage createTextMessage(final ClientSession session, final String s)
   {
      return createTextMessage(session, s, true);
   }


   protected ClientMessage createTextMessage(final ClientSession session, final String s, final boolean durable)
   {
      ClientMessage message = session.createMessage(Message.TEXT_TYPE,
                                                    durable,
                                                    0,
                                                    System.currentTimeMillis(),
                                                    (byte) 4);
      message.getBodyBuffer().writeString(s);
      return message;
   }

   protected XidImpl newXID()
   {
      return new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
   }

   protected int getMessageCount(final ActiveMQServer service, final String address) throws Exception
   {
      return getMessageCount(service.getPostOffice(), address);
   }

   /**
    * @param address
    * @param postOffice
    * @return
    * @throws Exception
    */
   protected int getMessageCount(final PostOffice postOffice, final String address) throws Exception
   {
      int messageCount = 0;

      List<QueueBinding> bindings = getLocalQueueBindings(postOffice, address);

      for (QueueBinding qBinding : bindings)
      {
         qBinding.getQueue().flushExecutor();
         messageCount += getMessageCount(qBinding.getQueue());
      }

      return messageCount;
   }

   protected int getMessageCount(final Queue queue)
   {
      queue.flushExecutor();
      return (int)queue.getMessageCount();
   }

   protected int getMessagesAdded(final Queue queue)
   {
      queue.flushExecutor();
      return (int)queue.getMessagesAdded();
   }

   private List<QueueBinding> getLocalQueueBindings(final PostOffice postOffice, final String address) throws Exception
   {
      ArrayList<QueueBinding> bindingsFound = new ArrayList<QueueBinding>();

      Bindings bindings = postOffice.getBindingsForAddress(new SimpleString(address));

      for (Binding binding : bindings.getBindings())
      {
         if (binding instanceof LocalQueueBinding)
         {
            bindingsFound.add((QueueBinding) binding);
         }
      }
      return bindingsFound;
   }

   protected final ServerLocator createInVMNonHALocator()
   {
      return createNonHALocator(false);
   }

   protected final ServerLocator createNettyNonHALocator()
   {
      return createNonHALocator(true);
   }

   protected final ServerLocator createNonHALocator(final boolean isNetty)
   {
      ServerLocator locatorWithoutHA = internalCreateNonHALocator(isNetty);
      return addServerLocator(locatorWithoutHA);
   }

   /**
    * Creates the Locator without adding it to the list where the tearDown will take place
    * This is because we don't want it closed in certain tests where we are issuing failures
    * @param isNetty
    * @return
    */
   public ServerLocator internalCreateNonHALocator(boolean isNetty)
   {
      return isNetty
              ? ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NETTY_CONNECTOR_FACTORY))
              : ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
   }

   protected static final void stopComponent(ActiveMQComponent component)
   {
      if (component == null)
         return;
      try
      {
         component.stop();
      }
      catch (Exception e)
      {
         // no-op
      }
   }

   protected static final void stopComponentOutputExceptions(ActiveMQComponent component)
   {
      if (component == null)
         return;
      try
      {
         component.stop();
      }
      catch (Exception e)
      {
         System.err.println("Exception closing " + component);
         e.printStackTrace();
      }
   }

   protected final ClientSessionFactory createSessionFactory(ServerLocator locator) throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();
      addSessionFactory(sf);
      return sf;
   }

   protected final ActiveMQServer addServer(final ActiveMQServer server)
   {
      if (server != null)
      {
         synchronized (servers)
         {
            servers.add(server);
         }
      }
      return server;
   }

   protected final ServerLocator addServerLocator(final ServerLocator locator)
   {
      if (locator != null)
      {
         synchronized (locators)
         {
            locators.add(locator);
         }
      }
      return locator;
   }

   protected final ClientSession addClientSession(final ClientSession session)
   {
      if (session != null)
      {
         synchronized (clientSessions)
         {
            clientSessions.add(session);
         }
      }
      return session;
   }

   protected final ClientConsumer addClientConsumer(final ClientConsumer consumer)
   {
      if (consumer != null)
      {
         synchronized (clientConsumers)
         {
            clientConsumers.add(consumer);
         }
      }
      return consumer;
   }

   protected final ClientProducer addClientProducer(final ClientProducer producer)
   {
      if (producer != null)
      {
         synchronized (clientProducers)
         {
            clientProducers.add(producer);
         }
      }
      return producer;
   }

   protected final void addActiveMQComponent(final ActiveMQComponent component)
   {
      if (component != null)
      {
         synchronized (otherComponents)
         {
            otherComponents.add(component);
         }
      }
   }

   protected final ClientSessionFactory addSessionFactory(final ClientSessionFactory sf)
   {
      if (sf != null)
      {
         synchronized (sessionFactories)
         {
            sessionFactories.add(sf);
         }
      }
      return sf;
   }

   private void assertAllClientConsumersAreClosed()
   {
      synchronized (clientConsumers)
      {
         for (ClientConsumer cc : clientConsumers)
         {
            if (cc == null)
               continue;
            assertTrue(cc.isClosed());
         }
         clientConsumers.clear();
      }
   }

   private void assertAllClientSessionsAreClosed()
   {
      synchronized (clientSessions)
      {
         for (final ClientSession cs : clientSessions)
         {
            if (cs == null)
               continue;
            assertTrue(cs.isClosed());
         }
         clientSessions.clear();
      }
   }

   protected void closeAllSessionFactories()
   {
      synchronized (sessionFactories)
      {
         for (ClientSessionFactory sf : sessionFactories)
         {
            closeSessionFactory(sf);
            assert sf.isClosed();
         }
         sessionFactories.clear();
      }
   }

   protected void closeAllServerLocatorsFactories()
   {
      synchronized (locators)
      {
         for (ServerLocator locator : locators)
         {
            closeServerLocator(locator);
         }
         locators.clear();
      }
   }

   public static final void closeServerLocator(ServerLocator locator)
   {
      if (locator == null)
         return;
      try
      {
         locator.close();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   public static final void closeSessionFactory(final ClientSessionFactory sf)
   {
      if (sf == null)
         return;
      try
      {
         sf.close();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   public static void crashAndWaitForFailure(ActiveMQServer server, ClientSession... sessions) throws Exception
   {
      CountDownLatch latch = new CountDownLatch(sessions.length);
      for (ClientSession session : sessions)
      {
         CountDownSessionFailureListener listener = new CountDownSessionFailureListener(latch, session);
         session.addFailureListener(listener);
      }

      ClusterManager clusterManager = server.getClusterManager();
      clusterManager.flushExecutor();
      clusterManager.clear();
      Assert.assertTrue("server should be running!", server.isStarted());
      server.stop(true);

      if (sessions.length > 0)
      {
         // Wait to be informed of failure
         boolean ok = latch.await(10000, TimeUnit.MILLISECONDS);
         Assert.assertTrue("Failed to stop the server! Latch count is " + latch.getCount() + " out of " +
                                   sessions.length, ok);
      }
   }

   public static void crashAndWaitForFailure(ActiveMQServer server, ServerLocator locator) throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();
      ClientSession session = sf.createSession();
      try
      {
         crashAndWaitForFailure(server, session);
      }
      finally
      {
         try
         {
            session.close();
            sf.close();
         }
         catch (Exception ignored)
         {
         }
      }
   }

   public interface RunnerWithEX
   {
      void run() throws Throwable;
   }

   // This can be used to interrupt a thread if it takes more than timeoutMilliseconds
   public boolean runWithTimeout(final RunnerWithEX runner, final long timeoutMilliseconds) throws Throwable
   {

      class ThreadRunner extends Thread
      {

         Throwable t;

         final RunnerWithEX run;

         ThreadRunner(RunnerWithEX run)
         {
            this.run = run;
         }

         public void run()
         {
            try
            {
               runner.run();
            }
            catch (Throwable t)
            {
               this.t = t;
            }
         }
      }

      ThreadRunner runnerThread = new ThreadRunner(runner);

      runnerThread.start();

      boolean hadToInterrupt = false;
      while (runnerThread.isAlive())
      {
         runnerThread.join(timeoutMilliseconds);
         if (runnerThread.isAlive())
         {
            System.err.println("Thread still running, interrupting it now:");
            for (Object t : runnerThread.getStackTrace())
            {
               System.err.println(t);
            }
            hadToInterrupt = true;
            runnerThread.interrupt();
         }
      }

      if (runnerThread.t != null)
      {
         runnerThread.t.printStackTrace();
         throw runnerThread.t;
      }

      // we are returning true if it ran ok.
      // had to Interrupt is exactly the opposite of what we are returning
      return !hadToInterrupt;
   }


   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   protected interface ActiveMQAction
   {
      void run() throws Exception;
   }

   /**
    * Asserts that latch completes within a (rather large interval).
    * <p>
    * Use this instead of just calling {@code latch.await()}. Otherwise your test may hang the whole
    * test run if it fails to count-down the latch.
    *
    * @param latch
    * @throws InterruptedException
    */
   public static void waitForLatch(CountDownLatch latch) throws InterruptedException
   {
      assertTrue("Latch has got to return within a minute", latch.await(1, TimeUnit.MINUTES));
   }
}
