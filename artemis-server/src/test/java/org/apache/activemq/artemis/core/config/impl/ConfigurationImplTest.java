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
package org.apache.activemq.artemis.core.config.impl;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.activemq.artemis.ArtemisConstants;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ConfigurationUtils;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.federation.FederationAddressPolicyConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationPolicySet;
import org.apache.activemq.artemis.core.config.federation.FederationQueuePolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.LiveOnlyPolicyConfiguration;
import org.apache.activemq.artemis.core.deployers.impl.FileConfigurationParser;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ComponentConfigurationRoutingType;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.plugin.impl.LoggingActiveMQServerPlugin;
import org.apache.activemq.artemis.core.server.routing.KeyType;
import org.apache.activemq.artemis.core.server.routing.policies.ConsistentHashModuloPolicy;
import org.apache.activemq.artemis.core.settings.impl.ResourceLimitSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzerPolicy;
import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConfigurationImplTest extends ActiveMQTestBase {

   private static final Logger log = LoggerFactory.getLogger(ConfigurationImplTest.class);

   protected Configuration conf;

   @Test
   public void testDefaults() {
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultScheduledThreadPoolMaxSize(), conf.getScheduledThreadPoolMaxSize());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultSecurityInvalidationInterval(), conf.getSecurityInvalidationInterval());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultSecurityEnabled(), conf.isSecurityEnabled());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultBindingsDirectory(), conf.getBindingsDirectory());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultCreateBindingsDir(), conf.isCreateBindingsDir());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalDir(), conf.getJournalDirectory());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultCreateJournalDir(), conf.isCreateJournalDir());
      Assert.assertEquals(ConfigurationImpl.DEFAULT_JOURNAL_TYPE, conf.getJournalType());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultJournalSyncTransactional(), conf.isJournalSyncTransactional());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultJournalSyncNonTransactional(), conf.isJournalSyncNonTransactional());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalFileSize(), conf.getJournalFileSize());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalMinFiles(), conf.getJournalMinFiles());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalMaxIoAio(), conf.getJournalMaxIO_AIO());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalMaxIoNio(), conf.getJournalMaxIO_NIO());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultWildcardRoutingEnabled(), conf.isWildcardRoutingEnabled());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultTransactionTimeout(), conf.getTransactionTimeout());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMessageExpiryScanPeriod(), conf.getMessageExpiryScanPeriod()); // OK
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultTransactionTimeoutScanPeriod(), conf.getTransactionTimeoutScanPeriod()); // OK
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultManagementAddress(), conf.getManagementAddress()); // OK
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultManagementNotificationAddress(), conf.getManagementNotificationAddress()); // OK
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultClusterUser(), conf.getClusterUser()); // OK
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultClusterPassword(), conf.getClusterPassword()); // OK
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultPersistenceEnabled(), conf.isPersistenceEnabled());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultPersistDeliveryCountBeforeDelivery(), conf.isPersistDeliveryCountBeforeDelivery());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultFileDeployerScanPeriod(), conf.getFileDeployerScanPeriod());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultThreadPoolMaxSize(), conf.getThreadPoolMaxSize());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultJmxManagementEnabled(), conf.isJMXManagementEnabled());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultConnectionTtlOverride(), conf.getConnectionTTLOverride());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultAsyncConnectionExecutionEnabled(), conf.isAsyncConnectionExecutionEnabled());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultPagingDir(), conf.getPagingDirectory());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultLargeMessagesDir(), conf.getLargeMessagesDirectory());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalCompactPercentage(), conf.getJournalCompactPercentage());
      Assert.assertEquals(ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, conf.getJournalBufferTimeout_AIO());
      Assert.assertEquals(ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO, conf.getJournalBufferTimeout_NIO());
      Assert.assertEquals(ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, conf.getJournalBufferSize_AIO());
      Assert.assertEquals(ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO, conf.getJournalBufferSize_NIO());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultJournalLogWriteRate(), conf.isLogJournalWriteRate());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultMessageCounterEnabled(), conf.isMessageCounterEnabled());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMessageCounterMaxDayHistory(), conf.getMessageCounterMaxDayHistory());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMessageCounterSamplePeriod(), conf.getMessageCounterSamplePeriod());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultIdCacheSize(), conf.getIDCacheSize());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultPersistIdCache(), conf.isPersistIDCache());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultServerDumpInterval(), conf.getServerDumpInterval());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMemoryWarningThreshold(), conf.getMemoryWarningThreshold());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMemoryMeasureInterval(), conf.getMemoryMeasureInterval());
      Assert.assertEquals(conf.getJournalLocation(), conf.getNodeManagerLockLocation());
      Assert.assertNull(conf.getJournalDeviceBlockSize());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultReadWholePage(), conf.isReadWholePage());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalBufferTimeoutNio(), conf.getPageSyncTimeout());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultTemporaryQueueNamespace(), conf.getTemporaryQueueNamespace());
   }

   @Test
   public void testNullMaskPassword() {
      ConfigurationImpl impl = new ConfigurationImpl();
      impl.setMaskPassword(null);

      Assert.assertEquals(impl.hashCode(), impl.hashCode());
   }

   @Test
   public void testSetGetAttributes() throws Exception {
      for (int j = 0; j < 100; j++) {
         int i = RandomUtil.randomInt();
         conf.setScheduledThreadPoolMaxSize(i);
         Assert.assertEquals(i, conf.getScheduledThreadPoolMaxSize());

         long l = RandomUtil.randomLong();
         conf.setSecurityInvalidationInterval(l);
         Assert.assertEquals(l, conf.getSecurityInvalidationInterval());

         boolean b = RandomUtil.randomBoolean();
         conf.setSecurityEnabled(b);
         Assert.assertEquals(b, conf.isSecurityEnabled());

         String s = RandomUtil.randomString();
         conf.setBindingsDirectory(s);
         Assert.assertEquals(s, conf.getBindingsDirectory());

         b = RandomUtil.randomBoolean();
         conf.setCreateBindingsDir(b);
         Assert.assertEquals(b, conf.isCreateBindingsDir());

         s = RandomUtil.randomString();
         conf.setJournalDirectory(s);
         Assert.assertEquals(s, conf.getJournalDirectory());

         b = RandomUtil.randomBoolean();
         conf.setCreateJournalDir(b);
         Assert.assertEquals(b, conf.isCreateJournalDir());

         i = RandomUtil.randomInt() % 2;
         JournalType journal = i == 0 ? JournalType.ASYNCIO : JournalType.NIO;
         conf.setJournalType(journal);
         Assert.assertEquals(journal, conf.getJournalType());

         b = RandomUtil.randomBoolean();
         conf.setJournalSyncTransactional(b);
         Assert.assertEquals(b, conf.isJournalSyncTransactional());

         b = RandomUtil.randomBoolean();
         conf.setJournalSyncNonTransactional(b);
         Assert.assertEquals(b, conf.isJournalSyncNonTransactional());

         i = RandomUtil.randomInt();
         conf.setJournalFileSize(i);
         Assert.assertEquals(i, conf.getJournalFileSize());

         i = RandomUtil.randomInt();
         conf.setJournalMinFiles(i);
         Assert.assertEquals(i, conf.getJournalMinFiles());

         i = RandomUtil.randomInt();
         conf.setJournalMaxIO_AIO(i);
         Assert.assertEquals(i, conf.getJournalMaxIO_AIO());

         i = RandomUtil.randomInt();
         conf.setJournalMaxIO_NIO(i);
         Assert.assertEquals(i, conf.getJournalMaxIO_NIO());

         s = RandomUtil.randomString();
         conf.setManagementAddress(new SimpleString(s));
         Assert.assertEquals(s, conf.getManagementAddress().toString());

         l = RandomUtil.randomLong();
         conf.setMessageExpiryScanPeriod(l);
         Assert.assertEquals(l, conf.getMessageExpiryScanPeriod());

         b = RandomUtil.randomBoolean();
         conf.setPersistDeliveryCountBeforeDelivery(b);
         Assert.assertEquals(b, conf.isPersistDeliveryCountBeforeDelivery());

         b = RandomUtil.randomBoolean();
         conf.setEnabledAsyncConnectionExecution(b);
         Assert.assertEquals(b, conf.isAsyncConnectionExecutionEnabled());

         b = RandomUtil.randomBoolean();
         conf.setPersistenceEnabled(b);
         Assert.assertEquals(b, conf.isPersistenceEnabled());

         b = RandomUtil.randomBoolean();
         conf.setJMXManagementEnabled(b);
         Assert.assertEquals(b, conf.isJMXManagementEnabled());

         l = RandomUtil.randomLong();
         conf.setFileDeployerScanPeriod(l);
         Assert.assertEquals(l, conf.getFileDeployerScanPeriod());

         l = RandomUtil.randomLong();
         conf.setConnectionTTLOverride(l);
         Assert.assertEquals(l, conf.getConnectionTTLOverride());

         i = RandomUtil.randomInt();
         conf.setThreadPoolMaxSize(i);
         Assert.assertEquals(i, conf.getThreadPoolMaxSize());

         SimpleString ss = RandomUtil.randomSimpleString();
         conf.setManagementNotificationAddress(ss);
         Assert.assertEquals(ss, conf.getManagementNotificationAddress());

         s = RandomUtil.randomString();
         conf.setClusterUser(s);
         Assert.assertEquals(s, conf.getClusterUser());

         i = RandomUtil.randomInt();
         conf.setIDCacheSize(i);
         Assert.assertEquals(i, conf.getIDCacheSize());

         b = RandomUtil.randomBoolean();
         conf.setPersistIDCache(b);
         Assert.assertEquals(b, conf.isPersistIDCache());

         i = RandomUtil.randomInt();
         conf.setJournalCompactMinFiles(i);
         Assert.assertEquals(i, conf.getJournalCompactMinFiles());

         i = RandomUtil.randomInt();
         conf.setJournalCompactPercentage(i);
         Assert.assertEquals(i, conf.getJournalCompactPercentage());

         i = RandomUtil.randomInt();
         conf.setJournalBufferSize_AIO(i);
         Assert.assertEquals(i, conf.getJournalBufferSize_AIO());

         i = RandomUtil.randomInt();
         conf.setJournalBufferTimeout_AIO(i);
         Assert.assertEquals(i, conf.getJournalBufferTimeout_AIO());

         i = RandomUtil.randomInt();
         conf.setJournalBufferSize_NIO(i);
         Assert.assertEquals(i, conf.getJournalBufferSize_NIO());

         i = RandomUtil.randomInt();
         conf.setJournalBufferTimeout_NIO(i);
         Assert.assertEquals(i, conf.getJournalBufferTimeout_NIO());

         b = RandomUtil.randomBoolean();
         conf.setLogJournalWriteRate(b);
         Assert.assertEquals(b, conf.isLogJournalWriteRate());

         l = RandomUtil.randomLong();
         conf.setServerDumpInterval(l);
         Assert.assertEquals(l, conf.getServerDumpInterval());

         s = RandomUtil.randomString();
         conf.setPagingDirectory(s);
         Assert.assertEquals(s, conf.getPagingDirectory());

         s = RandomUtil.randomString();
         conf.setLargeMessagesDirectory(s);
         Assert.assertEquals(s, conf.getLargeMessagesDirectory());

         l = RandomUtil.randomLong();
         conf.setTransactionTimeout(l);
         Assert.assertEquals(l, conf.getTransactionTimeout());

         b = RandomUtil.randomBoolean();
         conf.setMessageCounterEnabled(b);
         Assert.assertEquals(b, conf.isMessageCounterEnabled());

         l = RandomUtil.randomPositiveLong();
         conf.setMessageCounterSamplePeriod(l);
         Assert.assertEquals(l, conf.getMessageCounterSamplePeriod());

         i = RandomUtil.randomInt();
         conf.setMessageCounterMaxDayHistory(i);
         Assert.assertEquals(i, conf.getMessageCounterMaxDayHistory());

         l = RandomUtil.randomLong();
         conf.setTransactionTimeoutScanPeriod(l);
         Assert.assertEquals(l, conf.getTransactionTimeoutScanPeriod());

         s = RandomUtil.randomString();
         conf.setClusterPassword(s);
         Assert.assertEquals(s, conf.getClusterPassword());

         i = RandomUtil.randomInt();
         conf.setPageSyncTimeout(i);
         Assert.assertEquals(i, conf.getPageSyncTimeout());
      }
   }

   @Test
   public void testGetSetInterceptors() {
      final String name1 = "uqwyuqywuy";
      final String name2 = "yugyugyguyg";

      conf.getIncomingInterceptorClassNames().add(name1);
      conf.getIncomingInterceptorClassNames().add(name2);

      Assert.assertTrue(conf.getIncomingInterceptorClassNames().contains(name1));
      Assert.assertTrue(conf.getIncomingInterceptorClassNames().contains(name2));
      Assert.assertFalse(conf.getIncomingInterceptorClassNames().contains("iijij"));
   }

   @Test
   public void testSerialize() throws Exception {
      boolean b = RandomUtil.randomBoolean();

      conf.setHAPolicyConfiguration(new LiveOnlyPolicyConfiguration());

      int i = RandomUtil.randomInt();
      conf.setScheduledThreadPoolMaxSize(i);
      Assert.assertEquals(i, conf.getScheduledThreadPoolMaxSize());

      long l = RandomUtil.randomLong();
      conf.setSecurityInvalidationInterval(l);
      Assert.assertEquals(l, conf.getSecurityInvalidationInterval());

      b = RandomUtil.randomBoolean();
      conf.setSecurityEnabled(b);
      Assert.assertEquals(b, conf.isSecurityEnabled());

      String s = RandomUtil.randomString();
      conf.setBindingsDirectory(s);
      Assert.assertEquals(s, conf.getBindingsDirectory());

      b = RandomUtil.randomBoolean();
      conf.setCreateBindingsDir(b);
      Assert.assertEquals(b, conf.isCreateBindingsDir());

      s = RandomUtil.randomString();
      conf.setJournalDirectory(s);
      Assert.assertEquals(s, conf.getJournalDirectory());

      b = RandomUtil.randomBoolean();
      conf.setCreateJournalDir(b);
      Assert.assertEquals(b, conf.isCreateJournalDir());

      i = RandomUtil.randomInt() % 2;
      JournalType journal = i == 0 ? JournalType.ASYNCIO : JournalType.NIO;
      conf.setJournalType(journal);
      Assert.assertEquals(journal, conf.getJournalType());

      b = RandomUtil.randomBoolean();
      conf.setJournalSyncTransactional(b);
      Assert.assertEquals(b, conf.isJournalSyncTransactional());

      b = RandomUtil.randomBoolean();
      conf.setJournalSyncNonTransactional(b);
      Assert.assertEquals(b, conf.isJournalSyncNonTransactional());

      i = RandomUtil.randomInt();
      conf.setJournalFileSize(i);
      Assert.assertEquals(i, conf.getJournalFileSize());

      i = RandomUtil.randomInt();
      conf.setJournalMinFiles(i);
      Assert.assertEquals(i, conf.getJournalMinFiles());

      i = RandomUtil.randomInt();
      conf.setJournalMaxIO_AIO(i);
      Assert.assertEquals(i, conf.getJournalMaxIO_AIO());

      i = RandomUtil.randomInt();
      conf.setJournalMaxIO_NIO(i);
      Assert.assertEquals(i, conf.getJournalMaxIO_NIO());

      s = RandomUtil.randomString();
      conf.setManagementAddress(new SimpleString(s));
      Assert.assertEquals(s, conf.getManagementAddress().toString());

      l = RandomUtil.randomLong();
      conf.setMessageExpiryScanPeriod(l);
      Assert.assertEquals(l, conf.getMessageExpiryScanPeriod());

      b = RandomUtil.randomBoolean();
      conf.setPersistDeliveryCountBeforeDelivery(b);
      Assert.assertEquals(b, conf.isPersistDeliveryCountBeforeDelivery());

      b = RandomUtil.randomBoolean();
      conf.setEnabledAsyncConnectionExecution(b);
      Assert.assertEquals(b, conf.isAsyncConnectionExecutionEnabled());

      b = RandomUtil.randomBoolean();
      conf.setPersistenceEnabled(b);
      Assert.assertEquals(b, conf.isPersistenceEnabled());

      b = RandomUtil.randomBoolean();
      conf.setJMXManagementEnabled(b);
      Assert.assertEquals(b, conf.isJMXManagementEnabled());

      l = RandomUtil.randomLong();
      conf.setFileDeployerScanPeriod(l);
      Assert.assertEquals(l, conf.getFileDeployerScanPeriod());

      l = RandomUtil.randomLong();
      conf.setConnectionTTLOverride(l);
      Assert.assertEquals(l, conf.getConnectionTTLOverride());

      i = RandomUtil.randomInt();
      conf.setThreadPoolMaxSize(i);
      Assert.assertEquals(i, conf.getThreadPoolMaxSize());

      SimpleString ss = RandomUtil.randomSimpleString();
      conf.setManagementNotificationAddress(ss);
      Assert.assertEquals(ss, conf.getManagementNotificationAddress());

      s = RandomUtil.randomString();
      conf.setClusterUser(s);
      Assert.assertEquals(s, conf.getClusterUser());

      i = RandomUtil.randomInt();
      conf.setIDCacheSize(i);
      Assert.assertEquals(i, conf.getIDCacheSize());

      b = RandomUtil.randomBoolean();
      conf.setPersistIDCache(b);
      Assert.assertEquals(b, conf.isPersistIDCache());

      i = RandomUtil.randomInt();
      conf.setJournalCompactMinFiles(i);
      Assert.assertEquals(i, conf.getJournalCompactMinFiles());

      i = RandomUtil.randomInt();
      conf.setJournalCompactPercentage(i);
      Assert.assertEquals(i, conf.getJournalCompactPercentage());

      i = RandomUtil.randomInt();
      conf.setJournalBufferSize_AIO(i);
      Assert.assertEquals(i, conf.getJournalBufferSize_AIO());

      i = RandomUtil.randomInt();
      conf.setJournalBufferTimeout_AIO(i);
      Assert.assertEquals(i, conf.getJournalBufferTimeout_AIO());

      i = RandomUtil.randomInt();
      conf.setJournalBufferSize_NIO(i);
      Assert.assertEquals(i, conf.getJournalBufferSize_NIO());

      i = RandomUtil.randomInt();
      conf.setJournalBufferTimeout_NIO(i);
      Assert.assertEquals(i, conf.getJournalBufferTimeout_NIO());

      b = RandomUtil.randomBoolean();
      conf.setLogJournalWriteRate(b);
      Assert.assertEquals(b, conf.isLogJournalWriteRate());

      l = RandomUtil.randomLong();
      conf.setServerDumpInterval(l);
      Assert.assertEquals(l, conf.getServerDumpInterval());

      s = RandomUtil.randomString();
      conf.setPagingDirectory(s);
      Assert.assertEquals(s, conf.getPagingDirectory());

      s = RandomUtil.randomString();
      conf.setLargeMessagesDirectory(s);
      Assert.assertEquals(s, conf.getLargeMessagesDirectory());

      b = RandomUtil.randomBoolean();
      conf.setWildcardRoutingEnabled(b);
      Assert.assertEquals(b, conf.isWildcardRoutingEnabled());

      l = RandomUtil.randomLong();
      conf.setTransactionTimeout(l);
      Assert.assertEquals(l, conf.getTransactionTimeout());

      b = RandomUtil.randomBoolean();
      conf.setMessageCounterEnabled(b);
      Assert.assertEquals(b, conf.isMessageCounterEnabled());

      l = RandomUtil.randomPositiveLong();
      conf.setMessageCounterSamplePeriod(l);
      Assert.assertEquals(l, conf.getMessageCounterSamplePeriod());

      i = RandomUtil.randomInt();
      conf.setMessageCounterMaxDayHistory(i);
      Assert.assertEquals(i, conf.getMessageCounterMaxDayHistory());

      l = RandomUtil.randomLong();
      conf.setTransactionTimeoutScanPeriod(l);
      Assert.assertEquals(l, conf.getTransactionTimeoutScanPeriod());

      s = RandomUtil.randomString();
      conf.setClusterPassword(s);
      Assert.assertEquals(s, conf.getClusterPassword());

      i = RandomUtil.randomInt();
      conf.setPageSyncTimeout(i);
      Assert.assertEquals(i, conf.getPageSyncTimeout());

      conf.registerBrokerPlugin(new LoggingActiveMQServerPlugin());
      Assert.assertEquals("ensure one plugin registered", 1, conf.getBrokerPlugins().size());

      // This will use serialization to perform a deep copy of the object
      Configuration conf2 = conf.copy();

      Assert.assertTrue(conf.equals(conf2));
   }

   @Test
   public void testResolvePath() throws Throwable {
      // Validate that the resolve method will work even with artemis.instance doesn't exist

      String oldProperty = System.getProperty("artemis.instance");

      try {
         System.setProperty("artemis.instance", "/tmp/" + RandomUtil.randomString());
         ConfigurationImpl configuration = new ConfigurationImpl();
         configuration.setJournalDirectory("./data-journal");
         File journalLocation = configuration.getJournalLocation();
         Assert.assertFalse("This path shouldn't resolve to a real folder", journalLocation.exists());
         Assert.assertEquals(configuration.getJournalLocation(), configuration.getNodeManagerLockLocation());
         Assert.assertFalse(configuration.getNodeManagerLockLocation().exists());
         configuration.setNodeManagerLockDirectory("./lock-folder");
         Assert.assertNotEquals(configuration.getJournalLocation(), configuration.getNodeManagerLockLocation());
         Assert.assertFalse("This path shouldn't resolve to a real folder", configuration.getNodeManagerLockLocation().exists());
      } finally {
         if (oldProperty == null) {
            System.clearProperty("artemis.instance");
         } else {
            System.setProperty("artemis.instance", oldProperty);
         }
      }

   }

   @Test
   public void testAbsolutePath() throws Throwable {
      // Validate that the resolve method will work even with artemis.instance doesn't exist

      String oldProperty = System.getProperty("artemis.instance");
      String oldEtc = System.getProperty("artemis.instance.etc");

      File tempFolder = null;
      try {
         System.setProperty("artemis.instance", "/tmp/" + RandomUtil.randomString());
         tempFolder = File.createTempFile("journal-folder", "", temporaryFolder.getRoot());
         tempFolder.delete();

         tempFolder = new File(tempFolder.getAbsolutePath());
         tempFolder.mkdirs();

         log.debug("TempFolder = " + tempFolder.getAbsolutePath());

         ConfigurationImpl configuration = new ConfigurationImpl();
         configuration.setJournalDirectory(tempFolder.getAbsolutePath());
         File journalLocation = configuration.getJournalLocation();

         Assert.assertTrue(journalLocation.exists());
         Assert.assertEquals(configuration.getJournalLocation(), configuration.getNodeManagerLockLocation());
         Assert.assertTrue(configuration.getNodeManagerLockLocation().exists());

         tempFolder = File.createTempFile("lock-folder", "", temporaryFolder.getRoot());
         tempFolder.delete();

         tempFolder.getAbsolutePath();

         tempFolder = new File(tempFolder.getAbsolutePath());
         tempFolder.mkdirs();

         log.debug("TempFolder = " + tempFolder.getAbsolutePath());
         configuration.setNodeManagerLockDirectory(tempFolder.getAbsolutePath());
         File lockLocation = configuration.getNodeManagerLockLocation();
         Assert.assertTrue(lockLocation.exists());

      } finally {
         if (oldProperty == null) {
            System.clearProperty("artemis.instance");
            System.clearProperty("artemis.instance.etc");
         } else {
            System.setProperty("artemis.instance", oldProperty);
            System.setProperty("artemis.instance.etc", oldEtc);
         }

         if (tempFolder != null) {
            tempFolder.delete();
         }
      }

   }

   @Test
   public void testRootPrimitives() throws Exception {
      ConfigurationImpl configuration = new ConfigurationImpl();
      Properties properties = new Properties();
      Method[] declaredMethods = Configuration.class.getDeclaredMethods();
      HashMap<String, Object> props = new HashMap<>();
      int nextInt = 1;
      long nextLong = 1;
      // add random entries for all root primitive bean properties
      for (Method declaredMethod : declaredMethods) {
         if (declaredMethod.getName().startsWith("set") && declaredMethod.getAnnotation(Deprecated.class) == null &&
                 declaredMethod.getParameterCount() == 1 && (ClassUtils.isPrimitiveOrWrapper(declaredMethod.getParameters()[0].getType())
                 || declaredMethod.getParameters()[0].getType().equals(String.class))) {
            String prop = declaredMethod.getName().substring(3);
            prop = Character.toLowerCase(prop.charAt(0)) +
                    (prop.length() > 1 ? prop.substring(1) : "");
            Class<?> type = declaredMethod.getParameters()[0].getType();
            if (type.equals(Boolean.class)) {
               properties.put(prop, Boolean.TRUE);
            } else if (type.equals(boolean.class)) {
               properties.put(prop, true);
            } else if (type.equals(Long.class) || type.equals(long.class)) {
               properties.put(prop, nextLong++);
            } else if (type.equals(Integer.class) || type.equals(int.class)) {
               properties.put(prop, nextInt++);
            } else if (type.equals(String.class)) {
               byte[] array = new byte[7]; // length is bounded by 7
               new Random().nextBytes(array);
               String generatedString = new String(array, Charset.forName("UTF-8"));

               properties.put(prop, generatedString);
            }
         }
      }
      // now parse
      configuration.parsePrefixedProperties(properties, null);

      //then call the getter to make sure it gets set
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
         String methodName = entry.getKey().toString();
         methodName = Character.toUpperCase(methodName.charAt(0)) +
                 (methodName.length() > 1 ? methodName.substring(1) : "");
         if (entry.getValue().getClass() == Boolean.class || entry.getValue().getClass() == boolean.class) {
            methodName = "is" + methodName;
         } else {
            methodName = "get" + methodName;
         }

         Method declaredMethod = ConfigurationImpl.class.getDeclaredMethod(methodName);
         Object value = declaredMethod.invoke(configuration);
         Assert.assertEquals(value, properties.get(entry.getKey()));
      }
   }

   @Test
   public void testSetSystemProperty() throws Throwable {
      ConfigurationImpl configuration = new ConfigurationImpl();
      Properties properties = new Properties();
      properties.put(configuration.getSystemPropertyPrefix() + "fileDeployerScanPeriod", "1234");
      properties.put(configuration.getSystemPropertyPrefix() + "globalMaxSize", "4321");

      configuration.parsePrefixedProperties(properties, configuration.getSystemPropertyPrefix());

      Assert.assertEquals(1234, configuration.getFileDeployerScanPeriod());
      Assert.assertEquals(4321, configuration.getGlobalMaxSize());
   }

   @Test
   public void testSetConnectionRoutersPolicyConfiguration() throws Throwable {
      testSetConnectionRoutersPolicyConfiguration(new ConfigurationImpl());
   }

   @Test
   public void testSetConnectionRoutersPolicyFileConfiguration() throws Throwable {
      testSetConnectionRoutersPolicyConfiguration(new FileConfiguration());
   }

   private void testSetConnectionRoutersPolicyConfiguration(ConfigurationImpl configuration) throws Throwable {
      Properties insertionOrderedProperties = new ConfigurationImpl.InsertionOrderedProperties();
      insertionOrderedProperties.put("connectionRouters.autoShard.localTargetFilter", "NULL|$STATEFUL_SET_ORDINAL");
      insertionOrderedProperties.put("connectionRouters.autoShard.keyType", KeyType.CLIENT_ID);
      insertionOrderedProperties.put("connectionRouters.autoShard.policyConfiguration", ConsistentHashModuloPolicy.NAME);
      insertionOrderedProperties.put("connectionRouters.autoShard.policyConfiguration.properties." + ConsistentHashModuloPolicy.MODULO, 2);

      configuration.parsePrefixedProperties(insertionOrderedProperties, null);

      Assert.assertEquals(1, configuration.getConnectionRouters().size());
      Assert.assertEquals(KeyType.CLIENT_ID, configuration.getConnectionRouters().get(0).getKeyType());
      Assert.assertEquals("2", configuration.getConnectionRouters().get(0).getPolicyConfiguration().getProperties().get(ConsistentHashModuloPolicy.MODULO));
   }

   @Test
   public void testAMQPConnectionsConfiguration() throws Throwable {
      ConfigurationImpl configuration = new ConfigurationImpl();

      Properties insertionOrderedProperties = new ConfigurationImpl.InsertionOrderedProperties();
      insertionOrderedProperties.put("AMQPConnections.target.uri", "localhost:61617");
      insertionOrderedProperties.put("AMQPConnections.target.retryInterval", 55);
      insertionOrderedProperties.put("AMQPConnections.target.reconnectAttempts", -2);
      insertionOrderedProperties.put("AMQPConnections.target.user", "admin");
      insertionOrderedProperties.put("AMQPConnections.target.password", "password");
      insertionOrderedProperties.put("AMQPConnections.target.autoStart", "true");
      insertionOrderedProperties.put("AMQPConnections.target.connectionElements.mirror.type", "MIRROR");
      insertionOrderedProperties.put("AMQPConnections.target.connectionElements.mirror.messageAcknowledgements", "true");
      insertionOrderedProperties.put("AMQPConnections.target.connectionElements.mirror.queueCreation", "true");
      insertionOrderedProperties.put("AMQPConnections.target.connectionElements.mirror.queueRemoval", "true");
      insertionOrderedProperties.put("AMQPConnections.target.connectionElements.mirror.addressFilter", "foo");

      configuration.parsePrefixedProperties(insertionOrderedProperties, null);

      Assert.assertEquals(1, configuration.getAMQPConnections().size());
      AMQPBrokerConnectConfiguration connectConfiguration = configuration.getAMQPConnections().get(0);
      Assert.assertEquals("target", connectConfiguration.getName());
      Assert.assertEquals("localhost:61617", connectConfiguration.getUri());
      Assert.assertEquals(55, connectConfiguration.getRetryInterval());
      Assert.assertEquals(-2, connectConfiguration.getReconnectAttempts());
      Assert.assertEquals("admin", connectConfiguration.getUser());
      Assert.assertEquals("password", connectConfiguration.getPassword());
      Assert.assertEquals(1,connectConfiguration.getConnectionElements().size());
      AMQPBrokerConnectionElement amqpBrokerConnectionElement = connectConfiguration.getConnectionElements().get(0);
      Assert.assertTrue(amqpBrokerConnectionElement instanceof AMQPMirrorBrokerConnectionElement);
      AMQPMirrorBrokerConnectionElement amqpMirrorBrokerConnectionElement = (AMQPMirrorBrokerConnectionElement) amqpBrokerConnectionElement;
      Assert.assertEquals("mirror", amqpMirrorBrokerConnectionElement.getName());
      Assert.assertEquals(true, amqpMirrorBrokerConnectionElement.isMessageAcknowledgements());
      Assert.assertEquals(true, amqpMirrorBrokerConnectionElement.isQueueCreation());
      Assert.assertEquals(true, amqpMirrorBrokerConnectionElement.isQueueRemoval());
      Assert.assertEquals("foo", amqpMirrorBrokerConnectionElement.getAddressFilter());
   }

   @Test
   public void testCoreBridgeConfiguration() throws Throwable {
      ConfigurationImpl configuration = new ConfigurationImpl();

      final String queueName = "q";
      final String forwardingAddress = "fa";

      Properties properties = new ConfigurationImpl.InsertionOrderedProperties();

      properties.put("bridgeConfigurations.b1.queueName", queueName);
      properties.put("bridgeConfigurations.b1.forwardingAddress", forwardingAddress);
      properties.put("bridgeConfigurations.b1.confirmationWindowSize", "10");
      properties.put("bridgeConfigurations.b1.routingType", "STRIP");  // enum
      // this is a List<String> from comma sep value
      properties.put("bridgeConfigurations.b1.staticConnectors", "a,b");
      // flip b in place
      properties.put("bridgeConfigurations.b1.staticConnectors[1]", "c");

      configuration.parsePrefixedProperties(properties, null);

      Assert.assertEquals(1, configuration.getBridgeConfigurations().size());
      Assert.assertEquals(queueName, configuration.getBridgeConfigurations().get(0).getQueueName());

      Assert.assertEquals(forwardingAddress, configuration.getBridgeConfigurations().get(0).getForwardingAddress());
      Assert.assertEquals(10, configuration.getBridgeConfigurations().get(0).getConfirmationWindowSize());
      Assert.assertEquals(2, configuration.getBridgeConfigurations().get(0).getStaticConnectors().size());
      Assert.assertEquals("a", configuration.getBridgeConfigurations().get(0).getStaticConnectors().get(0));
      Assert.assertEquals("c", configuration.getBridgeConfigurations().get(0).getStaticConnectors().get(1));

      Assert.assertEquals(ComponentConfigurationRoutingType.STRIP, configuration.getBridgeConfigurations().get(0).getRoutingType());
   }

   @Test
   public void testFederationUpstreamConfiguration() throws Throwable {
      ConfigurationImpl configuration = new ConfigurationImpl();

      Properties properties = new ConfigurationImpl.InsertionOrderedProperties();

      properties.put("federationConfigurations.f1.upstreamConfigurations.joe.connectionConfiguration.reconnectAttempts", "1");
      properties.put("federationConfigurations.f1.upstreamConfigurations.joe.connectionConfiguration.staticConnectors", "a,b,c");
      properties.put("federationConfigurations.f1.upstreamConfigurations.joe.policyRefs", "pq1,pq2");

      properties.put("federationConfigurations.f1.queuePolicies.qp1.transformerRef", "simpleTransform");
      properties.put("federationConfigurations.f1.queuePolicies.qp2.includes.all-N.queueMatch", "N#");

      properties.put("federationConfigurations.f1.addressPolicies.a1.transformerRef", "simpleTransform");
      properties.put("federationConfigurations.f1.addressPolicies.a1.excludes.just-b.addressMatch", "b");

      properties.put("federationConfigurations.f1.policySets.combined.policyRefs", "qp1,qp2,a1");

      properties.put("federationConfigurations.f1.transformerConfigurations.simpleTransform.transformerConfiguration.className", "a.b");
      properties.put("federationConfigurations.f1.transformerConfigurations.simpleTransform.transformerConfiguration.properties.a", "b");

      properties.put("federationConfigurations.f1.credentials.user", "u");
      properties.put("federationConfigurations.f1.credentials.password", "ENC(2a7c211d21c295cdbcde3589c205decb)");

      configuration.parsePrefixedProperties(properties, null);

      Assert.assertEquals(1, configuration.getFederationConfigurations().size());
      Assert.assertEquals(1, configuration.getFederationConfigurations().get(0).getUpstreamConfigurations().get(0).getConnectionConfiguration().getReconnectAttempts());
      Assert.assertEquals(3, configuration.getFederationConfigurations().get(0).getUpstreamConfigurations().get(0).getConnectionConfiguration().getStaticConnectors().size());

      Assert.assertEquals(2, configuration.getFederationConfigurations().get(0).getUpstreamConfigurations().get(0).getPolicyRefs().size());

      Assert.assertEquals(4, configuration.getFederationConfigurations().get(0).getFederationPolicyMap().size());
      Assert.assertEquals("qp1", configuration.getFederationConfigurations().get(0).getFederationPolicyMap().get("qp1").getName());

      Assert.assertEquals("combined", configuration.getFederationConfigurations().get(0).getFederationPolicyMap().get("combined").getName());
      Assert.assertEquals(3, ((FederationPolicySet)configuration.getFederationConfigurations().get(0).getFederationPolicyMap().get("combined")).getPolicyRefs().size());

      Assert.assertEquals("simpleTransform", ((FederationQueuePolicyConfiguration)configuration.getFederationConfigurations().get(0).getFederationPolicyMap().get("qp1")).getTransformerRef());

      Assert.assertEquals("N#", ((FederationQueuePolicyConfiguration.Matcher)((FederationQueuePolicyConfiguration)configuration.getFederationConfigurations().get(0).getFederationPolicyMap().get("qp2")).getIncludes().toArray()[0]).getQueueMatch());
      Assert.assertEquals("b", ((FederationAddressPolicyConfiguration.Matcher)((FederationAddressPolicyConfiguration)configuration.getFederationConfigurations().get(0).getFederationPolicyMap().get("a1")).getExcludes().toArray()[0]).getAddressMatch());

      Assert.assertEquals("b", configuration.getFederationConfigurations().get(0).getTransformerConfigurations().get("simpleTransform").getTransformerConfiguration().getProperties().get("a"));
      Assert.assertEquals("a.b", configuration.getFederationConfigurations().get(0).getTransformerConfigurations().get("simpleTransform").getTransformerConfiguration().getClassName());

      Assert.assertEquals("u", configuration.getFederationConfigurations().get(0).getCredentials().getUser());
      Assert.assertEquals("secureexample", configuration.getFederationConfigurations().get(0).getCredentials().getPassword());
   }

   @Test
   public void testSetNestedPropertyOnCollections() throws Throwable {
      ConfigurationImpl configuration = new ConfigurationImpl();

      Properties properties = new Properties();
      properties.put("connectionRouters.joe.localTargetFilter", "LF");
      properties.put("connectionRouters.joe.keyFilter", "TF");
      properties.put("connectionRouters.joe.keyType", "SOURCE_IP");

      properties.put("acceptorConfigurations.tcp.params.HOST", "LOCALHOST");
      properties.put("acceptorConfigurations.tcp.params.PORT", "61616");

      properties.put("acceptorConfigurations.invm.params.ID", "0");

      //   <amqp-connection uri="tcp://HOST:PORT" name="other-server" retry-interval="100" reconnect-attempts="-1" user="john" password="doe">
      properties.put("AMQPConnections.other-server.uri", "tcp://HOST:PORT");
      properties.put("AMQPConnections.other-server.retryInterval", "100");
      properties.put("AMQPConnections.other-server.reconnectAttempts", "100");
      properties.put("AMQPConnections.other-server.user", "john");
      properties.put("AMQPConnections.other-server.password", "doe");

      //   <amqp-connection uri="tcp://brokerB:5672" name="brokerB"> <mirror/> </amqp-connection>
      properties.put("AMQPConnections.brokerB.uri", "tcp://brokerB:5672");
      properties.put("AMQPConnections.brokerB.type", AMQPBrokerConnectionAddressType.MIRROR.toString());
      properties.put("AMQPConnections.brokerB.connectionElements.mirror.mirrorSNF", "mirrorSNFQueue");

      properties.put("resourceLimitSettings.joe.maxConnections", "100");

      configuration.parsePrefixedProperties(properties, null);

      Assert.assertEquals(1, configuration.getConnectionRouters().size());
      Assert.assertEquals("LF", configuration.getConnectionRouters().get(0).getLocalTargetFilter());
      Assert.assertEquals("TF", configuration.getConnectionRouters().get(0).getKeyFilter());
      Assert.assertEquals(KeyType.SOURCE_IP, configuration.getConnectionRouters().get(0).getKeyType());

      Assert.assertEquals(2, configuration.getAcceptorConfigurations().size());

      for (TransportConfiguration acceptor : configuration.getAcceptorConfigurations()) {
         if ("tcp".equals(acceptor.getName())) {
            Assert.assertEquals("61616", acceptor.getParams().get("PORT"));
         }
         if ("invm".equals(acceptor.getName())) {
            Assert.assertEquals("0", acceptor.getParams().get("ID"));
         }
      }

      Assert.assertEquals(2, configuration.getAMQPConnection().size());
      for (AMQPBrokerConnectConfiguration amqpBrokerConnectConfiguration : configuration.getAMQPConnection()) {
         if ("brokerB".equals(amqpBrokerConnectConfiguration.getName())) {
            Assert.assertEquals(AMQPBrokerConnectionAddressType.MIRROR.toString(), amqpBrokerConnectConfiguration.getConnectionElements().get(0).getType().toString());
            Assert.assertEquals("mirrorSNFQueue", ((AMQPMirrorBrokerConnectionElement)amqpBrokerConnectConfiguration.getConnectionElements().get(0)).getMirrorSNF().toString());

         } else if ("other-server".equals(amqpBrokerConnectConfiguration.getName())) {
            Assert.assertEquals(100, amqpBrokerConnectConfiguration.getReconnectAttempts());
         } else {
            fail("unexpected amqp broker connection configuration: " + amqpBrokerConnectConfiguration.getName());
         }
      }

      Assert.assertEquals(100, configuration.getResourceLimitSettings().get("joe").getMaxConnections());
   }

   @Test
   public void testSetNestedPropertyOnExistingCollectionEntryViaMappedNotation() throws Throwable {
      ConfigurationImpl configuration = new ConfigurationImpl();

      Properties properties = new Properties();
      properties.put("connectionRouters.joe.localTargetFilter", "LF");
      // does not exist, ignored
      properties.put("connectionRouters(bob).keyFilter", "TF");

      // apply twice b/c there is no guarantee of order, this may be a problem
      configuration.parsePrefixedProperties(properties, null);

      properties = new Properties();
      // update existing
      properties.put("connectionRouters(joe).keyFilter", "TF");

      configuration.parsePrefixedProperties(properties, null);

      Assert.assertEquals(1, configuration.getConnectionRouters().size());
      Assert.assertEquals("LF", configuration.getConnectionRouters().get(0).getLocalTargetFilter());
      Assert.assertEquals("TF", configuration.getConnectionRouters().get(0).getKeyFilter());
   }


   @Test
   public void testAddressViaProperties() throws Throwable {
      ConfigurationImpl configuration = new ConfigurationImpl();

      Properties properties = new Properties();

      properties.put("addressConfigurations.\"LB.TEST\".queueConfigs.\"LB.TEST\".routingType", "ANYCAST");
      properties.put("addressConfigurations.\"LB.TEST\".queueConfigs.\"LB.TEST\".durable", "false");

      configuration.parsePrefixedProperties(properties, null);

      Assert.assertEquals(1, configuration.getAddressConfigurations().size());
      Assert.assertEquals(1, configuration.getAddressConfigurations().get(0).getQueueConfigs().size());
      Assert.assertEquals(SimpleString.toSimpleString("LB.TEST"), configuration.getAddressConfigurations().get(0).getQueueConfigs().get(0).getAddress());
      Assert.assertEquals(false, configuration.getAddressConfigurations().get(0).getQueueConfigs().get(0).isDurable());
   }


   @Test
   public void testAcceptorViaProperties() throws Throwable {
      ConfigurationImpl configuration = new ConfigurationImpl();

      configuration.getAcceptorConfigurations().add(ConfigurationUtils.parseAcceptorURI(
         "artemis", "tcp://0.0.0.0:61616?protocols=CORE,AMQP,STOMP,HORNETQ,MQTT,OPENWIRE;supportAdvisory=false;suppressInternalManagementObjects=false").get(0));

      Properties properties = new Properties();

      properties.put("acceptorConfigurations.artemis.extraParams.supportAdvisory", "true");
      properties.put("acceptorConfigurations.new.extraParams.supportAdvisory", "true");

      configuration.parsePrefixedProperties(properties, null);

      Assert.assertEquals(2, configuration.getAcceptorConfigurations().size());

      TransportConfiguration artemisTransportConfiguration = configuration.getAcceptorConfigurations().stream().filter(
         transportConfiguration -> transportConfiguration.getName().equals("artemis")).findFirst().get();
      Assert.assertTrue(artemisTransportConfiguration.getParams().containsKey("protocols"));
      Assert.assertEquals("CORE,AMQP,STOMP,HORNETQ,MQTT,OPENWIRE", artemisTransportConfiguration.getParams().get("protocols"));
      Assert.assertTrue(artemisTransportConfiguration.getExtraParams().containsKey("supportAdvisory"));
      Assert.assertEquals("true", artemisTransportConfiguration.getExtraParams().get("supportAdvisory"));
      Assert.assertTrue(artemisTransportConfiguration.getExtraParams().containsKey("suppressInternalManagementObjects"));
      Assert.assertEquals("false", artemisTransportConfiguration.getExtraParams().get("suppressInternalManagementObjects"));

      TransportConfiguration newTransportConfiguration = configuration.getAcceptorConfigurations().stream().filter(
         transportConfiguration -> transportConfiguration.getName().equals("new")).findFirst().get();
      Assert.assertTrue(newTransportConfiguration.getExtraParams().containsKey("supportAdvisory"));
      Assert.assertEquals("true", newTransportConfiguration.getExtraParams().get("supportAdvisory"));
   }


   @Test
   public void testAddressSettingsViaProperties() throws Throwable {
      ConfigurationImpl configuration = new ConfigurationImpl();

      Properties properties = new Properties();

      properties.put("addressesSettings.#.expiryAddress", "sharedExpiry"); // verify @Deprecation double plural still works
      properties.put("addressSettings.NeedToTrackExpired.expiryAddress", "important");
      properties.put("addressSettings.\"Name.With.Dots\".expiryAddress", "moreImportant");

      configuration.parsePrefixedProperties(properties, null);

      Assert.assertEquals(3, configuration.getAddressSettings().size());
      Assert.assertEquals(SimpleString.toSimpleString("sharedExpiry"), configuration.getAddressSettings().get("#").getExpiryAddress());
      Assert.assertEquals(SimpleString.toSimpleString("important"), configuration.getAddressSettings().get("NeedToTrackExpired").getExpiryAddress());
      Assert.assertEquals(SimpleString.toSimpleString("moreImportant"), configuration.getAddressSettings().get("Name.With.Dots").getExpiryAddress());
   }

   @Test
   public void testDivertViaProperties() throws Exception {
      ConfigurationImpl configuration = new ConfigurationImpl();


      final String routingName = "divert1";
      final String address = "testAddress";
      final String forwardAddress = "forwardAddress";
      final String className = "s.o.m.e.class";

      Properties properties = new ConfigurationImpl.InsertionOrderedProperties();

      properties.put("divertConfigurations.divert1.routingName", routingName);
      properties.put("divertConfigurations.divert1.address", address);
      properties.put("divertConfigurations.divert1.forwardingAddress", forwardAddress);
      properties.put("divertConfigurations.divert1.transformerConfiguration", className);
      properties.put("divertConfigurations.divert1.transformerConfiguration.properties.a", "va");
      properties.put("divertConfigurations.divert1.transformerConfiguration.properties.b", "vb");

      configuration.parsePrefixedProperties(properties, null);

      Assert.assertEquals(1, configuration.getDivertConfigurations().size());
      Assert.assertEquals(routingName, configuration.getDivertConfigurations().get(0).getRoutingName());
      Assert.assertEquals(address, configuration.getDivertConfigurations().get(0).getAddress());
      Assert.assertEquals(forwardAddress, configuration.getDivertConfigurations().get(0).getForwardingAddress());

      Assert.assertEquals(className, configuration.getDivertConfigurations().get(0).getTransformerConfiguration().getClassName());
      Assert.assertEquals("va", configuration.getDivertConfigurations().get(0).getTransformerConfiguration().getProperties().get("a"));
      Assert.assertEquals("vb", configuration.getDivertConfigurations().get(0).getTransformerConfiguration().getProperties().get("b"));
   }

   @Test
   public void testRoleSettingsViaProperties() throws Exception {
      ConfigurationImpl configuration = new ConfigurationImpl();

      Properties properties = new Properties();

      properties.put("securityRoles.TEST.users.send", "true");
      properties.put("securityRoles.TEST.users.consume", "true");

      configuration.parsePrefixedProperties(properties, null);

      Assert.assertEquals(1, configuration.getSecurityRoles().size());
      Assert.assertEquals(1, configuration.getSecurityRoles().get("TEST").size());
      Assert.assertTrue(configuration.getSecurityRoles().get("TEST").stream().findFirst().orElse(null).isConsume());
      Assert.assertTrue(configuration.getSecurityRoles().get("TEST").stream().findFirst().orElse(null).isSend());
      Assert.assertFalse(configuration.getSecurityRoles().get("TEST").stream().findFirst().orElse(null).isCreateAddress());
   }

   @Test
   public void testRoleAugmentViaProperties() throws Exception {

      final String xmlConfig = "<configuration xmlns=\"urn:activemq\"\n" +
         "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
         "xsi:schemaLocation=\"urn:activemq /schema/artemis-configuration.xsd\">\n" +
         "<security-settings>" + "\n" +
         "<security-setting match=\"#\">" + "\n" +
         "<permission type=\"consume\" roles=\"guest\"/>" + "\n" +
         "<permission type=\"send\" roles=\"guest\"/>" + "\n" +
         "</security-setting>" + "\n" +
         "</security-settings>" + "\n" +
         "</configuration>";

      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream input = new ByteArrayInputStream(xmlConfig.getBytes(StandardCharsets.UTF_8));

      ConfigurationImpl configuration = (ConfigurationImpl) parser.parseMainConfig(input);
      Properties properties = new Properties();

      // new entry
      properties.put("securityRoles.TEST.users.send", "true");
      properties.put("securityRoles.TEST.users.consume", "false");

      // modify existing role
      properties.put("securityRoles.#.guest.consume", "false");

      // modify with new role
      properties.put("securityRoles.#.users.send", "true");

      configuration.parsePrefixedProperties(properties, null);

      // verify new addition
      Assert.assertEquals(2, configuration.getSecurityRoles().size());
      Assert.assertEquals(1, configuration.getSecurityRoles().get("TEST").size());
      Assert.assertFalse(configuration.getSecurityRoles().get("TEST").stream().findFirst().orElse(null).isConsume());
      Assert.assertTrue(configuration.getSecurityRoles().get("TEST").stream().findFirst().orElse(null).isSend());

      // verify augmentation
      Assert.assertEquals(2, configuration.getSecurityRoles().get("#").size());
      Set roles = configuration.getSecurityRoles().get("#");
      class RolePredicate implements Predicate<Role> {
         final String roleName;
         RolePredicate(String name) {
            this.roleName = name;
         }
         @Override
         public boolean test(Role role) {
            return roleName.equals(role.getName()) && !role.isConsume() && role.isSend() && !role.isCreateAddress();
         }
      }
      Assert.assertEquals(1L, roles.stream().filter(new RolePredicate("guest")).count());
      Assert.assertEquals(1L, roles.stream().filter(new RolePredicate("users")).count());
   }

   @Test
   public void testValuePostFixModifier() throws Throwable {
      ConfigurationImpl configuration = new ConfigurationImpl();

      Properties properties = new Properties();

      properties.put("globalMaxSize", "25K");

      configuration.parsePrefixedProperties(properties, null);

      Assert.assertEquals(25 * 1024, configuration.getGlobalMaxSize());
   }

   @Test
   public void testSystemPropValueReplaced() throws Exception {
      ConfigurationImpl configuration = new ConfigurationImpl();
      Properties properties = new Properties();
      final String homeFromDefault = "default-home";
      final String homeFromEnv = System.getenv("HOME");
      properties.put("name", "${HOME:" + homeFromDefault + "}");
      configuration.parsePrefixedProperties(properties, null);
      if (homeFromEnv != null) {
         Assert.assertEquals(homeFromEnv, configuration.getName());
      } else {
         // if $HOME is not set for some platform
         Assert.assertEquals(homeFromDefault, configuration.getName());
      }
   }

   @Test
   public void testSystemPropValueNoMatch() throws Exception {
      ConfigurationImpl configuration = new ConfigurationImpl();
      Properties properties = new Properties();
      properties.put("name", "vv-${SOME_RANDOM_VV}");
      configuration.parsePrefixedProperties(properties, null);
      Assert.assertEquals("vv-", configuration.getName());
   }

   @Test
   public void testSystemPropValueNonExistWithDefault() throws Exception {
      ConfigurationImpl configuration = new ConfigurationImpl();
      Properties properties = new Properties();
      properties.put("name", "vv-${SOME_RANDOM_VV:y}");
      configuration.parsePrefixedProperties(properties, null);
      Assert.assertEquals("vv-y", configuration.getName());
   }


   @Test
   public void testSystemPropKeyReplacement() throws Exception {
      ConfigurationImpl configuration = new ConfigurationImpl();
      Properties properties = new Properties();

      final String newKeyName = RandomUtil.randomString();
      final String valueFromSysProp = "VV";
      System.setProperty(newKeyName, valueFromSysProp);

      try {
         properties.put("connectorConfigurations.KEY-${" + newKeyName + "}.name", "y");
         configuration.parsePrefixedProperties(properties, null);
         Assert.assertNotNull("configured new key from prop", configuration.connectorConfigs.get("KEY-" + valueFromSysProp));
         Assert.assertEquals("y", configuration.connectorConfigs.get("KEY-" + valueFromSysProp).getName());
      } finally {
         System.clearProperty(newKeyName);
      }
   }

   @Test
   public void testEnumConversion() throws Exception {
      ConfigurationImpl configuration = new ConfigurationImpl();
      Properties properties = new Properties();
      properties.put("clusterConfiguration.cc.name", "cc");
      properties.put("clusterConfigurations.cc.messageLoadBalancingType", "OFF_WITH_REDISTRIBUTION");
      properties.put("criticalAnalyzerPolicy", "SHUTDOWN");

      configuration.parsePrefixedProperties(properties, null);

      Assert.assertEquals("cc", configuration.getClusterConfigurations().get(0).getName());
      Assert.assertEquals(MessageLoadBalancingType.OFF_WITH_REDISTRIBUTION, configuration.getClusterConfigurations().get(0).getMessageLoadBalancingType());
      Assert.assertEquals(CriticalAnalyzerPolicy.SHUTDOWN, configuration.getCriticalAnalyzerPolicy());
   }

   @Test
   public void testPropertiesReaderRespectsOrderFromFile() throws Exception {

      File tmpFile = File.createTempFile("ordered-props-test", "", temporaryFolder.getRoot());

      FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
      PrintWriter printWriter = new PrintWriter(fileOutputStream);

      LinkedList<String> insertionOrderedKeys = new LinkedList<>();
      char ascii = 'a';
      for (int i = 0; i < 26; i++, ascii++) {
         printWriter.println("resourceLimitSettings." + i + ".maxConnections=100");
         insertionOrderedKeys.addLast(String.valueOf(i));

         printWriter.println("resourceLimitSettings." + ascii + ".maxConnections=100");
         insertionOrderedKeys.addLast(String.valueOf(ascii));
      }
      printWriter.flush();
      fileOutputStream.flush();
      fileOutputStream.close();

      final AtomicReference<String> errorAt = new AtomicReference<>();
      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setResourceLimitSettings(new HashMap<String, ResourceLimitSettings>() {
         @Override
         public ResourceLimitSettings put(String key, ResourceLimitSettings value) {
            if (!(key.equals(insertionOrderedKeys.remove()))) {
               errorAt.set(key);
               fail("Expected to see props applied in insertion order!, errorAt:" + errorAt.get());
            }
            return super.put(key, value);
         }
      });
      configuration.parseProperties(tmpFile.getAbsolutePath());
      assertNull("no errors in insertion order, errorAt:" + errorAt.get(), errorAt.get());
   }


   @Test
   public void testPropertiesFiles() throws Exception {

      LinkedList<String> files = new LinkedList<>();
      LinkedList<String> names = new LinkedList<>();
      names.addLast("one");
      names.addLast("two");

      for (String suffix : names) {
         File tmpFile = File.createTempFile("props-test", suffix, temporaryFolder.getRoot());

         FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
         PrintWriter printWriter = new PrintWriter(fileOutputStream);

         printWriter.println("name=" + suffix);

         printWriter.flush();
         fileOutputStream.flush();
         fileOutputStream.close();

         files.addLast(tmpFile.getAbsolutePath());
      }
      ConfigurationImpl configuration = new ConfigurationImpl() {
         @Override
         public ConfigurationImpl setName(String name) {
            if (!(name.equals(names.remove()))) {
               fail("Expected names from files in order");
            }
            return super.setName(name);
         }
      };
      configuration.parseProperties(files.stream().collect(Collectors.joining(",")));
      assertEquals("second won", "two", configuration.getName());
   }

   @Test
   public void testPropertiesFilesInDir() throws Exception {

      LinkedList<String> files = new LinkedList<>();
      LinkedList<String> names = new LinkedList<>();
      names.addLast("a_one");
      names.addLast("b_two");

      for (String name : names) {
         File tmpFile = File.createTempFile(name, ".properties", temporaryFolder.getRoot());

         FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
         PrintWriter printWriter = new PrintWriter(fileOutputStream);

         printWriter.println("name=" + name);

         printWriter.flush();
         fileOutputStream.flush();
         fileOutputStream.close();
         files.addLast(tmpFile.getAbsolutePath());
      }
      ConfigurationImpl configuration = new ConfigurationImpl() {
         @Override
         public ConfigurationImpl setName(String name) {
            if (!(name.equals(names.remove()))) {
               fail("Expected names from files in order");
            }
            return super.setName(name);
         }
      };
      configuration.parseProperties(temporaryFolder.getRoot() + "/");
      assertEquals("second won", "b_two", configuration.getName());
      assertTrue("all names applied", names.isEmpty());
   }

   @Test
   public void testNameWithDotsSurroundWithDollarDollar() throws Throwable {
      ConfigurationImpl configuration = new ConfigurationImpl();

      Properties properties = new Properties();

      // overrides the default surrounding string of '"' with '$$' using a property
      properties.put(ActiveMQDefaultConfiguration.BROKER_PROPERTIES_KEY_SURROUND_PROPERTY, "$$");
      properties.put("addressesSettings.#.expiryAddress", "sharedExpiry");
      properties.put("addressesSettings.NeedToTrackExpired.expiryAddress", "important");
      properties.put("addressesSettings.$$Name.With.Dots$$.expiryAddress", "moreImportant");

      configuration.parsePrefixedProperties(properties, null);

      Assert.assertEquals(3, configuration.getAddressSettings().size());
      Assert.assertEquals(SimpleString.toSimpleString("sharedExpiry"), configuration.getAddressSettings().get("#").getExpiryAddress());
      Assert.assertEquals(SimpleString.toSimpleString("important"), configuration.getAddressSettings().get("NeedToTrackExpired").getExpiryAddress());
      Assert.assertEquals(SimpleString.toSimpleString("moreImportant"), configuration.getAddressSettings().get("Name.With.Dots").getExpiryAddress());
   }

   /**
    * To test ARTEMIS-926
    * @throws Throwable
    */
   @Test
   public void testSetSystemPropertyCME() throws Throwable {
      Properties properties = new Properties();

      for (int i = 0; i < 5000; i++) {
         properties.put("key" + i, "value " + i);
      }


      final ConfigurationImpl configuration = new ConfigurationImpl();

      final AtomicBoolean running = new AtomicBoolean(true);
      final CountDownLatch latch = new CountDownLatch(1);


      Thread thread = new Thread() {
         @Override
         public void run() {
            latch.countDown();
            int i = 1;
            while (running.get()) {
               properties.remove("key" + i);
               properties.put("key" + i, "new value " + i);
               i++;
               if (i > 200) {
                  i = 1;
               }
            }
         }
      };

      thread.start();
      try {

         latch.await();
         properties.put(configuration.getSystemPropertyPrefix() + "fileDeployerScanPeriod", "1234");
         properties.put(configuration.getSystemPropertyPrefix() + "globalMaxSize", "4321");

         configuration.parsePrefixedProperties(properties, configuration.getSystemPropertyPrefix());


      } finally {
         running.set(false);
         thread.join();
      }

      Assert.assertEquals(1234, configuration.getFileDeployerScanPeriod());
      Assert.assertEquals(4321, configuration.getGlobalMaxSize());
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      conf = createConfiguration();
   }

   protected Configuration createConfiguration() throws Exception {
      return new ConfigurationImpl();
   }

}
