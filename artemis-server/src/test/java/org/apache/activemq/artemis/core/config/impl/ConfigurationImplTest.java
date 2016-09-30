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

import java.io.File;

import org.apache.activemq.artemis.ArtemisConstants;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ha.LiveOnlyPolicyConfiguration;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConfigurationImplTest extends ActiveMQTestBase {

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
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMessageExpiryThreadPriority(), conf.getMessageExpiryThreadPriority()); // OK
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
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalPerfBlastPages(), conf.getJournalPerfBlastPages());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultMessageCounterEnabled(), conf.isMessageCounterEnabled());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMessageCounterMaxDayHistory(), conf.getMessageCounterMaxDayHistory());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMessageCounterSamplePeriod(), conf.getMessageCounterSamplePeriod());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultIdCacheSize(), conf.getIDCacheSize());
      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultPersistIdCache(), conf.isPersistIDCache());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultServerDumpInterval(), conf.getServerDumpInterval());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMemoryWarningThreshold(), conf.getMemoryWarningThreshold());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMemoryMeasureInterval(), conf.getMemoryMeasureInterval());
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

         i = RandomUtil.randomInt();
         conf.setMessageExpiryThreadPriority(i);
         Assert.assertEquals(i, conf.getMessageExpiryThreadPriority());

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

         i = RandomUtil.randomInt();
         conf.setJournalPerfBlastPages(i);
         Assert.assertEquals(i, conf.getJournalPerfBlastPages());

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

      i = RandomUtil.randomInt();
      conf.setMessageExpiryThreadPriority(i);
      Assert.assertEquals(i, conf.getMessageExpiryThreadPriority());

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

      i = RandomUtil.randomInt();
      conf.setJournalPerfBlastPages(i);
      Assert.assertEquals(i, conf.getJournalPerfBlastPages());

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

      File tempFolder = null;
      try {
         System.setProperty("artemis.instance", "/tmp/" + RandomUtil.randomString());
         tempFolder = File.createTempFile("journal-folder", "");
         tempFolder.delete();

         tempFolder = new File(tempFolder.getAbsolutePath());
         tempFolder.mkdirs();

         System.out.println("TempFolder = " + tempFolder.getAbsolutePath());

         ConfigurationImpl configuration = new ConfigurationImpl();
         configuration.setJournalDirectory(tempFolder.getAbsolutePath());
         File journalLocation = configuration.getJournalLocation();

         Assert.assertTrue(journalLocation.exists());
      } finally {
         if (oldProperty == null) {
            System.clearProperty("artemis.instance");
         } else {
            System.setProperty("artemis.instance", oldProperty);
         }

         if (tempFolder != null) {
            tempFolder.delete();
         }
      }

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
