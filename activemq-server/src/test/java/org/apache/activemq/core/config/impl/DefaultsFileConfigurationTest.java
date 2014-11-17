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
package org.apache.activemq.core.config.impl;

import org.apache.activemq.core.config.ha.LiveOnlyPolicyConfiguration;
import org.junit.Test;

import java.util.Collections;

import org.junit.Assert;

import org.apache.activemq.api.config.HornetQDefaultConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.journal.impl.JournalConstants;

/**
 *
 * A DefaultsFileConfigurationTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class DefaultsFileConfigurationTest extends ConfigurationImplTest
{
   @Override
   @Test
   public void testDefaults()
   {

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultScheduledThreadPoolMaxSize(),
                          conf.getScheduledThreadPoolMaxSize());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultThreadPoolMaxSize(), conf.getThreadPoolMaxSize());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultSecurityInvalidationInterval(),
                          conf.getSecurityInvalidationInterval());

      Assert.assertEquals(HornetQDefaultConfiguration.isDefaultSecurityEnabled(), conf.isSecurityEnabled());

      Assert.assertEquals(HornetQDefaultConfiguration.isDefaultJmxManagementEnabled(), conf.isJMXManagementEnabled());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultJmxDomain(), conf.getJMXDomain());

      Assert.assertEquals(0, conf.getIncomingInterceptorClassNames().size());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultConnectionTtlOverride(), conf.getConnectionTTLOverride());

      Assert.assertEquals(0, conf.getAcceptorConfigurations().size());

      Assert.assertEquals(Collections.emptyMap(), conf.getConnectorConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getBroadcastGroupConfigurations());

      Assert.assertEquals(Collections.emptyMap(), conf.getDiscoveryGroupConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getBridgeConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getDivertConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getClusterConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getQueueConfigurations());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultManagementAddress(), conf.getManagementAddress());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultManagementNotificationAddress(),
                          conf.getManagementNotificationAddress());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultClusterUser(), conf.getClusterUser());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultClusterPassword(), conf.getClusterPassword());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultIdCacheSize(), conf.getIDCacheSize());

      Assert.assertEquals(HornetQDefaultConfiguration.isDefaultPersistIdCache(), conf.isPersistIDCache());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultBindingsDirectory(), conf.getBindingsDirectory());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultJournalDir(), conf.getJournalDirectory());

      Assert.assertEquals(getDefaultJournalType(), conf.getJournalType());

      Assert.assertEquals(HornetQDefaultConfiguration.isDefaultJournalSyncTransactional(), conf.isJournalSyncTransactional());

      Assert.assertEquals(HornetQDefaultConfiguration.isDefaultJournalSyncNonTransactional(),
                          conf.isJournalSyncNonTransactional());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultJournalFileSize(), conf.getJournalFileSize());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultJournalCompactMinFiles(), conf.getJournalCompactMinFiles());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultJournalCompactPercentage(), conf.getJournalCompactPercentage());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultJournalMinFiles(), conf.getJournalMinFiles());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultJournalMaxIoAio(), conf.getJournalMaxIO_AIO());

      Assert.assertEquals(JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, conf.getJournalBufferTimeout_AIO());

      Assert.assertEquals(JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, conf.getJournalBufferSize_AIO());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultJournalMaxIoNio(), conf.getJournalMaxIO_NIO());

      Assert.assertEquals(JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO, conf.getJournalBufferTimeout_NIO());

      Assert.assertEquals(JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO, conf.getJournalBufferSize_NIO());

      Assert.assertEquals(HornetQDefaultConfiguration.isDefaultCreateBindingsDir(), conf.isCreateBindingsDir());

      Assert.assertEquals(HornetQDefaultConfiguration.isDefaultCreateJournalDir(), conf.isCreateJournalDir());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultPagingDir(), conf.getPagingDirectory());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultLargeMessagesDir(), conf.getLargeMessagesDirectory());

      Assert.assertEquals(HornetQDefaultConfiguration.isDefaultWildcardRoutingEnabled(), conf.isWildcardRoutingEnabled());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultTransactionTimeout(), conf.getTransactionTimeout());

      Assert.assertEquals(HornetQDefaultConfiguration.isDefaultMessageCounterEnabled(), conf.isMessageCounterEnabled());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultTransactionTimeoutScanPeriod(),
                          conf.getTransactionTimeoutScanPeriod());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultMessageExpiryScanPeriod(), conf.getMessageExpiryScanPeriod());

      Assert.assertEquals(HornetQDefaultConfiguration.getDefaultMessageExpiryThreadPriority(),
                          conf.getMessageExpiryThreadPriority());

      Assert.assertTrue(conf.getHAPolicyConfiguration() instanceof LiveOnlyPolicyConfiguration);
   }

   // Protected ---------------------------------------------------------------------------------------------

   @Override
   protected Configuration createConfiguration() throws Exception
   {
      FileConfiguration fc = new FileConfiguration("ConfigurationTest-defaults.xml");

      fc.start();

      return fc;
   }
}
