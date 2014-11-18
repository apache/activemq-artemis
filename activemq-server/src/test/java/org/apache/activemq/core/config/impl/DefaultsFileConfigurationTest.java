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

import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.core.config.ha.LiveOnlyPolicyConfiguration;
import org.junit.Test;

import java.util.Collections;

import org.junit.Assert;

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

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultScheduledThreadPoolMaxSize(),
                          conf.getScheduledThreadPoolMaxSize());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultThreadPoolMaxSize(), conf.getThreadPoolMaxSize());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultSecurityInvalidationInterval(),
                          conf.getSecurityInvalidationInterval());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultSecurityEnabled(), conf.isSecurityEnabled());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultJmxManagementEnabled(), conf.isJMXManagementEnabled());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), conf.getJMXDomain());

      Assert.assertEquals(0, conf.getIncomingInterceptorClassNames().size());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultConnectionTtlOverride(), conf.getConnectionTTLOverride());

      Assert.assertEquals(0, conf.getAcceptorConfigurations().size());

      Assert.assertEquals(Collections.emptyMap(), conf.getConnectorConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getBroadcastGroupConfigurations());

      Assert.assertEquals(Collections.emptyMap(), conf.getDiscoveryGroupConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getBridgeConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getDivertConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getClusterConfigurations());

      Assert.assertEquals(Collections.emptyList(), conf.getQueueConfigurations());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultManagementAddress(), conf.getManagementAddress());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultManagementNotificationAddress(),
                          conf.getManagementNotificationAddress());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultClusterUser(), conf.getClusterUser());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultClusterPassword(), conf.getClusterPassword());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultIdCacheSize(), conf.getIDCacheSize());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultPersistIdCache(), conf.isPersistIDCache());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultBindingsDirectory(), conf.getBindingsDirectory());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalDir(), conf.getJournalDirectory());

      Assert.assertEquals(getDefaultJournalType(), conf.getJournalType());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultJournalSyncTransactional(), conf.isJournalSyncTransactional());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultJournalSyncNonTransactional(),
                          conf.isJournalSyncNonTransactional());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalFileSize(), conf.getJournalFileSize());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalCompactMinFiles(), conf.getJournalCompactMinFiles());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalCompactPercentage(), conf.getJournalCompactPercentage());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalMinFiles(), conf.getJournalMinFiles());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalMaxIoAio(), conf.getJournalMaxIO_AIO());

      Assert.assertEquals(JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, conf.getJournalBufferTimeout_AIO());

      Assert.assertEquals(JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, conf.getJournalBufferSize_AIO());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalMaxIoNio(), conf.getJournalMaxIO_NIO());

      Assert.assertEquals(JournalConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO, conf.getJournalBufferTimeout_NIO());

      Assert.assertEquals(JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO, conf.getJournalBufferSize_NIO());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultCreateBindingsDir(), conf.isCreateBindingsDir());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultCreateJournalDir(), conf.isCreateJournalDir());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultPagingDir(), conf.getPagingDirectory());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultLargeMessagesDir(), conf.getLargeMessagesDirectory());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultWildcardRoutingEnabled(), conf.isWildcardRoutingEnabled());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultTransactionTimeout(), conf.getTransactionTimeout());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultMessageCounterEnabled(), conf.isMessageCounterEnabled());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultTransactionTimeoutScanPeriod(),
                          conf.getTransactionTimeoutScanPeriod());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMessageExpiryScanPeriod(), conf.getMessageExpiryScanPeriod());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMessageExpiryThreadPriority(),
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
