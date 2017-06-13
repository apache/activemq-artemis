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

import java.util.Collections;

import org.apache.activemq.artemis.ArtemisConstants;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.ha.LiveOnlyPolicyConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class DefaultsFileConfigurationTest extends ConfigurationImplTest {

   @Override
   @Test
   public void testDefaults() {

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultScheduledThreadPoolMaxSize(), conf.getScheduledThreadPoolMaxSize());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultThreadPoolMaxSize(), conf.getThreadPoolMaxSize());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultSecurityInvalidationInterval(), conf.getSecurityInvalidationInterval());

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

      Assert.assertEquals(Collections.emptyList(), conf.getAddressConfigurations());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultManagementAddress(), conf.getManagementAddress());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultManagementNotificationAddress(), conf.getManagementNotificationAddress());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultClusterUser(), conf.getClusterUser());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultClusterPassword(), conf.getClusterPassword());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultIdCacheSize(), conf.getIDCacheSize());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultPersistIdCache(), conf.isPersistIDCache());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultBindingsDirectory(), conf.getBindingsDirectory());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalDir(), conf.getJournalDirectory());

      Assert.assertEquals(getDefaultJournalType(), conf.getJournalType());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultJournalSyncTransactional(), conf.isJournalSyncTransactional());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultJournalSyncNonTransactional(), conf.isJournalSyncNonTransactional());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalFileSize(), conf.getJournalFileSize());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalCompactMinFiles(), conf.getJournalCompactMinFiles());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalCompactPercentage(), conf.getJournalCompactPercentage());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalMinFiles(), conf.getJournalMinFiles());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalMaxIoAio(), conf.getJournalMaxIO_AIO());

      Assert.assertEquals(ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, conf.getJournalBufferTimeout_AIO());

      Assert.assertEquals(ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, conf.getJournalBufferSize_AIO());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultJournalMaxIoNio(), conf.getJournalMaxIO_NIO());

      Assert.assertEquals(ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO, conf.getJournalBufferTimeout_NIO());

      Assert.assertEquals(ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO, conf.getJournalBufferSize_NIO());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultCreateBindingsDir(), conf.isCreateBindingsDir());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultCreateJournalDir(), conf.isCreateJournalDir());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultPagingDir(), conf.getPagingDirectory());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultLargeMessagesDir(), conf.getLargeMessagesDirectory());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultWildcardRoutingEnabled(), conf.isWildcardRoutingEnabled());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultTransactionTimeout(), conf.getTransactionTimeout());

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultMessageCounterEnabled(), conf.isMessageCounterEnabled());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultTransactionTimeoutScanPeriod(), conf.getTransactionTimeoutScanPeriod());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMessageExpiryScanPeriod(), conf.getMessageExpiryScanPeriod());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultMessageExpiryThreadPriority(), conf.getMessageExpiryThreadPriority());

      Assert.assertTrue(conf.getHAPolicyConfiguration() instanceof LiveOnlyPolicyConfiguration);

      Assert.assertEquals(ActiveMQDefaultConfiguration.isDefaultGracefulShutdownEnabled(), conf.isGracefulShutdownEnabled());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultGracefulShutdownTimeout(), conf.getGracefulShutdownTimeout());

      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultAmqpUseCoreSubscriptionNaming(), conf.isAmqpUseCoreSubscriptionNaming());
   }

   // Protected ---------------------------------------------------------------------------------------------

   @Override
   protected Configuration createConfiguration() throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager("ConfigurationTest-defaults.xml");
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();

      return fc;
   }
}
