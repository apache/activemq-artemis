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
package org.apache.activemq6.tests.unit.core.deployers.impl;
import org.junit.Before;

import org.junit.Test;

import org.junit.Assert;

import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.core.deployers.DeploymentManager;
import org.apache.activemq6.core.deployers.impl.AddressSettingsDeployer;
import org.apache.activemq6.core.settings.HierarchicalRepository;
import org.apache.activemq6.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq6.core.settings.impl.AddressSettings;
import org.apache.activemq6.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq6.tests.util.UnitTestCase;
import org.apache.activemq6.utils.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class AddressSettingsDeployerTest extends UnitTestCase
{
   private final String conf = "<address-setting match=\"queues.*\">\n" + "      <dead-letter-address>DLQtest</dead-letter-address>\n"
                               + "      <expiry-address>ExpiryQueueTest</expiry-address>\n"
                               + "      <redelivery-delay>100</redelivery-delay>\n"
                               + "      <max-delivery-attempts>32</max-delivery-attempts>\n"
                               + "      <max-size-bytes>18238172365765</max-size-bytes>\n"
                               + "      <page-size-bytes>2387273767666</page-size-bytes>\n"
                               + "      <address-full-policy>DROP</address-full-policy>\n"
                               + "      <message-counter-history-day-limit>1000</message-counter-history-day-limit>\n"
                               + "      <last-value-queue>true</last-value-queue>\n"
                               + "      <redistribution-delay>38383</redistribution-delay>\n"
                               + "      <redelivery-delay-multiplier>2</redelivery-delay-multiplier>\n"
                               + "      <max-redelivery-delay>12000</max-redelivery-delay>\n"
                               + "      <send-to-dla-on-no-route>true</send-to-dla-on-no-route>\n"
                               + "   </address-setting>";

   private AddressSettingsDeployer addressSettingsDeployer;

   private HierarchicalRepository<AddressSettings> repository;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      repository = new HierarchicalObjectRepository<AddressSettings>();
      DeploymentManager deploymentManager = new FakeDeploymentManager();
      addressSettingsDeployer = new AddressSettingsDeployer(deploymentManager, repository);
   }

   @Test
   public void testDeploy() throws Exception
   {
      addressSettingsDeployer.deploy(XMLUtil.stringToElement(conf));
      AddressSettings as = repository.getMatch("queues.aq");
      Assert.assertNotNull(as);
      Assert.assertEquals(new SimpleString("DLQtest"), as.getDeadLetterAddress());
      Assert.assertEquals(new SimpleString("ExpiryQueueTest"), as.getExpiryAddress());
      Assert.assertEquals(100, as.getRedeliveryDelay());
      Assert.assertEquals(32, as.getMaxDeliveryAttempts());
      Assert.assertEquals(18238172365765L, as.getMaxSizeBytes());
      Assert.assertEquals(2387273767666L, as.getPageSizeBytes());
      Assert.assertEquals(AddressFullMessagePolicy.DROP, as.getAddressFullMessagePolicy());
      Assert.assertEquals(1000, as.getMessageCounterHistoryDayLimit());
      Assert.assertTrue(as.isLastValueQueue());
      Assert.assertEquals(38383, as.getRedistributionDelay());
      Assert.assertEquals(2.0, as.getRedeliveryMultiplier(), 0.000001);
      Assert.assertEquals(12000, as.getMaxRedeliveryDelay());
      Assert.assertTrue(as.isSendToDLAOnNoRoute());

   }

   @Test
   public void testDeployFromConfigurationFile() throws Exception
   {
      String xml = "<configuration xmlns='urn:hornetq'> " + "<address-settings>" +
                   conf +
                   "</address-settings>" +
                   "</configuration>";

      Element rootNode = org.apache.activemq6.utils.XMLUtil.stringToElement(xml);
      addressSettingsDeployer.validate(rootNode);
      NodeList addressSettingsNode = rootNode.getElementsByTagName("address-setting");
      Assert.assertEquals(1, addressSettingsNode.getLength());

      addressSettingsDeployer.deploy(addressSettingsNode.item(0));
      AddressSettings as = repository.getMatch("queues.aq");
      Assert.assertNotNull(as);
      Assert.assertEquals(new SimpleString("DLQtest"), as.getDeadLetterAddress());
      Assert.assertEquals(new SimpleString("ExpiryQueueTest"), as.getExpiryAddress());
      Assert.assertEquals(100, as.getRedeliveryDelay());
      Assert.assertEquals(2.0, as.getRedeliveryMultiplier(), 0.000001);
      Assert.assertEquals(12000, as.getMaxRedeliveryDelay());
      Assert.assertEquals(32, as.getMaxDeliveryAttempts());
      Assert.assertEquals(18238172365765L, as.getMaxSizeBytes());
      Assert.assertEquals(2387273767666L, as.getPageSizeBytes());
      Assert.assertEquals(AddressFullMessagePolicy.DROP, as.getAddressFullMessagePolicy());
      Assert.assertEquals(1000, as.getMessageCounterHistoryDayLimit());
      Assert.assertTrue(as.isLastValueQueue());
      Assert.assertEquals(38383, as.getRedistributionDelay());
      Assert.assertTrue(as.isSendToDLAOnNoRoute());
   }

   @Test
   public void testUndeploy() throws Exception
   {
      addressSettingsDeployer.deploy(XMLUtil.stringToElement(conf));
      AddressSettings as = repository.getMatch("queues.aq");
      Assert.assertNotNull(as);
      addressSettingsDeployer.undeploy(XMLUtil.stringToElement(conf));
      as = repository.getMatch("queues.aq");
      Assert.assertNull(as);
   }

}

