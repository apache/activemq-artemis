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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FederationConfiguration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ScaleDownConfiguration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationQueuePolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.PrimaryOnlyPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStorePrimaryPolicyConfiguration;
import org.apache.activemq.artemis.core.deployers.impl.FileConfigurationParser;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ServerTestBase;
import org.apache.activemq.artemis.utils.ClassloadingUtil;
import org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec;
import org.apache.activemq.artemis.utils.PasswordMaskingUtil;
import org.apache.activemq.artemis.utils.StringPrintStream;
import org.junit.jupiter.api.Test;

public class FileConfigurationParserTest extends ServerTestBase {

   /**
    * These "InvalidConfigurationTest*.xml" files are modified copies of {@value
    * ConfigurationTest-full-config.xml}, so just diff it for changes, e.g.
    * <p>
    * <pre>
    * diff ConfigurationTest-full-config.xml InvalidConfigurationTest4.xml
    * </pre>
    *
    * @throws Exception
    */
   @Test
   public void testSchemaValidation() throws Exception {
      for (int i = 0; i < 7; i++) {
         String filename = "InvalidConfigurationTest" + i + ".xml";
         FileConfiguration fc = new FileConfiguration();
         FileDeploymentManager deploymentManager = new FileDeploymentManager(filename);
         deploymentManager.addDeployable(fc);

         try {
            deploymentManager.readConfiguration();
            fail("parsing should have failed for " + filename);
         } catch (java.lang.IllegalStateException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof org.xml.sax.SAXParseException, "must have been org.xml.sax.SAXParseException");
         }
      }
   }

   @Test
   public void testDivertRoutingNameIsNotRequired() throws Exception {
      String filename = "divertRoutingNameNotRequired.xml";
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(filename);
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();
   }

   @Test
   public void testDuplicateQueue() throws Exception {
      String filename = "FileConfigurationParser-duplicateQueue.xml";
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(filename);
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();
      ActiveMQServer server = addServer((ActiveMQServer) deploymentManager.buildService(null, null, null).get("core"));
      server.start();
      assertEquals(0, server.locateQueue(SimpleString.of("q")).getMaxConsumers());
   }

   @Test
   public void testAddressWithNoRoutingType() throws Exception {
      String filename = "FileConfigurationParser-addressWithNoRoutingType.xml";
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(filename);
      deploymentManager.addDeployable(fc);
      try {
         deploymentManager.readConfiguration();
         fail();
      } catch (IllegalArgumentException e) {
         // expected exception when address has no routing type configured
      }
   }

   @Test
   public void testDuplicateAddressSettings() throws Exception {
      FileConfigurationParser parser = new FileConfigurationParser();
      Configuration config = parser.parseMainConfig(ClassloadingUtil.findResource("FileConfigurationParser-duplicateAddressSettings.xml").openStream());

      assertEquals(123, config.getAddressSettings().get("foo").getRedistributionDelay());
   }

   @Test
   public void testParsingClusterConnectionURIs() throws Exception {
      FileConfigurationParser parser = new FileConfigurationParser();

      String configStr = firstPart + "<cluster-connections>\n" +
         "   <cluster-connection-uri name=\"my-cluster\" address=\"multicast://my-discovery-group?messageLoadBalancingType=STRICT;retryInterval=333;connectorName=netty-connector;maxHops=1\"/>\n" +
         "</cluster-connections>\n" + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration config = parser.parseMainConfig(input);

      assertEquals(1, config.getClusterConfigurations().size());

      assertEquals("my-discovery-group", config.getClusterConfigurations().get(0).getDiscoveryGroupName());
      assertEquals(333, config.getClusterConfigurations().get(0).getRetryInterval());
   }

   @Test
   public void testParsingZeroIDCacheSize() throws Exception {
      FileConfigurationParser parser = new FileConfigurationParser();

      String configStr = firstPart + "<id-cache-size>0</id-cache-size>" + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration config = parser.parseMainConfig(input);

      assertEquals(0, config.getIDCacheSize());
   }

   @Test
   public void testWildcardConfiguration() throws Exception {
      FileConfigurationParser parser = new FileConfigurationParser();

      String configStr = firstPart + "<wildcard-addresses>\n<routing-enabled>true</routing-enabled>\n<delimiter>/</delimiter>\n<any-words>></any-words></wildcard-addresses>" + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration config = parser.parseMainConfig(input);
      WildcardConfiguration wildCard = config.getWildcardConfiguration();
      assertEquals('/', wildCard.getDelimiter());
      assertTrue(wildCard.isRoutingEnabled());
      assertEquals('>', wildCard.getAnyWords());
      assertEquals('*', wildCard.getSingleWord());
   }

   @Test
   public void testParsingHaSharedStoreWaitForActivation() throws Exception {
      FileConfigurationParser parser = new FileConfigurationParser();

      String configStr = firstPart + "<ha-policy><shared-store><primary><wait-for-activation>false</wait-for-activation></primary></shared-store></ha-policy>" + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration config = parser.parseMainConfig(input);
      HAPolicyConfiguration haConfig = config.getHAPolicyConfiguration();

      assertTrue(haConfig instanceof SharedStorePrimaryPolicyConfiguration);

      SharedStorePrimaryPolicyConfiguration primaryConfig = (SharedStorePrimaryPolicyConfiguration) haConfig;

      assertFalse(primaryConfig.isWaitForActivation());
   }

   @Test
   public void testParsingDefaultServerConfig() throws Exception {
      FileConfigurationParser parser = new FileConfigurationParser();

      String configStr = firstPart + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration config = parser.parseMainConfig(input);

      String clusterPassword = config.getClusterPassword();

      assertEquals(ActiveMQDefaultConfiguration.getDefaultClusterPassword(), clusterPassword);

      //if we add cluster-password, it should be default plain text
      String clusterPasswordPart = "<cluster-password>helloworld</cluster-password>";

      configStr = firstPart + clusterPasswordPart + lastPart;

      config = parser.parseMainConfig(new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8)));

      assertEquals("helloworld", config.getClusterPassword());

      //if we add mask, it should be able to decode correctly
      DefaultSensitiveStringCodec codec = PasswordMaskingUtil.getDefaultCodec();
      String mask = codec.encode("helloworld");

      String maskPasswordPart = "<mask-password>true</mask-password>";
      clusterPasswordPart = "<cluster-password>" + mask + "</cluster-password>";

      configStr = firstPart + clusterPasswordPart + maskPasswordPart + lastPart;

      config = parser.parseMainConfig(new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8)));

      assertEquals("helloworld", config.getClusterPassword());

      //if we change key, it should be able to decode correctly
      codec = new DefaultSensitiveStringCodec();
      Map<String, String> prop = new HashMap<>();
      prop.put("key", "newkey");
      codec.init(prop);

      mask = codec.encode("newpassword");

      clusterPasswordPart = "<cluster-password>" + mask + "</cluster-password>";

      String codecPart = "<password-codec>" + "org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec" +
         ";key=newkey</password-codec>";

      configStr = firstPart + clusterPasswordPart + maskPasswordPart + codecPart + lastPart;

      config = parser.parseMainConfig(new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8)));

      assertEquals("newpassword", config.getClusterPassword());
   }

   @Test
   public void testParsingDefaultServerConfigWithENCMaskedPwd() throws Exception {
      FileConfigurationParser parser = new FileConfigurationParser();

      String configStr = firstPart + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration config = parser.parseMainConfig(input);

      String clusterPassword = config.getClusterPassword();

      assertEquals(ActiveMQDefaultConfiguration.getDefaultClusterPassword(), clusterPassword);

      //if we add cluster-password, it should be default plain text
      String clusterPasswordPart = "<cluster-password>ENC(5aec0780b12bf225a13ab70c6c76bc8e)</cluster-password>";

      configStr = firstPart + clusterPasswordPart + lastPart;

      config = parser.parseMainConfig(new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8)));

      assertEquals("helloworld", config.getClusterPassword());

      //if we add mask, it should be able to decode correctly
      DefaultSensitiveStringCodec codec = PasswordMaskingUtil.getDefaultCodec();
      String mask = codec.encode("helloworld");

      clusterPasswordPart = "<cluster-password>" + PasswordMaskingUtil.wrap(mask) + "</cluster-password>";

      configStr = firstPart + clusterPasswordPart + lastPart;

      config = parser.parseMainConfig(new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8)));

      assertEquals("helloworld", config.getClusterPassword());

      //if we change key, it should be able to decode correctly
      codec = new DefaultSensitiveStringCodec();
      Map<String, String> prop = new HashMap<>();
      prop.put("key", "newkey");
      codec.init(prop);

      mask = codec.encode("newpassword");

      clusterPasswordPart = "<cluster-password>" + PasswordMaskingUtil.wrap(mask) + "</cluster-password>";

      String codecPart = "<password-codec>" + "org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec" +
              ";key=newkey</password-codec>";

      configStr = firstPart + clusterPasswordPart + codecPart + lastPart;

      config = parser.parseMainConfig(new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8)));

      assertEquals("newpassword", config.getClusterPassword());

      configStr = firstPart + bridgePart + lastPart;
      config = parser.parseMainConfig(new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8)));

      List<BridgeConfiguration> bridgeConfigs = config.getBridgeConfigurations();
      assertEquals(1, bridgeConfigs.size());

      BridgeConfiguration bconfig = bridgeConfigs.get(0);

      assertEquals("helloworld", bconfig.getPassword());
   }

   @Test
   public void testDefaultBridgeProducerWindowSize() throws Exception {
      FileConfigurationParser parser = new FileConfigurationParser();

      String configStr = firstPart +
         "<bridges>\n" +
         "   <bridge name=\"my-bridge\">\n" +
         "      <queue-name>sausage-factory</queue-name>\n" +
         "      <forwarding-address>mincing-machine</forwarding-address>\n" +
         "      <static-connectors>\n" +
         "         <connector-ref>remote-connector</connector-ref>\n" +
         "      </static-connectors>\n" +
         "   </bridge>\n" +
         "   <bridge name=\"my-other-bridge\">\n" +
         "      <static-connectors>\n" +
         "         <connector-ref>remote-connector</connector-ref>\n" +
         "      </static-connectors>\n" +
         "      <forwarding-address>mincing-machine</forwarding-address>\n" +
         "      <queue-name>sausage-factory</queue-name>\n" +
         "   </bridge>\n" +
         "</bridges>\n"
         + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration config = parser.parseMainConfig(input);

      List<BridgeConfiguration> bridgeConfigs = config.getBridgeConfigurations();
      assertEquals(2, bridgeConfigs.size());

      BridgeConfiguration bconfig = bridgeConfigs.get(0);

      assertEquals(ActiveMQDefaultConfiguration.getDefaultBridgeProducerWindowSize(), bconfig.getProducerWindowSize());
   }

   @Test
   public void testParsingOverflowPageSize() throws Exception {
      testParsingOverFlow("<address-settings>" + "\n" + "<address-setting match=\"#\">" + "\n" + "<page-size-bytes>2147483648</page-size-bytes>\n" + "</address-setting>" + "\n" + "</address-settings>" + "\n");
      testParsingOverFlow("<journal-file-size>2147483648</journal-file-size>");
      testParsingOverFlow("<journal-buffer-size>2147483648</journal-buffer-size>");

      testParsingOverFlow("<cluster-connections> \n" + "  <cluster-connection name=\"my-cluster\"> \n" + "    <connector-ref>netty</connector-ref>  \n" + "    <min-large-message-size>2147483648</min-large-message-size>\n" + "    <discovery-group-ref discovery-group-name=\"my-discovery-group\"/> \n" + "  </cluster-connection> \n" + "</cluster-connections>");
      testParsingOverFlow("<cluster-connections> \n" + "  <cluster-connection name=\"my-cluster\"> \n" + "    <connector-ref>netty</connector-ref>  \n" + "    <confirmation-window-size>2147483648</confirmation-window-size>\n" + "    <discovery-group-ref discovery-group-name=\"my-discovery-group\"/> \n" + "  </cluster-connection> \n" + "</cluster-connections>");
      testParsingOverFlow("<cluster-connections> \n" + "  <cluster-connection name=\"my-cluster\"> \n" + "    <connector-ref>netty</connector-ref>  \n" + "    <producer-window-size>2147483648</producer-window-size>\n" + "    <discovery-group-ref discovery-group-name=\"my-discovery-group\"/> \n" + "  </cluster-connection> \n" + "</cluster-connections>");

      testParsingOverFlow("<bridges> \n" + "  <bridge name=\"price-forward-bridge\"> \n" + "    <queue-name>priceForwarding</queue-name>  \n" + "    <forwarding-address>newYorkPriceUpdates</forwarding-address>\n" + "    <min-large-message-size>2147483648</min-large-message-size>\n" + "    <static-connectors> \n" + "      <connector-ref>netty</connector-ref> \n" + "    </static-connectors> \n" + "  </bridge> \n" + "</bridges>");
      testParsingOverFlow("<bridges> \n" + "  <bridge name=\"price-forward-bridge\"> \n" + "    <queue-name>priceForwarding</queue-name>  \n" + "    <forwarding-address>newYorkPriceUpdates</forwarding-address>\n" + "    <confirmation-window-size>2147483648</confirmation-window-size>\n" + "    <static-connectors> \n" + "      <connector-ref>netty</connector-ref> \n" + "    </static-connectors> \n" + "  </bridge> \n" + "</bridges>\n");
      testParsingOverFlow("<bridges> \n" + "  <bridge name=\"price-forward-bridge\"> \n" + "    <queue-name>priceForwarding</queue-name>  \n" + "    <forwarding-address>newYorkPriceUpdates</forwarding-address>\n" + "    <producer-window-size>2147483648</producer-window-size>\n" + "    <static-connectors> \n" + "      <connector-ref>netty</connector-ref> \n" + "    </static-connectors> \n" + "  </bridge> \n" + "</bridges>\n");
   }

   @Test
   public void testParsingScaleDownConfig() throws Exception {
      FileConfigurationParser parser = new FileConfigurationParser();

      String configStr = firstPart + "<ha-policy>\n" +
               "   <live-only>\n" +
               "      <scale-down>\n" +
               "         <connectors>\n" +
               "            <connector-ref>server0-connector</connector-ref>\n" +
               "         </connectors>\n" +
               "      </scale-down>\n" +
               "   </live-only>\n" +
               "</ha-policy>\n" + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration config = parser.parseMainConfig(input);

      HAPolicyConfiguration haConfig = config.getHAPolicyConfiguration();
      assertTrue(haConfig instanceof PrimaryOnlyPolicyConfiguration);

      PrimaryOnlyPolicyConfiguration primaryOnlyCfg = (PrimaryOnlyPolicyConfiguration) haConfig;
      ScaleDownConfiguration scaledownCfg = primaryOnlyCfg.getScaleDownConfiguration();
      List<String> connectors = scaledownCfg.getConnectors();
      assertEquals(1, connectors.size());
      String connector = connectors.get(0);
      assertEquals("server0-connector", connector);
   }


   private void testParsingOverFlow(String config) throws Exception {
      FileConfigurationParser parser = new FileConfigurationParser();
      String firstPartWithoutAddressSettings = firstPart.substring(0, firstPart.indexOf("<address-settings"));

      String configStr = firstPartWithoutAddressSettings + config + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      try {
         Configuration configuration = parser.parseMainConfig(input);
         fail("parsing should have failed bcs of overflow page size");
      } catch (java.lang.IllegalArgumentException e) {
      }
   }

   @Test
   public void testParseMaxSizeOnAddressSettings() throws Exception {
      String configStr = "<configuration><address-settings>" + "\n" + "<address-setting match=\"foo\">" + "\n" + "<max-size-messages>123</max-size-messages>\n" + "</address-setting>" + "\n" + "</address-settings></configuration>" + "\n";

      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration configuration = parser.parseMainConfig(input);
      AddressSettings settings = configuration.getAddressSettings().get("foo");
      assertEquals(123, settings.getMaxSizeMessages());
   }


   @Test
   public void testParseMaxReadAddressSettings() throws Exception {
      String configStr = "<configuration><address-settings>" + "\n" + "<address-setting match=\"foo\">" + "\n" + "<max-read-page-bytes>1k</max-read-page-bytes><max-read-page-messages>33</max-read-page-messages>.\n" + "</address-setting>" + "\n" + "</address-settings></configuration>" + "\n";

      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration configuration = parser.parseMainConfig(input);
      AddressSettings settings = configuration.getAddressSettings().get("foo");
      assertEquals(1024, settings.getMaxReadPageBytes());
      assertEquals(33, settings.getMaxReadPageMessages());
      assertNull(settings.getPageLimitBytes());
      assertNull(settings.getPageLimitMessages());
      assertNull(settings.getPageFullMessagePolicy());
   }

   @Test
   public void testParsePageLimitSettings() throws Exception {
      String configStr = "<configuration><address-settings>" + "\n" + "<address-setting match=\"foo\">" + "\n" + "<max-read-page-bytes>1k</max-read-page-bytes><prefetch-page-bytes>100M</prefetch-page-bytes><prefetch-page-messages>777</prefetch-page-messages><page-limit-bytes>10G</page-limit-bytes><page-limit-messages>3221225472</page-limit-messages><page-full-policy>FAIL</page-full-policy><max-read-page-messages>33</max-read-page-messages>.\n" + "</address-setting>" + "\n" + "</address-settings></configuration>" + "\n";

      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration configuration = parser.parseMainConfig(input);
      AddressSettings settings = configuration.getAddressSettings().get("foo");
      assertEquals(1024, settings.getMaxReadPageBytes());
      assertEquals(33, settings.getMaxReadPageMessages());
      assertEquals(10L * 1024 * 1024 * 1024, settings.getPageLimitBytes().longValue());
      assertEquals(100 * 1024 * 1024, settings.getPrefetchPageBytes());
      assertEquals(777, settings.getPrefetchPageMessages());
      assertEquals(3L * 1024 * 1024 * 1024, settings.getPageLimitMessages().longValue());
      assertEquals("FAIL", settings.getPageFullMessagePolicy().toString());
   }

   @Test
   public void testParseMaxReadAddressSettingsAllNegative() throws Exception {
      String configStr = "<configuration><address-settings>" + "\n" + "<address-setting match=\"foo\">" + "\n" + "<max-read-page-bytes>-1</max-read-page-bytes><max-read-page-messages>-1</max-read-page-messages>.\n" + "</address-setting>" + "\n" + "</address-settings></configuration>" + "\n";

      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration configuration = parser.parseMainConfig(input);
      AddressSettings settings = configuration.getAddressSettings().get("foo");
      assertEquals(-1, settings.getMaxReadPageBytes());
      assertEquals(-1, settings.getMaxReadPageMessages());
   }

   @Test
   public void testLiteralMatchMarkers() throws Exception {
      String configStr = "<configuration><literal-match-markers>()</literal-match-markers><address-settings>\n<address-setting match=\"(foo)\">\n<max-read-page-bytes>-1</max-read-page-bytes></address-setting>\n</address-settings></configuration>\n";

      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration configuration = parser.parseMainConfig(input);
      assertEquals("()", configuration.getLiteralMatchMarkers());
   }

   @Test
   public void testViewPermissionMethodMatchPattern() throws Exception {
      final String pattern = "^(get|list).+$";
      String configStr = "<configuration><view-permission-method-match-pattern>" + pattern + "</view-permission-method-match-pattern>\n</configuration>\n";

      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration configuration = parser.parseMainConfig(input);
      assertEquals(pattern, configuration.getViewPermissionMethodMatchPattern());
   }

   @Test
   public void testManagementRbacPrefix() throws Exception {
      final String pattern = "j.m.x";
      String configStr = "<configuration><management-rbac-prefix>" + pattern + "</management-rbac-prefix>\n</configuration>\n";

      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration configuration = parser.parseMainConfig(input);
      assertEquals(pattern, configuration.getManagementRbacPrefix());
   }

   @Test
   public void testManagementRbac() throws Exception {
      final boolean enabled = true;
      String configStr = "<configuration><management-message-rbac>" + enabled + "</management-message-rbac>\n</configuration>\n";

      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration configuration = parser.parseMainConfig(input);
      assertEquals(enabled, configuration.isManagementMessageRbac());
   }

   // you should not use K, M notations on address settings max-size-messages
   @Test
   public void testExpectedErrorOverMaxMessageNotation() throws Exception {
      String configStr = "<configuration><address-settings>" + "\n" + "<address-setting match=\"foo\">" + "\n" + "<max-size-messages>123K</max-size-messages>\n" + "</address-setting>" + "\n" + "</address-settings></configuration>" + "\n";

      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      boolean valid = false;
      try {
         parser.parseMainConfig(input);
      } catch (Throwable expected) {
         valid = true;
      }
      assertTrue(valid, "Exception expected");
   }

   private static String bridgePart = "<bridges>\n" +
           "            <bridge name=\"my-bridge\">\n" +
           "               <queue-name>sausage-factory</queue-name>\n" +
           "               <forwarding-address>mincing-machine</forwarding-address>\n" +
           "               <filter string=\"name='aardvark'\"/>\n" +
           "               <transformer-class-name>org.apache.activemq.artemis.jms.example.HatColourChangeTransformer</transformer-class-name>\n" +
           "               <reconnect-attempts>-1</reconnect-attempts>\n" +
           "               <user>bridge-user</user>" +
           "               <password>ENC(5aec0780b12bf225a13ab70c6c76bc8e)</password>" +
           "               <static-connectors>\n" +
           "                  <connector-ref>remote-connector</connector-ref>\n" +
           "               </static-connectors>\n" +
           "            </bridge>\n" +
           "</bridges>\n";

   @Test
   public void testParsingAddressSettings() throws Exception {
      long expected = 2147483648L;
      String firstPartWithoutAS = firstPart.substring(0, firstPart.indexOf("<address-settings"));
      String configStr = firstPartWithoutAS + ("<address-settings>\n"
                                               + "<address-setting match=\"#\">\n"
                                               + String.format("<max-size-bytes-reject-threshold>%d</max-size-bytes-reject-threshold>\n",
                                                               expected)
                                               + "</address-setting>\n"
                                               + "</address-settings>"
                                               + "\n") + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));
      Configuration configuration = new FileConfigurationParser().parseMainConfig(input);
      assertEquals(1, configuration.getAddressSettings().size());
      AddressSettings addressSettings = configuration.getAddressSettings().get("#");
      assertEquals(expected, addressSettings.getMaxSizeBytesRejectThreshold());
   }

   @Test
   public void testParsingPageSyncTimeout() throws Exception {
      int expected = 1000;
      FileConfigurationParser parser = new FileConfigurationParser();

      String configStr = firstPart + String.format("<page-sync-timeout>%d</page-sync-timeout>\n", expected) + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration config = parser.parseMainConfig(input);
      assertEquals(expected, config.getPageSyncTimeout());
   }


   @Test
   public void testMinimalXML() throws Exception {
      StringPrintStream stringPrintStream = new StringPrintStream();
      PrintStream stream = stringPrintStream.newStream();

      stream.println("<configuration><core>");
      stream.println("</core></configuration>");

      ByteArrayInputStream inputStream = new ByteArrayInputStream(stringPrintStream.getBytes());
      FileConfigurationParser parser = new FileConfigurationParser();
      Configuration configuration = parser.parseMainConfig(inputStream);
   }

   @Test
   public void testMaxSize() throws Exception {
      StringPrintStream stringPrintStream = new StringPrintStream();
      PrintStream stream = stringPrintStream.newStream();

      stream.println("<configuration><core>");
      stream.println("<global-max-size>10M</global-max-size>");
      stream.println("<global-max-messages>1000</global-max-messages>");
      stream.println("</core></configuration>");

      ByteArrayInputStream inputStream = new ByteArrayInputStream(stringPrintStream.getBytes());
      FileConfigurationParser parser = new FileConfigurationParser();
      Configuration configuration = parser.parseMainConfig(inputStream);

      assertEquals(10 * 1024 * 1024, configuration.getGlobalMaxSize());
      assertEquals(1000, configuration.getGlobalMaxMessages());
   }

   @Test
   public void testConfigurationPersistRedelivery() throws Exception {
      StringPrintStream stringPrintStream = new StringPrintStream();
      PrintStream stream = stringPrintStream.newStream();

      stream.println("<configuration><core>");
      stream.println("<max-redelivery-records>0</max-redelivery-records>");
      stream.println("</core></configuration>");

      ByteArrayInputStream inputStream = new ByteArrayInputStream(stringPrintStream.getBytes());
      FileConfigurationParser parser = new FileConfigurationParser();
      Configuration configuration = parser.parseMainConfig(inputStream);

      assertEquals(0, configuration.getMaxRedeliveryRecords());
   }

   @Test
   public void testExceptionMaxSize() throws Exception {
      StringPrintStream stringPrintStream = new StringPrintStream();
      PrintStream stream = stringPrintStream.newStream();

      stream.println("<configuration><core>");
      stream.println("<global-max-messages>1000K</global-max-messages>");
      stream.println("</core></configuration>");

      ByteArrayInputStream inputStream = new ByteArrayInputStream(stringPrintStream.getBytes());
      FileConfigurationParser parser = new FileConfigurationParser();
      boolean exceptionHappened = false;

      try {
         parser.parseMainConfig(inputStream);
      } catch (Throwable e) {
         exceptionHappened = true;
      }

      assertTrue(exceptionHappened, "Exception expected parsing notation for global-max-messages");

   }

   @Test
   public void testNotations() throws Exception {
      StringPrintStream stringPrintStream = new StringPrintStream();
      PrintStream stream = stringPrintStream.newStream();

      stream.println("<configuration><core>");
      stream.println("<global-max-size>100MiB</global-max-size>");
      stream.println("<journal-file-size>10M</journal-file-size>");
      stream.println("<journal-buffer-size>5Mb</journal-buffer-size>");
      stream.println("</core></configuration>");

      ByteArrayInputStream inputStream = new ByteArrayInputStream(stringPrintStream.getBytes());
      FileConfigurationParser parser = new FileConfigurationParser();
      Configuration configuration = parser.parseMainConfig(inputStream);

      // check that suffixes were interpreted well
      assertEquals(100 * 1024 * 1024, configuration.getGlobalMaxSize());
      assertEquals(10 * 1024 * 1024, configuration.getJournalFileSize());
      // one of the two will get the value
      assertTrue(5 * 1024 * 1024 == configuration.getJournalBufferSize_AIO() ||
                                 5 * 1024 * 1024 == configuration.getJournalBufferSize_NIO());
   }

   @Test
   public void testRetentionJournalOptionsDays() throws Exception {
      testStreamDatesOption("DAYS", TimeUnit.DAYS);
   }

   @Test
   public void testRetentionJournalOptionsHours() throws Exception {
      testStreamDatesOption("HOURS", TimeUnit.HOURS);
   }

   @Test
   public void testRetentionJournalOptionsMinutes() throws Exception {
      testStreamDatesOption("MINUTES", TimeUnit.MINUTES);
   }

   @Test
   public void testRetentionJournalOptionsSeconds() throws Exception {
      testStreamDatesOption("SECONDS", TimeUnit.SECONDS);
   }

   private void testStreamDatesOption(String option, TimeUnit expected) throws Exception {
      StringPrintStream stringPrintStream = new StringPrintStream();
      PrintStream stream = stringPrintStream.newStream();

      stream.println("<configuration><core>");
      stream.println("<journal-retention-directory unit=\"" + option + "\" period=\"365\" storage-limit=\"10G\">history</journal-retention-directory>");
      stream.println("</core></configuration>");

      ByteArrayInputStream inputStream = new ByteArrayInputStream(stringPrintStream.getBytes());
      FileConfigurationParser parser = new FileConfigurationParser();
      Configuration configuration = parser.parseMainConfig(inputStream);

      assertEquals("history", configuration.getJournalRetentionDirectory());

      assertEquals(expected.toMillis(365), configuration.getJournalRetentionPeriod());
   }


   @Test
   public void unlimitedJustHistory() throws Throwable {
      StringPrintStream stringPrintStream = new StringPrintStream();
      PrintStream stream = stringPrintStream.newStream();

      stream.println("<configuration><core>");
      stream.println("<journal-retention-directory>directory</journal-retention-directory>");
      stream.println("</core></configuration>");

      ByteArrayInputStream inputStream = new ByteArrayInputStream(stringPrintStream.getBytes());
      FileConfigurationParser parser = new FileConfigurationParser();
      Configuration configuration = null;
      boolean exceptionHappened = false;
      try {
         configuration = parser.parseMainConfig(inputStream);
      } catch (Exception e) {
         exceptionHappened = true;
      }

      assertTrue(exceptionHappened);
   }



   @Test
   public void noRetention() throws Throwable {
      StringPrintStream stringPrintStream = new StringPrintStream();
      PrintStream stream = stringPrintStream.newStream();

      stream.println("<configuration><core>");
      stream.println("<journal-directory>journal</journal-directory>");
      stream.println("</core></configuration>");

      ByteArrayInputStream inputStream = new ByteArrayInputStream(stringPrintStream.getBytes());
      FileConfigurationParser parser = new FileConfigurationParser();
      Configuration configuration = null;
      configuration = parser.parseMainConfig(inputStream);
      assertNull(configuration.getJournalRetentionLocation());
      assertNull(configuration.getJournalRetentionDirectory());
      assertEquals("journal", configuration.getJournalDirectory());
   }


   @Test
   public void noFolderOnRetention() throws Throwable {
      StringPrintStream stringPrintStream = new StringPrintStream();
      PrintStream stream = stringPrintStream.newStream();

      stream.println("<configuration><core>");
      stream.println("<journal-retention-directory period=\"3\"></journal-retention-directory>");
      stream.println("</core></configuration>");
      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream inputStream = new ByteArrayInputStream(stringPrintStream.getBytes());
      boolean exception = false;
      try {
         Configuration configuration = parser.parseMainConfig(inputStream);
      } catch (Exception e) {
         exception = true;
      }

      assertTrue(exception);
   }

   @Test
   public void testSyncLargeMessage() throws Throwable {
      StringPrintStream stringPrintStream = new StringPrintStream();
      PrintStream stream = stringPrintStream.newStream();

      stream.println("<configuration><core>");
      stream.println("<large-message-sync>false</large-message-sync>");
      stream.println("</core></configuration>");
      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream inputStream = new ByteArrayInputStream(stringPrintStream.getBytes());
      Configuration configuration = parser.parseMainConfig(inputStream);
      assertFalse(configuration.isLargeMessageSync());
   }

   private static String firstPart = "<core xmlns=\"urn:activemq:core\">" + "\n" +
      "<name>ActiveMQ.main.config</name>" + "\n" +
      "<log-delegate-factory-class-name>org.apache.activemq.artemis.integration.logging.Log4jLogDelegateFactory</log-delegate-factory-class-name>" + "\n" +
      "<bindings-directory>${jboss.server.data.dir}/activemq/bindings</bindings-directory>" + "\n" +
      "<journal-directory>${jboss.server.data.dir}/activemq/journal</journal-directory>" + "\n" +
      "<journal-min-files>10</journal-min-files>" + "\n" +
      "<large-messages-directory>${jboss.server.data.dir}/activemq/largemessages</large-messages-directory>" + "\n" +
      "<paging-directory>${jboss.server.data.dir}/activemq/paging</paging-directory>" + "\n" +
      "<connectors>" + "\n" +
      "<connector name=\"netty\">tcp://localhost:61616</connector>" + "\n" +
      "<connector name=\"netty-throughput\">tcp://localhost:5545</connector>" + "\n" +
      "<connector name=\"in-vm\">vm://0</connector>" + "\n" +
      "</connectors>" + "\n" +
      "<acceptors>" + "\n" +
      "<acceptor name=\"netty\">tcp://localhost:5545</acceptor>" + "\n" +
      "<acceptor name=\"netty-throughput\">tcp://localhost:5545</acceptor>" + "\n" +
      "<acceptor name=\"in-vm\">vm://0</acceptor>" + "\n" +
      "</acceptors>" + "\n" +
      "<security-settings>" + "\n" +
      "<security-setting match=\"#\">" + "\n" +
      "<permission type=\"createNonDurableQueue\" roles=\"guest\"/>" + "\n" +
      "<permission type=\"deleteNonDurableQueue\" roles=\"guest\"/>" + "\n" +
      "<permission type=\"createDurableQueue\" roles=\"guest\"/>" + "\n" +
      "<permission type=\"deleteDurableQueue\" roles=\"guest\"/>" + "\n" +
      "<permission type=\"consume\" roles=\"guest\"/>" + "\n" +
      "<permission type=\"send\" roles=\"guest\"/>" + "\n" +
      "</security-setting>" + "\n" +
      "</security-settings>" + "\n" +
      "<address-settings>" + "\n" +
      "<address-setting match=\"#\">" + "\n" + "<dead-letter-address>DLQ\n</dead-letter-address>" + "\n" + "<expiry-address>ExpiryQueue\n</expiry-address>" + "\n" + "<redelivery-delay>0\n</redelivery-delay>" + "\n" + "<max-size-bytes>10485760\n</max-size-bytes>" + "\n" + "<message-counter-history-day-limit>10</message-counter-history-day-limit>" + "\n" + "<address-full-policy>BLOCK</address-full-policy>" + "\n" +
      "</address-setting>" + "\n" +
      "</address-settings>" + "\n";

   private static String lastPart = "</core>";

   @Test
   public void testParseQueueMatchInFederationConfiguration() throws Exception {
      String configStr = firstPart +
                         "<federations>" +
                          "<federation name=\"server-1-federation\">" +
                           "<upstream name=\"upstream\">" +
                            "<static-connectors>" +
                             "<connector-ref>server-connector</connector-ref>" +
                            "</static-connectors>" +
                            "<policy ref=\"queue-federation\"/>" +
                           "</upstream>" +
                           "" +
                           "<queue-policy name=\"queue-federation\">" +
                            "<include queue-match=\"myQueue\" address-match=\"#\"/>" +
                           "</queue-policy>" +
                          "</federation>" +
                         "</federations>" +
                         lastPart;

      final FileConfigurationParser parser = new FileConfigurationParser();
      final ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      final Configuration configuration = parser.parseMainConfig(input);
      final List<FederationConfiguration> federations = configuration.getFederationConfigurations();

      assertEquals(1, federations.size());

      final FederationConfiguration federation = federations.get(0);
      final FederationQueuePolicyConfiguration policy =
         (FederationQueuePolicyConfiguration) federation.getQueuePolicies().get("queue-federation");

      assertNotNull(policy);

      final Set<FederationQueuePolicyConfiguration.Matcher> matches = policy.getIncludes();

      assertEquals(1, matches.size());

      final FederationQueuePolicyConfiguration.Matcher match = matches.iterator().next();

      assertEquals("#", match.getAddressMatch());
      assertEquals("myQueue", match.getQueueMatch());
   }
}
