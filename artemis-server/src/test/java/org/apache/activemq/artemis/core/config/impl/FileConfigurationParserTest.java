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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.deployers.impl.FileConfigurationParser;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec;
import org.apache.activemq.artemis.utils.PasswordMaskingUtil;
import org.junit.Assert;
import org.junit.Test;

public class FileConfigurationParserTest extends ActiveMQTestBase {

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
            assertTrue("must have been org.xml.sax.SAXParseException", cause instanceof org.xml.sax.SAXParseException);
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
      String filename = "duplicateQueue.xml";
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(filename);
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();
      ActiveMQServer server = addServer((ActiveMQServer) deploymentManager.buildService(null, null).get("core"));
      server.start();
      assertEquals(0, server.locateQueue(SimpleString.toSimpleString("q")).getMaxConsumers());
   }

   @Test
   public void testParsingClusterConnectionURIs() throws Exception {
      FileConfigurationParser parser = new FileConfigurationParser();

      String configStr = firstPart + "<cluster-connections>\n" +
         "   <cluster-connection-uri name=\"my-cluster\" address=\"multicast://my-discovery-group?messageLoadBalancingType=STRICT;retryInterval=333;connectorName=netty-connector;maxHops=1\"/>\n" +
         "</cluster-connections>\n" + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration config = parser.parseMainConfig(input);

      Assert.assertEquals(1, config.getClusterConfigurations().size());

      Assert.assertEquals("my-discovery-group", config.getClusterConfigurations().get(0).getDiscoveryGroupName());
      Assert.assertEquals(333, config.getClusterConfigurations().get(0).getRetryInterval());
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

      String configStr = firstPart + "<ha-policy><shared-store><master><wait-for-activation>false</wait-for-activation></master></shared-store></ha-policy>" + lastPart;
      ByteArrayInputStream input = new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8));

      Configuration config = parser.parseMainConfig(input);
      HAPolicyConfiguration haConfig = config.getHAPolicyConfiguration();

      assertTrue(haConfig instanceof SharedStoreMasterPolicyConfiguration);

      SharedStoreMasterPolicyConfiguration masterConfig = (SharedStoreMasterPolicyConfiguration) haConfig;

      assertFalse(masterConfig.isWaitForActivation());
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
      DefaultSensitiveStringCodec codec = new DefaultSensitiveStringCodec();
      String mask = (String) codec.encode("helloworld");

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

      mask = (String) codec.encode("newpassword");

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
      DefaultSensitiveStringCodec codec = new DefaultSensitiveStringCodec();
      String mask = (String) codec.encode("helloworld");

      clusterPasswordPart = "<cluster-password>" + PasswordMaskingUtil.wrap(mask) + "</cluster-password>";

      configStr = firstPart + clusterPasswordPart + lastPart;

      config = parser.parseMainConfig(new ByteArrayInputStream(configStr.getBytes(StandardCharsets.UTF_8)));

      assertEquals("helloworld", config.getClusterPassword());

      //if we change key, it should be able to decode correctly
      codec = new DefaultSensitiveStringCodec();
      Map<String, String> prop = new HashMap<>();
      prop.put("key", "newkey");
      codec.init(prop);

      mask = (String) codec.encode("newpassword");

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
}
