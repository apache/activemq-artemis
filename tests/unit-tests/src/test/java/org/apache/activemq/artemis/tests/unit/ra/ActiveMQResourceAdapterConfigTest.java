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
package org.apache.activemq.artemis.tests.unit.ra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import javax.xml.parsers.DocumentBuilder;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.ra.ActiveMQResourceAdapter;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.XmlProvider;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * This test is used to generate the commented out configs in the src/config/ra.xml. If you add a setter to the ActiveMQResourceAdapter
 * this test should fail, if it does paste the new commented out configs into the ra.xml file and in here. don't forget to
 * add a description for each new property added and try and put it in the config some where appropriate.
 */
public class ActiveMQResourceAdapterConfigTest extends ActiveMQTestBase {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static String config = "" +
      "<config-property>\n" +
      "         <description>\n" +
      "            The transport type. Multiple connectors can be configured by using a comma separated list,\n" +
      "            i.e. org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory,org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory.\n" +
      "         </description>\n" +
      "         <config-property-name>ConnectorClassName</config-property-name>\n" +
      "         <config-property-type>java.lang.String</config-property-type>\n" +
      "         <config-property-value>org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>The transport configuration. These values must be in the form of key=val;key=val;,\n" +
      "            if multiple connectors are used then each set must be separated by a comma i.e. host=host1;port=61616,host=host2;port=61617.\n" +
      "            Each set of params maps to the connector classname specified.\n" +
      "         </description>\n" +
      "         <config-property-name>ConnectionParameters</config-property-name>\n" +
      "         <config-property-type>java.lang.String</config-property-type>\n" +
      "         <config-property-value>server-id=0</config-property-value>\n" +
      "      </config-property>";

   private static String commentedOutConfigs = "" +
      "      <config-property>\n" +
      "        <description>Does we support HA</description>\n" +
      "        <config-property-name>HA</config-property-name>\n" +
      "        <config-property-type>boolean</config-property-type>\n" +
      "        <config-property-value>false</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>Use A local Transaction instead of XA?</description>\n" +
      "        <config-property-name>UseLocalTx</config-property-name>\n" +
      "        <config-property-type>boolean</config-property-type>\n" +
      "        <config-property-value>false</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The user name used to login to the JMS server</description>\n" +
      "        <config-property-name>UserName</config-property-name>\n" +
      "        <config-property-type>java.lang.String</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The password used to login to the JMS server</description>\n" +
      "        <config-property-name>Password</config-property-name>\n" +
      "        <config-property-type>java.lang.String</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The jndi params to use to look up the jms resources if local jndi is not to be used</description>\n" +
      "        <config-property-name>JndiParams</config-property-name>\n" +
      "        <config-property-type>java.lang.String</config-property-type>\n" +
      "        <config-property-value>java.naming.factory.initial=org.jnp.interfaces.NamingContextFactory;java.naming.provider.url=jnp://localhost:1199;java.naming.factory.url.pkgs=org.jboss.naming:org.jnp.interfaces</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The jndi entries to use to look up the connection factory</description>\n" +
      "        <config-property-name>Entries</config-property-name>\n" +
      "        <config-property-type>java.lang.String</config-property-type>\n" +
      "        <config-property-value>[\"java://jmsXA\"]</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The discovery group address</description>\n" +
      "        <config-property-name>DiscoveryLocalBindAddress</config-property-name>\n" +
      "        <config-property-type>java.lang.String</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The discovery group local bind address</description>\n" +
      "        <config-property-name>DiscoveryAddress</config-property-name>\n" +
      "        <config-property-type>java.lang.String</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The discovery group port</description>\n" +
      "        <config-property-name>DiscoveryPort</config-property-name>\n" +
      "        <config-property-type>int</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The discovery refresh timeout</description>\n" +
      "        <config-property-name>DiscoveryRefreshTimeout</config-property-name>\n" +
      "        <config-property-type>long</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The discovery initial wait timeout</description>\n" +
      "        <config-property-name>DiscoveryInitialWaitTimeout</config-property-name>\n" +
      "        <config-property-type>long</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property> \n" +
      "      <config-property>\n" +
      "         <description>The class to use for load balancing connections</description>\n" +
      "         <config-property-name>ConnectionLoadBalancingPolicyClassName</config-property-name>\n" +
      "         <config-property-type>java.lang.String</config-property-type>\n" +
      "         <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>number of reconnect attempts for connections after failover occurs</description>\n" +
      "         <config-property-name>ReconnectAttempts</config-property-name>\n" +
      "         <config-property-type>int</config-property-type>\n" +
      "         <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The client failure check period</description>\n" +
      "        <config-property-name>ClientFailureCheckPeriod</config-property-name>\n" +
      "        <config-property-type>long</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The connection TTL</description>\n" +
      "        <config-property-name>ConnectionTTL</config-property-name>\n" +
      "        <config-property-type>long</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The call timeout</description>\n" +
      "        <config-property-name>CallTimeout</config-property-name>\n" +
      "        <config-property-type>long</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The dups ok batch size</description>\n" +
      "        <config-property-name>DupsOKBatchSize</config-property-name>\n" +
      "        <config-property-type>int</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The transaction batch size</description>\n" +
      "        <config-property-name>TransactionBatchSize</config-property-name>\n" +
      "        <config-property-type>int</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The consumer window size</description>\n" +
      "        <config-property-name>ConsumerWindowSize</config-property-name>\n" +
      "        <config-property-type>int</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The consumer max rate</description>\n" +
      "        <config-property-name>ConsumerMaxRate</config-property-name>\n" +
      "        <config-property-type>int</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The confirmation window size</description>\n" +
      "        <config-property-name>ConfirmationWindowSize</config-property-name>\n" +
      "        <config-property-type>int</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The producer max rate</description>\n" +
      "        <config-property-name>ProducerMaxRate</config-property-name>\n" +
      "        <config-property-type>int</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The min large message size</description>\n" +
      "        <config-property-name>MinLargeMessageSize</config-property-name>\n" +
      "        <config-property-type>int</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The block on acknowledge</description>\n" +
      "        <config-property-name>BlockOnAcknowledge</config-property-name>\n" +
      "        <config-property-type>boolean</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The block on non durable send</description>\n" +
      "        <config-property-name>BlockOnNonDurableSend</config-property-name>\n" +
      "        <config-property-type>boolean</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The block on durable send</description>\n" +
      "        <config-property-name>BlockOnDurableSend</config-property-name>\n" +
      "        <config-property-type>boolean</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The auto group</description>\n" +
      "        <config-property-name>AutoGroup</config-property-name>\n" +
      "        <config-property-type>boolean</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The pre acknowledge</description>\n" +
      "        <config-property-name>PreAcknowledge</config-property-name>\n" +
      "        <config-property-type>boolean</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The retry interval</description>\n" +
      "        <config-property-name>RetryInterval</config-property-name>\n" +
      "        <config-property-type>long</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The retry interval multiplier</description>\n" +
      "        <config-property-name>RetryIntervalMultiplier</config-property-name>\n" +
      "        <config-property-type>java.lang.Double</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "        <description>The client id</description>\n" +
      "        <config-property-name>ClientID</config-property-name>\n" +
      "        <config-property-type>java.lang.String</config-property-type>\n" +
      "        <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>use global pools for client</description>\n" +
      "         <config-property-name>UseGlobalPools</config-property-name>\n" +
      "         <config-property-type>boolean</config-property-type>\n" +
      "         <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>" +
      "         <description>Cache destinations per session</description>" +
      "         <config-property-name>CacheDestinations</config-property-name>" +
      "         <config-property-type>boolean</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>" +
      "      <config-property>\n" +
      "         <description>max number of threads for scheduled thread pool</description>\n" +
      "         <config-property-name>ScheduledThreadPoolMaxSize</config-property-name>\n" +
      "         <config-property-type>int</config-property-type>\n" +
      "         <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>max number of threads in pool</description>\n" +
      "         <config-property-name>ThreadPoolMaxSize</config-property-name>\n" +
      "         <config-property-type>int</config-property-type>\n" +
      "         <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>whether to use jndi for looking up destinations etc</description>\n" +
      "         <config-property-name>UseJNDI</config-property-name>\n" +
      "         <config-property-type>boolean</config-property-type>\n" +
      "         <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>how long in milliseconds to wait before retry on failed MDB setup</description>\n" +
      "         <config-property-name>SetupInterval</config-property-name>\n" +
      "         <config-property-type>long</config-property-type>\n" +
      "         <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>How many attempts should be made when connecting the MDB</description>\n" +
      "         <config-property-name>SetupAttempts</config-property-name>\n" +
      "         <config-property-type>int</config-property-type>\n" +
      "         <config-property-value></config-property-value>\n" +
      "      </config-property>" +
      "      <config-property>\n" +
      "         <description>Add a new managed connection factory</description>\n" +
      "         <config-property-name>ManagedConnectionFactory</config-property-name>\n" +
      "         <config-property-type>org.apache.activemq.artemis.ra.ActiveMQRAManagedConnectionFactory</config-property-type>\n" +
      "         <config-property-value></config-property-value>\n" +
      "      </config-property>" +
      "      <config-property>\n" +
      "         <description>Whether to use password mask, default false</description>\n" +
      "         <config-property-name>UseMaskedPassword</config-property-name>\n" +
      "         <config-property-type>boolean</config-property-type>\n" +
      "         <config-property-value>false</config-property-value>\n" +
      "      </config-property>" +
      "      <config-property>\n" +
      "         <description>The class definition (full qualified name and its properties) used to encrypt the password</description>\n" +
      "         <config-property-name>PasswordCodec</config-property-name>\n" +
      "         <config-property-type>java.lang.String</config-property-type>\n" +
      "         <config-property-value>org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec;key=clusterpassword;algorithm=ssss</config-property-value>\n" +
      "      </config-property>" +
      "      <config-property>" +
      "         <description>Whether the resource adapter must use auto recovery</description>\n" +
      "         <config-property-name>UseAutoRecovery</config-property-name>\n" +
      "         <config-property-type>boolean</config-property-type>\n" +
      "         <config-property-value>true</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>The initial size for message packet</description>\n" +
      "         <config-property-name>InitialMessagePacketSize</config-property-name>\n" +
      "         <config-property-type>int</config-property-type>\n" +
      "         <config-property-value>1500</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>the Group ID</description>\n" +
      "         <config-property-name>GroupID</config-property-name>\n" +
      "         <config-property-type>java.lang.String</config-property-type>\n" +
      "         <config-property-value></config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>Whether the resource adapter must failover on the initial connection</description>\n" +
      "         <config-property-name>FailoverOnInitialConnection</config-property-name>\n" +
      "         <config-property-type>boolean</config-property-type>\n" +
      "         <config-property-value>false</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>Whether the resource adapter compress large messages and send them as regular when possible</description>" +
      "         <config-property-name>CompressLargeMessage</config-property-name>" +
      "         <config-property-type>boolean</config-property-type>" +
      "         <config-property-value>false</config-property-value>" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>The level of compression to use. Must be -1 or between 0-9</description>" +
      "         <config-property-name>CompressionLevel</config-property-name>" +
      "         <config-property-type>int</config-property-type>" +
      "         <config-property-value>-1</config-property-value>" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>The timeout in milliseconds for failover call (or -1 for infinite)</description>\n" +
      "         <config-property-name>CallFailoverTimeout</config-property-name>\n" +
      "         <config-property-type>long</config-property-type>\n" +
      "         <config-property-value>-1</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>The maximum interval (in milliseconds) between reconnection attempts</description>\n" +
      "         <config-property-name>MaxRetryInterval</config-property-name>\n" +
      "         <config-property-type>long</config-property-type>\n" +
      "         <config-property-value>2000</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>Whether the resource adapter must cache large messages</description>\n" +
      "         <config-property-name>CacheLargeMessagesClient</config-property-name>\n" +
      "         <config-property-type>boolean</config-property-type>\n" +
      "         <config-property-value>false</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>The producer window size (in bytes)</description>\n" +
      "         <config-property-name>ProducerWindowSize</config-property-name>\n" +
      "         <config-property-type>int</config-property-type>\n" +
      "         <config-property-value>65536</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>\n" +
      "         <description>The number of attempts for the initial connection</description>\n" +
      "         <config-property-name>InitialConnectAttempts</config-property-name>\n" +
      "         <config-property-type>int</config-property-type>\n" +
      "         <config-property-value>1</config-property-value>\n" +
      "      </config-property>\n" +
      "      <config-property>" +
      "         <description>my-channel</description>" +
      "         <config-property-name>JgroupsChannelName</config-property-name>" +
      "         <config-property-type>java.lang.String</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>" +
      "      <config-property>" +
      "         <description>my-file</description>" +
      "         <config-property-name>JgroupsFile</config-property-name>" +
      "         <config-property-type>java.lang.String</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>" +
      "      <config-property>" +
      "         <description>Jgroups Channel Locator Class</description>" +
      "         <config-property-name>JgroupsChannelLocatorClass</config-property-name>" +
      "         <config-property-type>java.lang.String</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>" +
      "      <config-property>" +
      "         <description>Name to locate a JChannel by JGroups Channel locator class</description>" +
      "         <config-property-name>JgroupsChannelRefName</config-property-name>" +
      "         <config-property-type>java.lang.String</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>" +
      "      <config-property>" +
      "         <description>ProtocolManagerConfig</description>" +
      "         <config-property-name>ProtocolManagerFactoryStr</config-property-name>" +
      "         <config-property-type>java.lang.String</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>" +
      "      <config-property>" +
      "         <description>List of package/class names against which matching objects are permitted to be deserialized</description>" +
      "         <config-property-name>DeserializationWhiteList</config-property-name>" +
      "         <config-property-type>java.lang.String</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>" +
      "      <config-property>" +
      "         <description>List of package/class names against which matching objects are permitted to be deserialized</description>" +
      "         <config-property-name>DeserializationAllowList</config-property-name>" +
      "         <config-property-type>java.lang.String</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>" +
      "      <config-property>" +
      "         <description>List of package/class names against which matching objects are forbidden to be deserialized</description>" +
      "         <config-property-name>DeserializationBlackList</config-property-name>" +
      "         <config-property-type>java.lang.String</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>" +
      "      <config-property>" +
      "         <description>List of package/class names against which matching objects are forbidden to be deserialized</description>" +
      "         <config-property-name>DeserializationDenyList</config-property-name>" +
      "         <config-property-type>java.lang.String</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>" +
      "      <config-property>" +
      "         <description>***add***</description>" +
      "         <config-property-name>IgnoreJTA</config-property-name>" +
      "         <config-property-type>boolean</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>" +
      "      <config-property>" +
      "         <description>***add***</description>" +
      "         <config-property-name>Enable1xPrefixes</config-property-name>" +
      "         <config-property-type>boolean</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>" +
      "      <config-property>" +
      "         <description>***add***</description>" +
      "         <config-property-name>UseTopologyForLoadBalancing</config-property-name>" +
      "         <config-property-type>boolean</config-property-type>" +
      "         <config-property-value></config-property-value>" +
      "      </config-property>";

   private static String rootConfig = "<root>" + config + commentedOutConfigs + "</root>";

   @Test
   public void testConfiguration() throws Exception {
      Method[] methods = ActiveMQResourceAdapter.class.getMethods();
      Map<String, Method> methodList = new HashMap<>();
      for (Method method : methods) {
         if (method.getName().startsWith("set")) {
            methodList.put(method.getName(), method);
         }
      }
      DocumentBuilder db = XmlProvider.newDocumentBuilder();
      InputStream io = new ByteArrayInputStream(rootConfig.getBytes());
      Document dom = db.parse(new InputSource(io));

      Element docEle = dom.getDocumentElement();

      NodeList nl = docEle.getElementsByTagName("config-property");

      for (int i = 0; i < nl.getLength(); i++) {
         Element el = (Element) nl.item(i);
         NodeList elementsByTagName = el.getElementsByTagName("config-property-name");
         assertEquals(elementsByTagName.getLength(), 1, el.toString());
         Node configPropertyNameNode = elementsByTagName.item(0);
         String configPropertyName = configPropertyNameNode.getTextContent();
         Method setter = methodList.remove("set" + configPropertyName);
         assertNotNull(setter, "setter " + configPropertyName + " does not exist");
         Class c = lookupType(setter);
         elementsByTagName = el.getElementsByTagName("config-property-type");
         assertEquals(elementsByTagName.getLength(), 1, "setter " + configPropertyName + " has no type set");
         Node configPropertyTypeNode = elementsByTagName.item(0);
         String configPropertyTypeName = configPropertyTypeNode.getTextContent();
         assertEquals(configPropertyTypeName, c.getName());
      }
      if (!methodList.isEmpty()) {
         StringBuffer newConfig = new StringBuffer(commentedOutConfigs);
         newConfig.append("\n");
         for (Method method : methodList.values()) {
            newConfig.append("         \"      <config-property>\" + \n");
            newConfig.append("         \"         <description>***add***</description>\" + \n");
            newConfig.append("         \"         <config-property-name>").append(method.getName().substring(3)).append("</config-property-name>\" + \n");
            newConfig.append("         \"         <config-property-type>").append(lookupType(method).getName()).append("</config-property-type>\" + \n");
            newConfig.append("         \"         <config-property-value></config-property-value>\" + \n");
            newConfig.append("         \"      </config-property>\" + \n");
         }
         logger.debug(newConfig.toString());
         fail("methods not shown please see previous and add");
      }
   }

   /**
    * @param setter
    * @return
    */
   private Class<?> lookupType(Method setter) {
      Class<?> clzz = setter.getParameterTypes()[0];

      if (clzz == Boolean.class) {
         return Boolean.TYPE;
      } else if (clzz == Long.class) {
         return Long.TYPE;
      } else if (clzz == Integer.class) {
         return Integer.TYPE;
      } else {
         return clzz;
      }
   }
}
