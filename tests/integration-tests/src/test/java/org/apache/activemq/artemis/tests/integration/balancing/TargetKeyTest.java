/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.balancing;

import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.balancing.policies.FirstElementPolicy;
import org.apache.activemq.artemis.core.server.balancing.policies.Policy;
import org.apache.activemq.artemis.core.server.balancing.policies.PolicyFactory;
import org.apache.activemq.artemis.core.server.balancing.policies.PolicyFactoryResolver;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKey;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.integration.security.SecurityTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RunWith(Parameterized.class)
public class TargetKeyTest extends BalancingTestBase {

   private static final String MOCK_POLICY_NAME = "MOCK_POLICY";

   @Parameterized.Parameters(name = "protocol: {0}")
   public static Collection<Object[]> data() {
      Collection<Object[]> data = new ArrayList<>();

      for (String protocol : Arrays.asList(new String[] {AMQP_PROTOCOL, CORE_PROTOCOL, OPENWIRE_PROTOCOL})) {
         data.add(new Object[] {protocol});
      }

      return data;
   }


   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = SecurityTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            path = resource.getFile();
            System.setProperty("java.security.auth.login.config", path);
         }
      }
   }

   private final String protocol;

   private final List<String> keys = new ArrayList<>();


   public TargetKeyTest(String protocol) {
      this.protocol = protocol;
   }

   @Before
   public void setup() throws Exception {
      PolicyFactoryResolver.getInstance().registerPolicyFactory(MOCK_POLICY_NAME,
         new PolicyFactory() {
            @Override
            public Policy create() {
               return new FirstElementPolicy(MOCK_POLICY_NAME) {
                  @Override
                  public Target selectTarget(List<Target> targets, String key) {
                     keys.add(key);
                     return super.selectTarget(targets, key);
                  }
               };
            }
         });
   }

   @Test
   public void testClientIDKey() throws Exception {
      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupBalancerServerWithDiscovery(0, TargetKey.CLIENT_ID, MOCK_POLICY_NAME, null, true, null, 1);
      startServers(0);

      ConnectionFactory connectionFactory = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 0, "test", null, null);

      keys.clear();

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
      }

      Assert.assertEquals(1, keys.size());
      Assert.assertEquals("test", keys.get(0));
   }

   @Test
   public void testSNIHostKey() throws Exception {
      String localHostname = "localhost.localdomain";

      if (!checkLocalHostname(localHostname)) {
         localHostname = "artemis.localtest.me";

         if (!checkLocalHostname(localHostname)) {
            localHostname = "localhost";

            Assume.assumeTrue(CORE_PROTOCOL.equals(protocol) && checkLocalHostname(localHostname));
         }
      }

      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      getDefaultServerAcceptor(0).getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      getDefaultServerAcceptor(0).getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "server-keystore.jks");
      getDefaultServerAcceptor(0).getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "securepass");

      setupBalancerServerWithDiscovery(0, TargetKey.SNI_HOST, MOCK_POLICY_NAME, null, true, null, 1);
      startServers(0);

      ConnectionFactory connectionFactory = createFactory(protocol, true, localHostname,
         TransportConstants.DEFAULT_PORT + 0, null, null, null);

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
      }

      Assert.assertEquals(1, keys.size());
      Assert.assertEquals(localHostname, keys.get(0));
   }

   @Test
   public void testSourceIPKey() throws Exception {
      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupBalancerServerWithDiscovery(0, TargetKey.SOURCE_IP, MOCK_POLICY_NAME, null, true, null, 1);
      startServers(0);

      ConnectionFactory connectionFactory = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 0, null, null, null);

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
      }

      Assert.assertEquals(1, keys.size());
      Assert.assertEquals(InetAddress.getLoopbackAddress().getHostAddress(), keys.get(0));
   }

   @Test
   public void testUserNameKey() throws Exception {
      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupBalancerServerWithDiscovery(0, TargetKey.USER_NAME, MOCK_POLICY_NAME, null, true, null, 1);
      startServers(0);

      ConnectionFactory connectionFactory = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
         TransportConstants.DEFAULT_PORT + 0, null, "admin", "admin");

      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
      }

      Assert.assertEquals(1, keys.size());
      Assert.assertEquals("admin", keys.get(0));
   }

   @Test
   public void testRoleNameKeyLocalTarget() throws Exception {

      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("PropertiesLogin");
      servers[0] = addServer(ActiveMQServers.newActiveMQServer(createDefaultConfig(true).setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      setupBalancerServerWithLocalTarget(0, TargetKey.ROLE_NAME, "b", "b");

      // ensure advisory permission is present for openwire connection creation by 'b'
      HierarchicalRepository<Set<Role>> securityRepository = servers[0].getSecurityRepository();
      Role role = new Role("b", true, true, true, true, true, true, false, false, true, true);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch("ActiveMQ.Advisory.#", roles);

      startServers(0);

      final int noRetriesSuchThatWeGetAnErrorOnRejection = 0;
      ConnectionFactory connectionFactory = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
                                                          TransportConstants.DEFAULT_PORT + 0, null, "a", "a", noRetriesSuchThatWeGetAnErrorOnRejection);

      // expect disconnect/reject as not role b
      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
         fail("Expect to be rejected as not in role b");
      } catch (Exception expectedButNotSpecificDueToDifferentProtocolsInPlay) {
      }

      connectionFactory = createFactory(protocol, false, TransportConstants.DEFAULT_HOST,
                                        TransportConstants.DEFAULT_PORT + 0, null, "b", "b");

      // expect to be accepted, b has role b
      try (Connection connection = connectionFactory.createConnection()) {
         connection.start();
      }
   }

   private boolean checkLocalHostname(String host) {
      try {
         return InetAddress.getByName(host).isLoopbackAddress();
      } catch (UnknownHostException ignore) {
         return false;
      }
   }
}
