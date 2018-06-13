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
package org.apache.activemq.artemis.tests.integration.management;

import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.security.jaas.PropertiesLoginModuleTest;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.management.ManagementContext;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.dto.ManagementContextDTO;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.jms.Connection;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public abstract class JmxSecurityTestBase extends ActiveMQTestBase {

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = PropertiesLoginModuleTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            try {
               path = URLDecoder.decode(resource.getFile(), "UTF-8");
               System.setProperty("java.security.auth.login.config", path);
            } catch (UnsupportedEncodingException e) {
               throw new RuntimeException(e);
            }
         }
      }
      System.setProperty("java.rmi.server.hostname", "localhost");
   }

   @Rule
   public TemporaryFolder tmpTestFolder = new TemporaryFolder();

   protected ActiveMQServer server;
   protected MBeanServer mbeanServer;
   protected String brokerName = "amq";
   protected ObjectNameBuilder objectNameBuilder;
   protected ManagementContext mcontext;

   protected List<Connection> connections = new ArrayList<>();
   protected javax.jms.ConnectionFactory factory;

   protected String jmxServiceURL = null;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      //don't check thread leak against underlying rmi threads
      leakCheckRule.disable();

      jmxServiceURL = "service:jmx:rmi://localhost/jndi/rmi://localhost:" + getJmxPort() + "/jmxrmi";

      mcontext = configureJmxAccess();
      if (mcontext != null) mcontext.start();

      server = createServerWithJaas();
      Configuration serverConfig = server.getConfiguration();
      serverConfig.setJMXManagementEnabled(true);
      serverConfig.setName(brokerName);
      String dataDir = this.temporaryFolder.getRoot().getAbsolutePath();
      serverConfig.setPagingDirectory(dataDir + "/" + serverConfig.getPagingDirectory());
      serverConfig.setBindingsDirectory(dataDir + "/" + serverConfig.getBindingsDirectory());
      serverConfig.setLargeMessagesDirectory(dataDir + "/" + serverConfig.getLargeMessagesDirectory());
      serverConfig.setJournalDirectory(dataDir + "/" + serverConfig.getJournalDirectory());

      server.start();
      factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
      objectNameBuilder = server.getManagementService().getObjectNameBuilder();
   }

   protected int getJmxPort() {
      return 11099;
   }

   private ActiveMQServer createServerWithJaas() throws Exception {
      Configuration configuration = this.createDefaultConfig(true);
      ActiveMQSecurityManager securityManager = new ActiveMQJAASSecurityManager("jmx-security");
      MBeanServer mbs = mcontext.getMBeanServer();
      ActiveMQServer server = this.addServer(ActiveMQServers.newActiveMQServer(configuration, mbs, securityManager, true));

      AddressSettings defaultSetting = (new AddressSettings()).setPageSizeBytes(10485760L).setMaxSizeBytes(-1L).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      server.getAddressSettingsRepository().addMatch("#", defaultSetting);
      return server;
   }

   protected ManagementContext configureJmxAccess() throws Exception {
      //cleanup existing platform mbeanserver
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      Field field = ManagementFactory.class.getDeclaredField("platformMBeanServer");
      field.setAccessible(true);
      field.set(null, null);
      MBeanServerFactory.releaseMBeanServer(mBeanServer);

      ManagementContextDTO managementDTO = getManagementDTO();
      ManagementContext managementContext = org.apache.activemq.artemis.cli.factory.jmx.ManagementFactory.create(managementDTO);
      return managementContext;
   }

   private ManagementContextDTO getManagementDTO() throws Exception {
      URL url = getClass().getClassLoader().getResource(getManagementFileName());
      String configuration = "xml:" + url.toURI().toString().substring("file:".length());

      configuration = configuration.replace("\\", "/");

      ManagementContextDTO mContextDTO = org.apache.activemq.artemis.cli.factory.jmx.ManagementFactory.createJmxAclConfiguration(configuration, null, null, null);
      return mContextDTO;
   }

   protected String getManagementFileName() {
      return "management.xml";
   }

   @Override
   @After
   public void tearDown() throws Exception {
      if (mcontext != null) {
         mcontext.stop();
         mcontext = null;
      }
      for (Connection conn : connections) {
         try {
            conn.close();
         } catch (Exception e) {
            //ignore
         }
      }
      server.stop();
      System.out.println("server stopped");
      super.tearDown();
   }

   protected void createConnection() throws Exception {
      Connection conn = factory.createConnection();
      connections.add(conn);
   }

   protected JMXConnector connectJmxWithAuthen(String user, String password) throws IOException {
      return connectJmxWithAuthen(user, password, true);
   }

   protected JMXConnector connectJmxWithAuthen(String user, String password, boolean drop) throws IOException {

      HashMap env = new HashMap();

      String[] credentials = new String[]{user, password};
      env.put("jmx.remote.credentials", credentials);

      JMXServiceURL url = new JMXServiceURL(this.jmxServiceURL);

      JMXConnector jmxc = null;
      try {
         jmxc = JMXConnectorFactory.connect(url, env);
         jmxc.connect();
      } finally {
         if (drop && jmxc != null) {
            jmxc.close();
         }
      }
      return jmxc;
   }

   //Current implementation doesn't use Jaas Authenticator
   //if authentication-type is configured as "certificate"
   //(i.e. SSL with client auth).
   protected boolean isUseJaas() {
      return true;
   }
}
