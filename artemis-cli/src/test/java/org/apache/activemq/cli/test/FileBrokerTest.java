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
package org.apache.activemq.cli.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.dto.ServerDTO;
import org.apache.activemq.artemis.integration.FileBroker;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

public class FileBrokerTest {

   @Test
   public void startWithJMS() throws Exception {
      ServerDTO serverDTO = new ServerDTO();
      serverDTO.configuration = "broker.xml";
      FileBroker broker = null;
      try {
         broker = new FileBroker(serverDTO, new ActiveMQJAASSecurityManager());
         broker.start();
         JMSServerManagerImpl jmsServerManager = (JMSServerManagerImpl) broker.getComponents().get("jms");
         Assert.assertNotNull(jmsServerManager);
         Assert.assertTrue(jmsServerManager.isStarted());
         //this tells us the jms server is activated
         Assert.assertTrue(jmsServerManager.getJMSStorageManager().isStarted());
         ActiveMQServerImpl activeMQServer = (ActiveMQServerImpl) broker.getComponents().get("core");
         Assert.assertNotNull(activeMQServer);
         Assert.assertTrue(activeMQServer.isStarted());
         Assert.assertTrue(broker.isStarted());
      } finally {
         if (broker != null) {
            broker.stop();
         }
      }
   }

   @Test
   public void startWithoutJMS() throws Exception {
      ServerDTO serverDTO = new ServerDTO();
      serverDTO.configuration = "broker-nojms.xml";
      FileBroker broker = null;
      try {
         broker = new FileBroker(serverDTO, new ActiveMQJAASSecurityManager());
         broker.start();
         JMSServerManagerImpl jmsServerManager = (JMSServerManagerImpl) broker.getComponents().get("jms");
         Assert.assertNull(jmsServerManager);
         ActiveMQServerImpl activeMQServer = (ActiveMQServerImpl) broker.getComponents().get("core");
         Assert.assertNotNull(activeMQServer);
         Assert.assertTrue(activeMQServer.isStarted());
         Assert.assertTrue(broker.isStarted());
      } finally {
         assert broker != null;
         broker.stop();
      }
   }

   @Test
   public void testConfigFileReload() throws Exception {
      ServerDTO serverDTO = new ServerDTO();
      serverDTO.configuration = "broker-reload.xml";
      FileBroker broker = null;
      String path = null;
      try {
         SecurityConfiguration securityConfiguration = new SecurityConfiguration();
         securityConfiguration.addUser("myUser", "myPass");
         securityConfiguration.addRole("myUser", "guest");
         ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), securityConfiguration);
         broker = new FileBroker(serverDTO, securityManager);
         broker.start();
         ActiveMQServerImpl activeMQServer = (ActiveMQServerImpl) broker.getComponents().get("core");
         Assert.assertNotNull(activeMQServer);
         Assert.assertTrue(activeMQServer.isStarted());
         Assert.assertTrue(broker.isStarted());
         File file = new File(activeMQServer.getConfiguration().getConfigurationUrl().toURI());
         path = file.getPath();
         Assert.assertNotNull(activeMQServer.getConfiguration().getConfigurationUrl());

         Thread.sleep(activeMQServer.getConfiguration().getConfigurationFileRefreshPeriod() * 2);

         ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61616");
         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession("myUser", "myPass", false, true, false, false, 0);
         ClientProducer producer = session.createProducer("jms.queue.DLQ");
         producer.send(session.createMessage(true));

         replacePatternInFile(path, "guest", "X");

         Thread.sleep(activeMQServer.getConfiguration().getConfigurationFileRefreshPeriod() * 2);

         try {
            producer.send(session.createMessage(true));
            fail("Should throw a security exception");
         } catch (Exception e) {
         }

         locator.close();
      } finally {
         assert broker != null;
         broker.stop();
         if (path != null) {
            replacePatternInFile(path, "X", "guest");
         }
      }
   }

   private void replacePatternInFile(String file, String regex, String replacement) throws IOException {
      Path path = Paths.get(file);
      Charset charset = StandardCharsets.UTF_8;
      String content = new String(Files.readAllBytes(path), charset);
      String replaced = content.replaceAll(regex, replacement);
      Files.write(path, replaced.getBytes(charset));
      Files.setLastModifiedTime(path, FileTime.fromMillis(System.currentTimeMillis()));
   }
}
