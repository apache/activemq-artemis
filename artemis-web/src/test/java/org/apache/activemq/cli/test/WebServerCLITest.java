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
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.cli.commands.tools.LockAbstract;
import org.apache.activemq.artemis.component.WebTmpCleaner;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.management.ManagementContext;
import org.apache.activemq.artemis.nativo.jlibaio.LibaioContext;
import org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoader;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class WebServerCLITest {

   @Rule
   public TemporaryFolder temporaryFolder;

   @Rule
   public ThreadLeakCheckRule leakCheckRule = new ThreadLeakCheckRule();

   private String original = System.getProperty("java.security.auth.login.config");

   public WebServerCLITest() {
      File parent = new File("./target/tmp");
      parent.mkdirs();
      temporaryFolder = new TemporaryFolder(parent);
   }

   @Before
   public void setup() throws Exception {
      System.setProperty("java.security.auth.login.config", temporaryFolder.getRoot().getAbsolutePath() + "/etc/login.config");
      Run.setEmbedded(true);
      PropertiesLoader.resetUsersAndGroupsCache();
   }

   @After
   public void tearDown() throws Exception {
      ActiveMQClient.clearThreadPools();
      System.clearProperty("artemis.instance");
      System.clearProperty("artemis.instance.etc");
      Run.setEmbedded(false);

      if (original == null) {
         System.clearProperty("java.security.auth.login.config");
      } else {
         System.setProperty("java.security.auth.login.config", original);
      }

      LockAbstract.unlock();
   }

   @Test
   public void testStopEmbeddedWebServerOnCriticalIOError() throws Exception {
      Run.setEmbedded(true);
      File instance1 = new File(temporaryFolder.getRoot(), "instance_user");
      System.setProperty("java.security.auth.login.config", instance1.getAbsolutePath() + "/etc/login.config");
      Artemis.main("create", instance1.getAbsolutePath(), "--silent", "--no-autotune", "--no-amqp-acceptor", "--no-mqtt-acceptor", "--no-stomp-acceptor", "--no-hornetq-acceptor");
      System.setProperty("artemis.instance", instance1.getAbsolutePath());
      Object result = Artemis.internalExecute("run");
      ActiveMQServer activeMQServer = ((Pair<ManagementContext, ActiveMQServer>) result).getB();
      List<ActiveMQComponent> externalComponents = activeMQServer.getExternalComponents();

      /**
       * simulate critical IO error as this is what is eventually called by org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl.ShutdownOnCriticalErrorListener
       */
      ((ActiveMQServerImpl) activeMQServer).stop(false, true, false);

      for (ActiveMQComponent externalComponent : externalComponents) {
         assertTrue(Wait.waitFor(() -> externalComponent.isStarted() == false, 3000, 100));
      }
      stopServer();
   }


   @Test
   public void testCleanupFolder() throws Exception {
      List<File> fileList = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
         File directory =  temporaryFolder.newFolder("test & output " + i);
         fillup(directory);
      }
      Process process = WebTmpCleaner.cleanupTmpFiles(null, fileList);
      Assert.assertEquals(0, process.waitFor());
      for (File f : fileList) {
         Assert.assertFalse(f.exists());
      }

   }

   private void fillup(File file) throws Exception {
      file.mkdirs();
      for (int i = 0; i < 10; i++) {
         File fi = new File(file, "file" + i + ".txt");
         PrintStream str = new PrintStream(new FileOutputStream(fi));
         str.println("hello");
         str.close();
      }
   }

   private void stopServer() throws Exception {
      Artemis.internalExecute("stop");
      assertTrue(Run.latchRunning.await(5, TimeUnit.SECONDS));
      assertEquals(0, LibaioContext.getTotalMaxIO());
   }
}
