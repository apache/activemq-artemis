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

package org.apache.activemq.cli.test;

import java.io.File;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.CLIException;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.cli.commands.check.NodeCheck;
import org.apache.activemq.artemis.cli.commands.check.QueueCheck;
import org.apache.activemq.artemis.cli.commands.tools.LockAbstract;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Test;

public class CheckTest extends CliTestBase {
   final String queueName = "TEST";

   @Test
   public void testNodeCheckUp() throws Exception {
      NodeCheck nodeCheck;
      TestActionContext context;

      startServer();

      try {
         context = new TestActionContext();
         nodeCheck = new NodeCheck();
         nodeCheck.setUser("admin");
         nodeCheck.setPassword("admin");
         Assert.assertEquals(1, nodeCheck.execute(context));

         context = new TestActionContext();
         nodeCheck = new NodeCheck();
         nodeCheck.setUser("admin");
         nodeCheck.setPassword("admin");
         nodeCheck.setUp(true);
         Assert.assertEquals(1, nodeCheck.execute(context));
      } finally {
         stopServer();
      }
   }

   @Test
   public void testNodeCheckDiskUsage() throws Exception {
      NodeCheck nodeCheck;
      TestActionContext context;

      startServer();

      try {
         context = new TestActionContext();
         nodeCheck = new NodeCheck();
         nodeCheck.setUser("admin");
         nodeCheck.setPassword("admin");
         nodeCheck.setDiskUsage(-1);
         Assert.assertEquals(1, nodeCheck.execute(context));

         context = new TestActionContext();
         nodeCheck = new NodeCheck();
         nodeCheck.setUser("admin");
         nodeCheck.setPassword("admin");
         nodeCheck.setDiskUsage(90);
         Assert.assertEquals(1, nodeCheck.execute(context));

         try {
            context = new TestActionContext();
            nodeCheck = new NodeCheck();
            nodeCheck.setUser("admin");
            nodeCheck.setPassword("admin");
            nodeCheck.setDiskUsage(0);
            nodeCheck.execute(context);

            Assert.fail("CLIException expected.");
         } catch (Exception e) {
            Assert.assertTrue("CLIException expected.", e instanceof CLIException);
         }
      } finally {
         stopServer();
      }
   }

   @Test
   public void testNodeCheckMemoryUsage() throws Exception {
      NodeCheck nodeCheck;
      TestActionContext context;

      startServer();

      try {
         context = new TestActionContext();
         nodeCheck = new NodeCheck();
         nodeCheck.setUser("admin");
         nodeCheck.setPassword("admin");
         nodeCheck.setMemoryUsage(90);
         Assert.assertEquals(1, nodeCheck.execute(context));

         try {
            context = new TestActionContext();
            nodeCheck = new NodeCheck();
            nodeCheck.setUser("admin");
            nodeCheck.setPassword("admin");
            nodeCheck.setMemoryUsage(-1);
            nodeCheck.execute(context);

            Assert.fail("CLIException expected.");
         } catch (Exception e) {
            Assert.assertTrue("CLIException expected.", e instanceof CLIException);
         }
      } finally {
         stopServer();
      }
   }

   @Test
   public void testNodeCheckTopology() throws Exception {
      NodeCheck nodeCheck;
      TestActionContext context;

      File masterInstance = new File(temporaryFolder.getRoot(), "masterInstance");
      File slaveInstance = new File(temporaryFolder.getRoot(), "slaveInstance");

      Run.setEmbedded(true);
      setupAuth(masterInstance);

      Artemis.main("create", masterInstance.getAbsolutePath(), "--cluster-password", "artemis", "--cluster-user", "artemis", "--clustered",
                   "--replicated", "--host", "127.0.0.1", "--default-port", "61616", "--silent", "--no-autotune", "--no-web", "--require-login");
      Artemis.main("create", slaveInstance.getAbsolutePath(), "--cluster-password", "artemis", "--cluster-user", "artemis", "--clustered",
                   "--replicated", "--host", "127.0.0.1", "--default-port", "61626", "--silent", "--no-autotune", "--no-web", "--require-login", "--slave");

      System.setProperty("artemis.instance", masterInstance.getAbsolutePath());
      Object master = Artemis.execute(false, null, masterInstance, "run");
      ActiveMQServerImpl masterServer = (ActiveMQServerImpl)((Pair)master).getB();

      try {
         Wait.assertTrue("Master isn't active", () -> masterServer.isActive(), 10000);

         context = new TestActionContext();
         nodeCheck = new NodeCheck();
         nodeCheck.setUser("admin");
         nodeCheck.setPassword("admin");
         nodeCheck.setLive(true);
         Assert.assertEquals(1, nodeCheck.execute(context));

         try {
            context = new TestActionContext();
            nodeCheck = new NodeCheck();
            nodeCheck.setUser("admin");
            nodeCheck.setPassword("admin");
            nodeCheck.setBackup(true);
            nodeCheck.execute(context);

            Assert.fail("CLIException expected.");
         } catch (Exception e) {
            Assert.assertTrue("CLIException expected.", e instanceof CLIException);
         }

         LockAbstract.unlock();
         Object slave = Artemis.execute(false, null, slaveInstance, "run");
         ActiveMQServerImpl slaveServer = (ActiveMQServerImpl)((Pair)slave).getB();

         Wait.assertTrue("Backup isn't announced", () -> slaveServer.getBackupManager() != null &&
            slaveServer.getBackupManager().isStarted() && slaveServer.getBackupManager().isBackupAnnounced(), 30000);

         try {
            context = new TestActionContext();
            nodeCheck = new NodeCheck();
            nodeCheck.setUser("admin");
            nodeCheck.setPassword("admin");
            nodeCheck.setLive(true);
            nodeCheck.setBackup(true);
            nodeCheck.setPeers(2);
            Assert.assertEquals(3, nodeCheck.execute(context));
         } finally {
            Artemis.internalExecute(null, slaveInstance, new String[] {"stop"}, ActionContext.system());
         }
      } finally {
         stopServer();
      }
   }

   @Test
   public void testQueueCheckUp() throws Exception {
      QueueCheck queueCheck;
      TestActionContext context;

      Object serverInstance = startServer();

      try {
         ActiveMQServerImpl server = (ActiveMQServerImpl)((Pair)serverInstance).getB();

         try {
            context = new TestActionContext();
            queueCheck = new QueueCheck();
            queueCheck.setUser("admin");
            queueCheck.setPassword("admin");
            queueCheck.setName(queueName);
            queueCheck.execute(context);

            Assert.fail("CLIException expected.");
         } catch (Exception e) {
            Assert.assertTrue("CLIException expected.", e instanceof CLIException);
         }

         server.createQueue(new QueueConfiguration(queueName));

         context = new TestActionContext();
         queueCheck = new QueueCheck();
         queueCheck.setUser("admin");
         queueCheck.setPassword("admin");
         queueCheck.setName(queueName);
         Assert.assertEquals(1, queueCheck.execute(context));

         context = new TestActionContext();
         queueCheck = new QueueCheck();
         queueCheck.setUser("admin");
         queueCheck.setPassword("admin");
         queueCheck.setUp(true);
         queueCheck.setName(queueName);
         Assert.assertEquals(1, queueCheck.execute(context));
      } finally {
         stopServer();
      }
   }

   @Test
   public void testQueueCheckBrowse() throws Exception {
      final int messages = 3;

      QueueCheck queueCheck;
      TestActionContext context;

      Object serverInstance = startServer();

      try {
         ActiveMQServerImpl server = (ActiveMQServerImpl)((Pair)serverInstance).getB();

         server.createQueue(new QueueConfiguration(queueName));

         context = new TestActionContext();
         queueCheck = new QueueCheck();
         queueCheck.setUser("admin");
         queueCheck.setPassword("admin");
         queueCheck.setName(queueName);
         queueCheck.setBrowse(null);
         Assert.assertEquals(1, queueCheck.execute(context));

         QueueControl queueControl = (QueueControl)server.getManagementService().
            getResource(ResourceNames.QUEUE + queueName);

         for (int i = 0; i < messages; i++) {
            queueControl.sendMessage(null, Message.BYTES_TYPE, Base64.encodeBytes(
               queueName.getBytes()), true, "admin", "admin");
         }

         context = new TestActionContext();
         queueCheck = new QueueCheck();
         queueCheck.setUser("admin");
         queueCheck.setPassword("admin");
         queueCheck.setName(queueName);
         queueCheck.setBrowse(messages);
         Assert.assertEquals(1, queueCheck.execute(context));
      } finally {
         stopServer();
      }
   }

   @Test
   public void testQueueCheckConsume() throws Exception {
      final int messages = 3;

      QueueCheck queueCheck;
      TestActionContext context;

      Object serverInstance = startServer();

      try {
         ActiveMQServerImpl server = (ActiveMQServerImpl)((Pair)serverInstance).getB();

         server.createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));

         context = new TestActionContext();
         queueCheck = new QueueCheck();
         queueCheck.setUser("admin");
         queueCheck.setPassword("admin");
         queueCheck.setName(queueName);
         queueCheck.setConsume(null);
         Assert.assertEquals(1, queueCheck.execute(context));

         QueueControl queueControl = (QueueControl)server.getManagementService().
            getResource(ResourceNames.QUEUE + queueName);

         for (int i = 0; i < messages; i++) {
            queueControl.sendMessage(null, Message.BYTES_TYPE, Base64.encodeBytes(
               queueName.getBytes()), true, "admin", "admin");
         }

         context = new TestActionContext();
         queueCheck = new QueueCheck();
         queueCheck.setUser("admin");
         queueCheck.setPassword("admin");
         queueCheck.setName(queueName);
         queueCheck.setConsume(messages);
         Assert.assertEquals(1, queueCheck.execute(context));
      } finally {
         stopServer();
      }
   }

   @Test
   public void testQueueCheckConsumeTimeout() throws Exception {
      QueueCheck queueCheck;
      TestActionContext context;

      startServer();

      try {
         try {
            context = new TestActionContext();
            queueCheck = new QueueCheck();
            queueCheck.setUser("admin");
            queueCheck.setPassword("admin");
            queueCheck.setName(queueName);
            queueCheck.setConsume(1);
            queueCheck.setTimeout(100);
            queueCheck.execute(context);

            Assert.fail("CLIException expected.");
         } catch (Exception e) {
            Assert.assertTrue("CLIException expected.", e instanceof CLIException);
         }
      } finally {
         stopServer();
      }
   }

   @Test
   public void testQueueCheckProduce() throws Exception {
      final int messages = 3;

      QueueCheck queueCheck;
      TestActionContext context;
      QueueControl queueControl;

      Object serverInstance = startServer();
      ActiveMQServerImpl server = (ActiveMQServerImpl)((Pair)serverInstance).getB();

      try {
         context = new TestActionContext();
         queueCheck = new QueueCheck();
         queueCheck.setUser("admin");
         queueCheck.setPassword("admin");
         queueCheck.setName(queueName);
         queueCheck.setProduce(messages);
         Assert.assertEquals(1, queueCheck.execute(context));

         queueControl = (QueueControl)server.getManagementService().
            getResource(ResourceNames.QUEUE + queueName);
         Wait.assertEquals(messages, queueControl::getMessageCount);
      } finally {
         stopServer();
      }
   }

   @Test
   public void testQueueCheckProduceAndConsume() throws Exception {
      final int messages = 3;

      QueueCheck queueCheck;
      TestActionContext context;

      Object serverInstance = startServer();
      ActiveMQServerImpl server = (ActiveMQServerImpl)((Pair)serverInstance).getB();

      server.createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));

      try {
         context = new TestActionContext();
         queueCheck = new QueueCheck();
         queueCheck.setUser("admin");
         queueCheck.setPassword("admin");
         queueCheck.setName(queueName);
         queueCheck.setProduce(messages);
         queueCheck.setBrowse(messages);
         queueCheck.setConsume(messages);
         Assert.assertEquals(3, queueCheck.execute(context));

         QueueControl queueControl = (QueueControl)server.getManagementService().
            getResource(ResourceNames.QUEUE + queueName);
         Assert.assertEquals(0, queueControl.getMessageCount());
      } finally {
         stopServer();
      }
   }
}
