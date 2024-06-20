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

package org.apache.activemq.artemis.tests.integration.cluster.distribution;

import java.io.File;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.remote.JMXConnector;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.utils.RealServerTestBase;
import org.apache.activemq.artemis.utils.cli.helper.HelperCreate;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies ARTEMIS-4305. Servers 2 and 3 are deliberately configured on the same port.
 */
// to run locally on OSX see https://issues.apache.org/jira/browse/ARTEMIS-2341
public class ZeroPersistenceSymmetricalClusterTest extends RealServerTestBase {
   private static final Logger log = LoggerFactory.getLogger(ZeroPersistenceSymmetricalClusterTest.class);

   public static final String SERVER_NAME_0 = "0";
   public static final String SERVER_NAME_1 = "1";
   public static final String SERVER_NAME_2 = "2";
   public static final String SERVER_NAME_3 = "3";

   private static final int TOTAL_TOPOLOGY_AWAIT_SECONDS = 90;

   private ScheduledExecutorService s;

   @Override
   public void setUp() throws Exception {
      super.setUp();
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      cleanupData(SERVER_NAME_2);
      cleanupData(SERVER_NAME_3);
      s = Executors.newSingleThreadScheduledExecutor();
   }

   @Override
   public void after() throws Exception {
      super.after();
      s.shutdownNow();
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      cleanupData(SERVER_NAME_2);
      cleanupData(SERVER_NAME_3);
   }

   @Test
   public void test() throws Throwable {
      createServer(SERVER_NAME_0);
      createServer(SERVER_NAME_1);
      createServer(SERVER_NAME_2);
      Process s0 = startServer(SERVER_NAME_0, 0, 5000);
      Process s1 = startServer(SERVER_NAME_1, 1, 5000);
      Process s2 = startServer(SERVER_NAME_2, 2, 5000);
      await(
         expectTopology(SERVER_NAME_0, 0, 3),
         expectTopology(SERVER_NAME_1, 1, 3),
         expectTopology(SERVER_NAME_2, 2, 3)
      );
      killServer(s2, true);
      await(
         expectTopology(SERVER_NAME_0, 0, 2),
         expectTopology(SERVER_NAME_1, 1, 2)
      );
      createServer(SERVER_NAME_3);
      startServer(SERVER_NAME_3, 3, 5000);
      killServer(s0, true);
      killServer(s1, true);
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      await(expectTopology(SERVER_NAME_3, 3, 1));
      createServer(SERVER_NAME_0);
      createServer(SERVER_NAME_1);
      startServer(SERVER_NAME_0, 0, 5000);
      startServer(SERVER_NAME_1, 1, 5000);
      await(
         expectTopology(SERVER_NAME_0, 0, 3),
         expectTopology(SERVER_NAME_1, 1, 3),
         expectTopology(SERVER_NAME_3, 3, 3)
      );
   }

   private void createServer(String name) throws Exception {
      File serverLocation = getFileServerLocation(name);
      deleteDirectory(serverLocation);

      HelperCreate cliCreateServer = new HelperCreate()
         .setAllowAnonymous(true)
         .setNoWeb(true)
         .setArtemisInstance(serverLocation)
         .setConfiguration("./src/test/resources/servers/symmetric-discovery" + name)
         .setArgs("--java-options", "-Djava.rmi.server.hostname=localhost");
      cliCreateServer.createServer();
   }

   private void await(CompletableFuture<?>... tasks) throws Throwable {
      Throwable result = CompletableFuture
         .allOf(tasks)
         .orTimeout(TOTAL_TOPOLOGY_AWAIT_SECONDS, TimeUnit.SECONDS)
         .handle((v, t) -> t).join();
      if (result instanceof CompletionException && result.getCause() instanceof TimeoutException) {
         fail("The nodes did not get the expected topology on time");
      }
      if (result != null) {
         throw result;
      }
   }

   // wait to get the expected count, and then wait 5 more seconds and check again
   private CompletableFuture<?> expectTopology(String name, int node, int expectedCount) throws Exception {
      JMXConnector jmxConnector = getJmxConnector("localhost", 10090 + node);
      MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
      ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create(
         ActiveMQDefaultConfiguration.getDefaultJmxDomain(),
         name,
         true);

      ActiveMQServerControl control = MBeanServerInvocationHandler.newProxyInstance(
         mBeanServerConnection,
         objectNameBuilder.getActiveMQServerObjectName(),
         ActiveMQServerControl.class,
         false);
      CompletableFuture<?> monitor = new CompletableFuture<>().orTimeout(TOTAL_TOPOLOGY_AWAIT_SECONDS, TimeUnit.SECONDS);
      ScheduledFuture<?> notifier = s.scheduleAtFixedRate(() -> {
         try {
            int count = getNodeCount(name, control);
            log.debug("Node count at {}: {}", name, count);
            if (count == expectedCount) {
               s.schedule(() -> {
                  try {
                     int countAfter = getNodeCount(name, control);
                     if (countAfter == expectedCount) {
                        monitor.complete(null);
                     }
                  } catch (Exception e) {
                     monitor.completeExceptionally(e);
                  }
               }, 5, TimeUnit.SECONDS);
               throw new CancellationException();
            }
         } catch (CancellationException e) {
            throw e;
         } catch (Exception e) {
            monitor.completeExceptionally(e);
         }
      }, 500, 500, TimeUnit.MILLISECONDS);
      return monitor.whenComplete((v, t) -> {
         notifier.cancel(true);
         try {
            if (log.isDebugEnabled()) {
               log.debug("Final topology for {}:{}{}", name, System.lineSeparator(), control.listNetworkTopology());
            }
            jmxConnector.close();
         } catch (Exception e) {
            log.debug("Could not close the jmx connector");
         }
      });
   }

   private int getNodeCount(String serverName, ActiveMQServerControl control) {
      try {
         return control.countTopologyPeers("cluster0");
      } catch (Exception e) {
         log.debug("Could not get the node count of " + serverName, e);
         throw new RuntimeException(e);
      }
   }
}
