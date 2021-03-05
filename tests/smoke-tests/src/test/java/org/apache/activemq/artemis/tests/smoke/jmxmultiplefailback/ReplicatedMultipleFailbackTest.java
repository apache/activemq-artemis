/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.smoke.jmxmultiplefailback;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.utils.Wait;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReplicatedMultipleFailbackTest extends SmokeTestBase {

   private static final Logger LOGGER = Logger.getLogger(ReplicatedMultipleFailbackTest.class);

   @FunctionalInterface
   interface ThrowableFunction<T, R> {

      R apply(T t) throws Throwable;
   }

   private static <C, T> Optional<T> queryControl(JMXServiceURL serviceURI,
                                                  ObjectName objectName,
                                                  ThrowableFunction<C, T> queryControl,
                                                  Class<C> controlClass,
                                                  Function<Throwable, T> onThrowable) {
      try {
         try (JMXConnector jmx = JMXConnectorFactory.connect(serviceURI)) {
            final C control = MBeanServerInvocationHandler.newProxyInstance(jmx.getMBeanServerConnection(), objectName, controlClass, false);
            return Optional.ofNullable(queryControl.apply(control));
         }
      } catch (Throwable t) {
         return Optional.ofNullable(onThrowable.apply(t));
      }
   }

   private static Optional<Boolean> isBackup(JMXServiceURL serviceURI, ObjectNameBuilder builder) throws Exception {
      return queryControl(serviceURI, builder.getActiveMQServerObjectName(), ActiveMQServerControl::isBackup, ActiveMQServerControl.class, throwable -> null);
   }

   private static Optional<String> getNodeID(JMXServiceURL serviceURI, ObjectNameBuilder builder) throws Exception {
      return queryControl(serviceURI, builder.getActiveMQServerObjectName(), ActiveMQServerControl::getNodeID, ActiveMQServerControl.class, throwable -> null);
   }

   private static Optional<String> listNetworkTopology(JMXServiceURL serviceURI,
                                                       ObjectNameBuilder builder) throws Exception {
      return queryControl(serviceURI, builder.getActiveMQServerObjectName(), ActiveMQServerControl::listNetworkTopology, ActiveMQServerControl.class, throwable -> null);
   }

   private static Map<String, Pair<String, String>> decodeNetworkTopologyJson(String networkTopologyJson) {
      if (networkTopologyJson == null || networkTopologyJson.isEmpty()) {
         return Collections.emptyMap();
      }
      try (JsonReader jsonReader = Json.createReader(new StringReader(networkTopologyJson))) {
         final JsonArray nodeIDs = jsonReader.readArray();
         final int nodeCount = nodeIDs.size();
         Map<String, Pair<String, String>> networkTopology = new HashMap<>(nodeCount);
         for (int i = 0; i < nodeCount; i++) {
            final JsonObject nodePair = nodeIDs.getJsonObject(i);
            try {
               final String nodeID = nodePair.getString("nodeID");
               final String live = nodePair.getString("live");
               final String backup = nodePair.getString("backup", null);
               networkTopology.put(nodeID, new Pair<>(live, backup));
            } catch (Exception e) {
               LOGGER.warnf(e, "Error on %s", nodePair);
            }
         }
         return networkTopology;
      }
   }

   private static long countMembers(Map<String, Pair<String, String>> networkTopology) {
      final long count = networkTopology.values().stream()
         .map(Pair::getA).filter(live -> live != null && !live.isEmpty())
         .count();
      return count;
   }

   private static long countNodes(Map<String, Pair<String, String>> networkTopology) {
      final long count =  networkTopology.values().stream()
         .flatMap(pair -> Stream.of(pair.getA(), pair.getB()))
         .filter(liveOrBackup -> liveOrBackup != null && !liveOrBackup.isEmpty())
         .count();
      return count;
   }

   private static boolean validateNetworkTopology(String networkTopologyJson,Predicate<Map<String, Pair<String, String>>> checkTopology) {
      final Map<String, Pair<String, String>> networkTopology = decodeNetworkTopologyJson(networkTopologyJson);
      return checkTopology.test(networkTopology);
   }

   private static String backupOf(String nodeID, Map<String, Pair<String, String>> networkTopology) {
      return networkTopology.get(nodeID).getB();
   }

   private static String liveOf(String nodeID, Map<String, Pair<String, String>> networkTopology) {
      return networkTopology.get(nodeID).getA();
   }

   private static Predicate<Map<String, Pair<String, String>>> containsExactNodeIds(String... nodeID) {
      Objects.requireNonNull(nodeID);
      return topology -> topology.size() == nodeID.length && Stream.of(nodeID).allMatch(topology::containsKey);
   }

   private static Predicate<Map<String, Pair<String, String>>> withMembers(int count) {
      return topology -> countMembers(topology) == count;
   }

   private static Predicate<Map<String, Pair<String, String>>> withNodes(int count) {
      return topology -> countNodes(topology) == count;
   }

   private static Predicate<Map<String, Pair<String, String>>> withBackup(String nodeId, Predicate<String> compare) {
      return topology -> compare.test(backupOf(nodeId, topology));
   }

   private static Predicate<Map<String, Pair<String, String>>> withLive(String nodeId, Predicate<String> compare) {
      return topology -> compare.test(liveOf(nodeId, topology));
   }

   private static final String JMX_SERVER_HOSTNAME = "localhost";
   private static final int JMX_PORT_MASTER_1 = 10099;
   private static final int JMX_PORT_MASTER_2 = 10199;
   private static final int JMX_PORT_MASTER_3 = 10299;
   private static final int JMX_PORT_SLAVE_1 = 10399;

   private static final String MASTER_1_DATA_FOLDER = "replicated-failback-master1";
   private static final String MASTER_2_DATA_FOLDER = "replicated-failback-master2";
   private static final String MASTER_3_DATA_FOLDER = "replicated-failback-master3";
   private static final String SLAVE_1_DATA_FOLDER = "replicated-failback-slave1";

   private static final int MASTER_1_PORT_ID = 0;
   private static final int MASTER_2_PORT_ID = MASTER_1_PORT_ID + 100;
   private static final int MASTER_3_PORT_ID = MASTER_2_PORT_ID + 100;
   private static final int SLAVE_1_PORT_ID = MASTER_3_PORT_ID + 100;

   private enum Broker {
      master1(JMX_PORT_MASTER_1, MASTER_1_DATA_FOLDER, MASTER_1_PORT_ID), master2(JMX_PORT_MASTER_2, MASTER_2_DATA_FOLDER, MASTER_2_PORT_ID), master3(JMX_PORT_MASTER_3, MASTER_3_DATA_FOLDER, MASTER_3_PORT_ID), slave1(JMX_PORT_SLAVE_1, SLAVE_1_DATA_FOLDER, SLAVE_1_PORT_ID);

      final ObjectNameBuilder objectNameBuilder;
      final String dataFolder;
      final JMXServiceURL jmxServiceURL;
      final int portID;

      Broker(int jmxPort, String dataFolder, int portID) {
         this.portID = portID;
         this.dataFolder = dataFolder;
         try {
            jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + jmxPort + "/jmxrmi");
         } catch (MalformedURLException e) {
            throw new RuntimeException(e);
         }
         this.objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), name(), true);
      }

      public Process startServer(SmokeTestBase env, int millisTimeout) throws Exception {
         return env.startServer(dataFolder, portID, millisTimeout);
      }

      public void cleanupData() {
         ReplicatedMultipleFailbackTest.cleanupData(dataFolder);
      }

      public Optional<Boolean> isBackup() throws Exception {
         return ReplicatedMultipleFailbackTest.isBackup(jmxServiceURL, objectNameBuilder);
      }

      public Optional<String> getNodeID() throws Exception {
         return ReplicatedMultipleFailbackTest.getNodeID(jmxServiceURL, objectNameBuilder);
      }

      public Optional<String> listNetworkTopology() throws Exception {
         return ReplicatedMultipleFailbackTest.listNetworkTopology(jmxServiceURL, objectNameBuilder);
      }
   }

   @Before
   public void before() {
      Stream.of(Broker.values()).forEach(Broker::cleanupData);
      disableCheckThread();
   }

   @Test
   public void testMultipleFailback() throws Exception {
      LOGGER.infof("TEST BOOTSTRAPPING START: STARTING brokers %s", Arrays.toString(Broker.values()));
      final int failbackRetries = 10;
      final int timeout = (int) TimeUnit.SECONDS.toMillis(30);
      Process master1 = Broker.master1.startServer(this, timeout);
      Wait.assertTrue(() -> !Broker.master1.isBackup().orElse(true), timeout);
      Broker.master2.startServer(this, timeout);
      Wait.assertTrue(() -> !Broker.master2.isBackup().orElse(true), timeout);
      Broker.master3.startServer(this, timeout);
      Wait.assertTrue(() -> !Broker.master3.isBackup().orElse(true), timeout);
      Broker.slave1.startServer(this, 0);
      Wait.assertTrue(() -> Broker.slave1.isBackup().orElse(false), timeout);

      final String nodeIDlive1 = Broker.master1.getNodeID().get();
      final String nodeIDlive2 = Broker.master2.getNodeID().get();
      final String nodeIDlive3 = Broker.master3.getNodeID().get();

      for (Broker broker : Broker.values()) {
         LOGGER.infof("CHECKING NETWORK TOPOLOGY FOR %s", broker);
         Wait.assertTrue(() -> validateNetworkTopology(broker.listNetworkTopology().orElse(""),
                                                       containsExactNodeIds(nodeIDlive1, nodeIDlive2, nodeIDlive3)
                                                          .and(withLive(nodeIDlive1, Objects::nonNull))
                                                          .and(withBackup(nodeIDlive1, Objects::nonNull))
                                                          .and(withMembers(3))
                                                          .and(withNodes(4))), timeout);
      }

      final String urlSlave1 = backupOf(nodeIDlive1, decodeNetworkTopologyJson(Broker.slave1.listNetworkTopology().get()));
      Assert.assertNotNull(urlSlave1);
      final String urlMaster1 = liveOf(nodeIDlive1, decodeNetworkTopologyJson(Broker.master1.listNetworkTopology().get()));
      Assert.assertNotNull(urlMaster1);
      Assert.assertNotEquals(urlMaster1, urlSlave1);

      LOGGER.infof("Node ID live 1 is %s", nodeIDlive1);
      LOGGER.infof("Node ID live 2 is %s", nodeIDlive2);
      LOGGER.infof("Node ID live 3 is %s", nodeIDlive3);

      LOGGER.infof("%s has url: %s", Broker.master1, urlMaster1);
      LOGGER.infof("%s has url: %s", Broker.slave1, urlSlave1);

      LOGGER.info("BOOTSTRAPPING ENDED: READ nodeIds and master1/slave1 urls");

      for (int i = 0; i < failbackRetries; i++) {
         LOGGER.infof("START TEST %d", i + 1);
         LOGGER.infof("KILLING master1");
         killServer(master1);
         // wait until slave1 became live
         Wait.assertTrue(() -> !Broker.slave1.isBackup().orElse(true), timeout);
         LOGGER.info("slave1 is LIVE");
         LOGGER.info("VALIDATE TOPOLOGY OF ALIVE BROKERS");
         Stream.of(Broker.master2, Broker.master3, Broker.slave1).forEach(
            broker -> Wait.assertTrue(() -> validateNetworkTopology(broker.listNetworkTopology().orElse(""),
                                                                    containsExactNodeIds(nodeIDlive1, nodeIDlive2, nodeIDlive3)
                                                                       .and(withLive(nodeIDlive1, urlSlave1::equals))
                                                                       .and(withBackup(nodeIDlive1, Objects::isNull))
                                                                       .and(withMembers(3))
                                                                       .and(withNodes(3))), timeout)
         );
         // restart master1
         LOGGER.info("STARTING master1");
         master1 = Broker.master1.startServer(this, 0);
         Wait.assertTrue(() -> Broker.slave1.isBackup().orElse(false), timeout);
         LOGGER.info("slave1 is BACKUP");
         Wait.assertTrue(() -> !Broker.master1.isBackup().orElse(true), timeout);
         LOGGER.info("master1 is LIVE");
         for (Broker broker : Broker.values()) {
            LOGGER.infof("CHECKING NETWORK TOPOLOGY FOR %s", broker);
            Wait.assertTrue(() -> validateNetworkTopology(broker.listNetworkTopology().orElse(""),
                                                          containsExactNodeIds(nodeIDlive1, nodeIDlive2, nodeIDlive3)
                                                             .and(withLive(nodeIDlive1, urlMaster1::equals))
                                                             .and(withBackup(nodeIDlive1, urlSlave1::equals))
                                                             .and(withMembers(3))
                                                             .and(withNodes(4))), timeout);
         }
      }
      LOGGER.info("TEST COMPLETED");
   }
}

