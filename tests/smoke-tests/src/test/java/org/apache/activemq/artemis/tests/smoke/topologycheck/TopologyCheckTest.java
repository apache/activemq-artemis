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

package org.apache.activemq.artemis.tests.smoke.topologycheck;

import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.tools.cluster.ClusterVerifier;
import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.json.JsonString;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologyCheckTest extends SmokeTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String SERVER_NAME_1 = "topology-check/broker1";
   private static final String SERVER_NAME_2 = "topology-check/broker2";
   private static final String SERVER_NAME_3 = "topology-check/broker3";
   private static final String SERVER_NAME_4 = "topology-check/broker4";

   private static final String URI_1 = "tcp://localhost:61616";
   private static final String URI_2 = "tcp://localhost:61617";
   private static final String URI_3 = "tcp://localhost:61618";
   private static final String URI_4 = "tcp://localhost:61619";

   Process[] process = new Process[4];
   String[] URLS = new String[]{URI_1, URI_2, URI_3, URI_4};
   String[] SERVER_NAMES = new String[]{SERVER_NAME_1, SERVER_NAME_2, SERVER_NAME_3, SERVER_NAME_4};

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_1);
      cleanupData(SERVER_NAME_2);
      cleanupData(SERVER_NAME_3);
      cleanupData(SERVER_NAME_4);
      for (int i = 0; i < process.length; i++) {
         process[i] = startServer(SERVER_NAMES[i], i, 0);
      }

      for (int i = 0; i < process.length; i++) {
         ServerUtil.waitForServerToStart(i, "admin", "admin", 30_000);
      }

      disableCheckThread();
   }

   @Test
   public void testCheckTopology() throws Throwable {
      String[] uris = new String[]{URI_1, URI_2, URI_3, URI_4};
      String[] nodes = new String[4];
      for (int i = 0; i < 4; i++) {
         SimpleManagement simpleManagement = new SimpleManagement(uris[i], "admin", "admin");
         nodes[i] = simpleManagement.getNodeID();
         logger.info("node[{}]={}", i, nodes[i]);
      }

      validateTopology(uris, nodes, 0, 1, 2, 3);

      shutdown(process[1]);
      validateTopology(uris, nodes, 0, 2, 3);

      shutdown(process[3]);
      validateTopology(uris, nodes, 0, 2);

      process[1] = startServer(SERVER_NAMES[1], 1, 10_000);
      validateTopology(uris, nodes, 0, 1, 2);

      process[3] = startServer(SERVER_NAMES[3], 3, 10_000);
      validateTopology(uris, nodes, 0, 1, 2, 3);

      shutdown(process[0]);
      process[0] = startServer(SERVER_NAMES[0], 0, 10_000);
      validateTopology(uris, nodes, 0, 1, 2, 3);

      shutdown(process[0]);
      shutdown(process[1]);
      shutdown(process[2]);
      process[2] = startServer(SERVER_NAMES[2], 2, 10_000);
      validateTopology(uris, nodes, 2, 3);

      process[0] = startServer(SERVER_NAMES[0], 0, 10_000);
      process[1] = startServer(SERVER_NAMES[1], 1, 10_000);
      validateTopology(uris, nodes, 0, 1, 2, 3);

      shutdown(process[3]);
      process[3] = startServer(SERVER_NAMES[3], 0, 10_000);
      validateTopology(uris, nodes, 0, 1, 2, 3);
   }

   private void shutdown(Process process) {
      process.destroy();
      Wait.assertFalse(process::isAlive);
      removeProcess(process);
   }

   private void validateTopology(String[] uris, String[] nodeIDs, int... validNodes) throws Exception {
      for (int i : validNodes) {
         SimpleManagement simpleManagement = new SimpleManagement(uris[i], "admin", "adming");
         Wait.assertEquals(validNodes.length, () -> simpleManagement.listNetworkTopology().size(), 500, 5000);

         JsonArray topologyArray = simpleManagement.listNetworkTopology();
         Assert.assertNotNull(topologyArray);

         for (int j : validNodes) {
            JsonObject itemTopology = findTopologyNode(nodeIDs[j], topologyArray);
            Assert.assertNotNull(itemTopology);
            JsonString jsonString = (JsonString) itemTopology.get("live");
            Assert.assertEquals(uris[j], "tcp://" + jsonString.getString());
         }
      }

      ClusterVerifier clusterVerifier = new ClusterVerifier(uris[validNodes[0]], "admin", "admin");
      Assert.assertTrue(clusterVerifier.verify(new ActionContext()));
   }

   JsonObject findTopologyNode(String nodeID, JsonArray topologyArray) {
      for (int i = 0; i < topologyArray.size(); i++) {
         JsonObject object = topologyArray.getJsonObject(i);
         if (nodeID.equals(object.getString("nodeID"))) {
            return object;
         }
      }
      return null;
   }
}