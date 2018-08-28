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

package org.apache.activemq.artemis.tests.compatibility;

import org.apache.activemq.artemis.tests.compatibility.base.VersionedBase;
import org.apache.activemq.artemis.utils.FileUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.HORNETQ_235;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.ONE_FIVE;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.SNAPSHOT;

/**
 * Runs Artemis server with HornetQ client and verifies that the client receives
 * correct connector parameters (keys must be dash-delimited instead of camelCase).
 */
@RunWith(Parameterized.class)
public class HQClientTopologyTest extends VersionedBase {

   @Parameterized.Parameters(name = "server={0}, producer={1}, consumer={2}")
   public static Collection getParameters() {
      List<Object[]> combinations = new ArrayList<>();

      combinations.add(new Object[]{SNAPSHOT, HORNETQ_235, HORNETQ_235});
      combinations.add(new Object[]{SNAPSHOT, ONE_FIVE, ONE_FIVE});
      return combinations;
   }

   public HQClientTopologyTest(String server, String sender, String receiver) throws Exception {
      super(server, sender, receiver);
   }

   @Before
   public void setUp() throws Throwable {
      FileUtil.deleteDirectory(serverFolder.getRoot());
      evaluate(serverClassloader, "hqclienttopologytest/artemisServer.groovy", serverFolder.getRoot().getAbsolutePath());
   }

   @After
   public void tearDown() throws Throwable {
      execute(serverClassloader, "server.stop();");
   }

   @Test
   public void topologyChangeMessageTest() throws Throwable {
      evaluate(senderClassloader, "hqclienttopologytest/verifyTopologyChangeMessage.groovy", server, sender);
   }
}
