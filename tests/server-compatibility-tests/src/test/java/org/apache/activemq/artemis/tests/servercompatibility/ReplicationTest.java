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

package org.apache.activemq.artemis.tests.servercompatibility;

import org.apache.activemq.artemis.tests.servercompatibility.base.ScriptedCompatibilityTest;
import org.jboss.logging.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

import static org.apache.activemq.artemis.tests.servercompatibility.base.VersionTags.ARTEMIS_2_17_0;
import static org.apache.activemq.artemis.tests.servercompatibility.base.VersionTags.ARTEMIS_2_18_0;
import static org.apache.activemq.artemis.tests.servercompatibility.base.VersionTags.ARTEMIS_SNAPSHOT;

@RunWith(Parameterized.class)
public class ReplicationTest extends ScriptedCompatibilityTest {

   private static final int MASTER = FIRST;
   private static final int SLAVE = SECOND;
   private static final Logger log = Logger.getLogger(ReplicationTest.class);


   @Parameterized.Parameters(name = "master={0}, slave={1}")
   public static Collection<?> getParameters() {
      return List.of(

            // ARTEMIS-3767
            // new Object[]{ARTEMIS_2_17_0, ARTEMIS_2_18_0},
            // new Object[]{ARTEMIS_2_18_0, ARTEMIS_2_17_0},

            // Current
            new Object[]{ARTEMIS_2_17_0, ARTEMIS_SNAPSHOT},
            new Object[]{ARTEMIS_SNAPSHOT, ARTEMIS_2_17_0},

            // These below should work OK
            new Object[]{ARTEMIS_2_17_0, ARTEMIS_2_17_0},
            new Object[]{ARTEMIS_2_18_0, ARTEMIS_2_18_0},
            new Object[]{ARTEMIS_SNAPSHOT, ARTEMIS_SNAPSHOT}

      );
   }

   public ReplicationTest(String master, String slave) {
      super(master, slave);
   }

   @Test
   public void testReplication() throws Throwable {
      try {
         executeScript(MASTER, "ReplicationTest/createMaster.groovy");
         executeScript(SLAVE, "ReplicationTest/createSlave.groovy");
         executeScript(MASTER, "ReplicationTest/testSendMessageToMaster.groovy");
         executeScript(MASTER, "servers/stopServer.groovy");
         executeScript(SLAVE, "ReplicationTest/testReceiveMessageFromSlave.groovy");
         executeScript(SLAVE, "servers/stopServer.groovy");
      } catch (Throwable t) {
         try {
            executeScript(MASTER, "servers/stopServer.groovy");
         } catch (Throwable t1) {
            log.error("Failed to stop master server.", t1);
         }
         try {
            executeScript(SLAVE, "servers/stopServer.groovy");
         } catch (Throwable t2) {
            log.error("Failed to stop slave server.", t2);
         }
         throw t;
      }
   }

}
