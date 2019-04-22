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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.activemq.artemis.tests.compatibility.base.ServerBase;
import org.apache.activemq.artemis.utils.FileUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.SNAPSHOT;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.TWO_FOUR;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.TWO_ONE;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.TWO_SEVEN_ZERO;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.TWO_SIX_THREE;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.TWO_ZERO;

@RunWith(Parameterized.class)
public class FQQNConsumerTest extends ServerBase {

   @Parameterized.Parameters(name = "server={0}, producer={1}, consumer={2}")
   public static Collection getParameters() {
      List<Object[]> combinations = new ArrayList<>();

      // FQQN was added into 2.7.0, hence we only test the server as SNAPSHOT or TWO_SEVEN_ZERO
      List testsList = combinatory(new Object[]{SNAPSHOT}, new Object[]{SNAPSHOT, TWO_ZERO, TWO_FOUR, TWO_ONE, TWO_SIX_THREE, TWO_SEVEN_ZERO}, new Object[]{SNAPSHOT, TWO_ZERO, TWO_FOUR, TWO_ONE, TWO_SIX_THREE, TWO_SEVEN_ZERO});
      addCombinations(testsList, null, new Object[] {TWO_SEVEN_ZERO}, new Object[]{SNAPSHOT, TWO_SEVEN_ZERO}, new Object[]{SNAPSHOT, TWO_SEVEN_ZERO});
      return testsList;
   }

   public FQQNConsumerTest(String server, String sender, String receiver) throws Exception {
      super(server, sender, receiver);
   }

   @Before
   @Override
   public void setUp() throws Throwable {
      FileUtil.deleteDirectory(serverFolder.getRoot());
      evaluate(serverClassloader, "fqqnconsumertest/artemisServer.groovy", serverFolder.getRoot().getAbsolutePath());
   }

   @After
   @Override
   public void tearDown() throws Throwable {
      execute(serverClassloader, "server.stop();");
   }

   @Test
   public void testSendReceive() throws Throwable {
      evaluate(senderClassloader,  "fqqnconsumertest/fqqnConsumerProducer.groovy", server, sender, "sendMessage");
      evaluate(receiverClassloader,  "fqqnconsumertest/fqqnConsumerProducer.groovy", server, receiver, "receiveMessage");
   }

}

