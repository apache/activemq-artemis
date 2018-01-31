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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.HORNETQ_247;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.ONE_FIVE;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.SNAPSHOT;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.TWO_FOUR;

@RunWith(Parameterized.class)
public class PrefixSendAckTest extends ServerBaseTest {
   @Parameterized.Parameters(name = "server={0}, producer={1}, consumer={2}")
   public static Collection getParameters() {
      List<Object[]> combinations = new ArrayList<>();

      combinations.addAll(combinatory(new Object[]{SNAPSHOT}, new Object[]{ONE_FIVE, HORNETQ_247, TWO_FOUR, SNAPSHOT}, new Object[]{ONE_FIVE, HORNETQ_247, TWO_FOUR, SNAPSHOT}));
      return combinations;
   }

   public PrefixSendAckTest(String server, String sender, String receiver) throws Exception {
      super(server, sender, receiver);
   }

   @Test
   public void testSendReceive() throws Throwable {
      evaluate(senderClassloader,  "prefixSendAckTest/sendAckMessages.groovy", server, sender, "sendAckMessages");
      evaluate(receiverClassloader,  "prefixSendAckTest/sendAckMessages.groovy", server, receiver, "receiveMessages");
   }

   @Override
   public String getServerScriptToUse() {
      return "prefixSendAckTest/artemisServer.groovy";
   }

}

