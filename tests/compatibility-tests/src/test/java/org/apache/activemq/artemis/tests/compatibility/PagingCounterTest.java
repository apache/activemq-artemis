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

import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.SNAPSHOT;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.TWO_TWENTYTWO_ZERO;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.activemq.artemis.tests.compatibility.base.VersionedBase;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.utils.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class PagingCounterTest extends VersionedBase {

   // this will ensure that all tests in this class are run twice,
   // once with "true" passed to the class' constructor and once with "false"
   @Parameters(name = "server={0}, producer={1}, consumer={2}")
   public static Collection getParameters() {
      // we don't need every single version ever released..
      // if we keep testing current one against 2.4 and 1.4.. we are sure the wire and API won't change over time
      List<Object[]> combinations = new ArrayList<>();

      /*
      // during development sometimes is useful to comment out the combinations
      // and add the ones you are interested.. example:
       */
      //      combinations.add(new Object[]{SNAPSHOT, ONE_FIVE, ONE_FIVE});
      //      combinations.add(new Object[]{ONE_FIVE, ONE_FIVE, ONE_FIVE});

      combinations.add(new Object[]{null, TWO_TWENTYTWO_ZERO, SNAPSHOT});
      combinations.add(new Object[]{null, SNAPSHOT, TWO_TWENTYTWO_ZERO});
      // the purpose on this one is just to validate the test itself.
      /// if it can't run against itself it won't work at all
      combinations.add(new Object[]{null, SNAPSHOT, SNAPSHOT});
      return combinations;
   }

   public PagingCounterTest(String server, String sender, String receiver) throws Exception {
      super(server, sender, receiver);
   }

   @BeforeEach
   public void removeFolder() throws Throwable {
      FileUtil.deleteDirectory(serverFolder);
      serverFolder.mkdirs();
   }

   @AfterEach
   public void tearDown() {
      try {
         stopServer(serverClassloader);
      } catch (Throwable ignored) {
      }
      try {
         stopServer(receiverClassloader);
      } catch (Throwable ignored) {
      }
   }

   @TestTemplate
   public void testSendReceivePaging() throws Throwable {
      setVariable(senderClassloader, "persistent", true);
      startServer(serverFolder, senderClassloader, "pageCounter", null, true);
      evaluate(senderClassloader, "journalcompatibility/forcepaging.groovy");
      evaluate(senderClassloader, "pageCounter/sendMessages.groovy", server, "core", "1000");
      evaluate(senderClassloader, "journalcompatibility/ispaging.groovy");
      evaluate(senderClassloader, "pageCounter/checkMessages.groovy", "1000");
      stopServer(senderClassloader);

      setVariable(receiverClassloader, "persistent", true);
      startServer(serverFolder, receiverClassloader, "pageCounter", null, false);
      evaluate(receiverClassloader, "journalcompatibility/ispaging.groovy");
      evaluate(receiverClassloader, "pageCounter/checkMessages.groovy", "1000");

   }
}

