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

import org.apache.activemq.artemis.utils.FileUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.ONE_FIVE;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.SNAPSHOT;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.TWO_FOUR;

/**
 * To run this test on the IDE and debug it, run the compatibility-tests through a command line once:
 *
 * cd /compatibility-tests
 * mvn install -Ptests | tee output.log
 *
 * on the output.log you will see the output generated by {@link #getClasspathProperty(String)}
 *
 * On your IDE, edit the Run Configuration to your test and add those -D as parameters to your test.
 * On Idea you would do the following:
 *
 * Run->Edit Configuration->Add ArtemisMeshTest and add your properties.
 */
@RunWith(Parameterized.class)
public class ExportImportTest extends VersionedBaseTest {

   // this will ensure that all tests in this class are run twice,
   // once with "true" passed to the class' constructor and once with "false"
   @Parameterized.Parameters(name = "server={0}, producer={1}, consumer={2}")
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

      combinations.addAll(combinatory(new Object[]{null}, new Object[]{ONE_FIVE, TWO_FOUR, SNAPSHOT}, new Object[]{ONE_FIVE, TWO_FOUR, SNAPSHOT}));
      return combinations;
   }

   public ExportImportTest(String server, String sender, String receiver) throws Exception {
      super(server, sender, receiver);
   }

   @Before
   public void removeFolder() throws Throwable {
      FileUtil.deleteDirectory(serverFolder.getRoot());
      serverFolder.getRoot().mkdirs();
   }

   @After
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

   @Test
   @Ignore // There's some work to be done on exporter / importer, but I wanted to send it in already
   public void testSendReceive() throws Throwable {
      setVariable(senderClassloader, "persistent", Boolean.TRUE);
      startServer(serverFolder.getRoot(), senderClassloader, "sender");
      callScript(senderClassloader, "meshTest/sendMessages.groovy", server, sender, "sendAckMessages");
      stopServer(senderClassloader);

      if (sender.startsWith("ARTEMIS-1")) {
         callScript(senderClassloader, "exportimport/export1X.groovy", serverFolder.getRoot().getAbsolutePath());
      } else {
         callScript(senderClassloader, "exportimport/export.groovy", serverFolder.getRoot().getAbsolutePath());
      }

      setVariable(receiverClassloader, "persistent", Boolean.TRUE);
      startServer(serverFolder.getRoot(), receiverClassloader, "receiver");

      if (receiver.startsWith("ARTEMIS-1")) {
         callScript(receiverClassloader, "exportimport/import1X.groovy", serverFolder.getRoot().getAbsolutePath());
      } else {
         callScript(receiverClassloader, "exportimport/import.groovy", serverFolder.getRoot().getAbsolutePath());
      }

      setVariable(receiverClassloader, "latch", null);
      callScript(receiverClassloader, "meshTest/sendMessages.groovy", server, receiver, "receiveMessages");
   }

}

