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

import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.ONE_FIVE;
import static org.apache.activemq.artemis.tests.compatibility.GroovyRun.SNAPSHOT;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.tests.compatibility.base.ServerBase;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.utils.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class ReplyToTest extends ServerBase {

   @BeforeEach
   @Override
   public void setUp() throws Throwable {

      FileUtil.deleteDirectory(serverFolder);
      serverFolder.mkdirs();

      File file = File.createTempFile(ActiveMQJMSClient.class.getName() + ".properties", null, serverFolder);
      FileOutputStream fileOutputStream = new FileOutputStream(file);
      PrintStream stream = new PrintStream(fileOutputStream);
      stream.println("enable1xPrefixes=true");
      stream.close();

      setVariable(serverClassloader, "persistent", Boolean.FALSE);
      startServer(serverFolder, serverClassloader, "live");
   }

   @AfterEach
   @Override
   public void tearDown() throws Throwable {
      super.tearDown();

      localClassLoaders.forEach(cl -> {
         clearClassLoader(cl);
      });
      localClassLoaders.clear();
   }

   private static final ArrayList<ClassLoader> localClassLoaders = new ArrayList<>();

   @Override
   public ClassLoader getClasspath(String name) throws Exception {
      if (name.equals(SNAPSHOT)) {

         String snapshotPath = System.getProperty(SNAPSHOT);
         assumeTrue(snapshotPath != null);

         String path = serverFolder.getAbsolutePath() + File.pathSeparator + snapshotPath;

         ClassLoader loader = defineClassLoader(path);

         clearGroovy(loader);

         localClassLoaders.add(loader);

         return loader;
      } else {
         return super.getClasspath(name);
      }
   }

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

      combinations.add(new Object[]{SNAPSHOT, ONE_FIVE, ONE_FIVE});
      combinations.add(new Object[]{ONE_FIVE, SNAPSHOT, SNAPSHOT});

      combinations.add(new Object[]{ONE_FIVE, SNAPSHOT, ONE_FIVE});
      combinations.add(new Object[]{ONE_FIVE, ONE_FIVE, SNAPSHOT});

      return combinations;
   }

   public ReplyToTest(String server, String sender, String receiver) throws Exception {
      super(server, sender, receiver);
   }

   @TestTemplate
   public void testSendReceive() throws Throwable {

      setVariable(receiverClassloader, "latch", null);
      evaluate(senderClassloader, "ReplyToTest/replyToSend.groovy");
      evaluate(receiverClassloader, "ReplyToTest/replyToReceive.groovy");
   }

}

