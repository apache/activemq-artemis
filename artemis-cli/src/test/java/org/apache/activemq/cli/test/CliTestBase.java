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
package org.apache.activemq.cli.test;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.cli.commands.tools.LockAbstract;
import org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoader;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class CliTestBase {

   @Rule
   public TemporaryFolder temporaryFolder;

   @Rule
   public ThreadLeakCheckRule leakCheckRule = new ThreadLeakCheckRule();

   private String original = System.getProperty("java.security.auth.login.config");

   public CliTestBase() {
      File parent = new File("./target/tmp");
      parent.mkdirs();
      temporaryFolder = new TemporaryFolder(parent);
   }

   @Before
   public void setup() throws Exception {
      Run.setEmbedded(true);
      PropertiesLoader.resetUsersAndGroupsCache();
   }

   @After
   public void tearDown() throws Exception {
      ActiveMQClient.clearThreadPools();
      System.clearProperty("artemis.instance");
      System.clearProperty("artemis.instance.etc");
      Run.setEmbedded(false);

      if (original == null) {
         System.clearProperty("java.security.auth.login.config");
      } else {
         System.setProperty("java.security.auth.login.config", original);
      }

      LockAbstract.unlock();
   }

}
