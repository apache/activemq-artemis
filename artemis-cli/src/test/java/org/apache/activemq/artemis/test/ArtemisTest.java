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
package org.apache.activemq.artemis.test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.Run;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test to validate that the CLI doesn't throw improper exceptions when invoked.
 */
public class ArtemisTest
{
   @Rule
   public TemporaryFolder temporaryFolder;

   public ArtemisTest()
   {
      File parent = new File("./target/tmp");
      parent.mkdirs();
      temporaryFolder = new TemporaryFolder(parent);
   }


   @After
   public void cleanup()
   {
      System.clearProperty("artemis.instance");
      Run.setEmbedded(false);
   }

   @Test
   public void invalidCliDoesntThrowException()
   {
      testCli("create");
   }

   @Test
   public void invalidPathDoesntThrowException()
   {
      testCli("create","/rawr");
   }

   @Test
   public void testSimpleRun() throws Exception
   {
      Run.setEmbedded(true);
      Artemis.main("create", temporaryFolder.getRoot().getAbsolutePath(), "--force", "--silent-input", "--no-web");
      System.setProperty("artemis.instance", temporaryFolder.getRoot().getAbsolutePath());
      // Some exceptions may happen on the initialization, but they should be ok on start the basic core protocol
      Artemis.main("run");
      Artemis.main("produce", "--txSize", "500");
      Artemis.main("consume", "--txSize", "500", "--verbose");
      Artemis.main("stop");
      Artemis.main("data", "print");
      Assert.assertTrue(Run.latchRunning.await(5, TimeUnit.SECONDS));

   }

   private void testCli(String... args)
   {
      try
      {
         Artemis.main(args);
      }
      catch (Exception e)
      {
         e.printStackTrace();
         Assert.fail("Exception caught " + e.getMessage());
      }
   }
}
