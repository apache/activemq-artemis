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

import io.airlift.airline.ParseArgumentsUnexpectedException;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.InvalidOptionsError;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(value = Parameterized.class)
public class OptionsValidationTest extends CliTestBase {

   private File artemisInstance;

   private String group;
   private String command;
   private boolean needInstance;

   @Parameterized.Parameters(name = "group={0}, command={1}, need-instance={2}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{null, "create", false},
                                          {null, "run", true},
                                          {null, "kill", true},
                                          {null, "stop", true},
                                          {"address", "create", false},
                                          {"address", "delete", false},
                                          {"address", "update", false},
                                          {"address", "show", false},
                                          {null, "browser", false},
                                          {null, "consumer", false},
                                          {null, "mask", false},
                                          {null, "help", false},
                                          {null, "migrate1x", false},
                                          {null, "producer", false},
                                          {"queue", "create", false},
                                          {"queue", "delete", false},
                                          {"queue", "update", false},
                                          {"data", "print", false},
                                          {"data", "print", true},
                                          {"data", "exp", true},
                                          {"data", "imp", true},
                                          {"data", "encode", true},
                                          {"data", "decode", true},
                                          {"data", "compact", true},
                                          {"user", "add", true},
                                          {"user", "rm", true},
                                          {"user", "list", true},
                                          {"user", "reset", true}
      });
   }

   public OptionsValidationTest(String group, String command, boolean needInstance) {
      this.group = group;
      this.command = command;
      this.needInstance = needInstance;
   }

   @Before
   public void setUp() throws Exception {
      super.setup();
      this.artemisInstance = new File(temporaryFolder.getRoot() + "instance1");
   }

   @After
   @Override
   public void tearDown() throws Exception {
      super.tearDown();
   }

   @Test
   public void testCommand() throws Exception {
      ActionContext context = new TestActionContext();
      String[] invalidArgs = null;
      if (group == null) {
         invalidArgs = new String[] {command, "--blahblah-" + command, "--rubbish-" + command + "=" + "more-rubbish", "--input=blahblah"};
      } else {
         invalidArgs = new String[] {group, command, "--blahblah-" + command, "--rubbish-" + command + "=" + "more-rubbish", "--input=blahblah"};
      }
      try {
         Artemis.internalExecute(null, needInstance ? this.artemisInstance : null, invalidArgs, context);
         fail("cannot detect invalid options");
      } catch (InvalidOptionsError e) {
         assertTrue(e.getMessage().contains("Found unexpected parameters"));
      } catch (ParseArgumentsUnexpectedException e) {
         //airline can detect some invalid args during parsing
         //which is fine.
      }
   }
}
