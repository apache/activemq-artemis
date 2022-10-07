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

package org.apache.activemq.artemis.tests.smoke.upgradeTest;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This test will compare expected values at the upgraded servers. */
public class CompareUpgradeTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String basedir = System.getProperty("basedir");

   @Test
   public void testWindows() throws Exception {
      String windowsBin = basedir + "/target/classes/servers/windows/bin";
      String windowsETC = basedir + "/target/classes/servers/windowsETC";

      checkExpectedValues(windowsBin + "/artemis.cmd", "set ARTEMIS_INSTANCE_ETC=", "\"" + windowsETC + "\"");
      Map<String, String> result = checkExpectedValues(windowsBin + "/artemis-service.xml",
                                                       "<env name=\"ARTEMIS_HOME\" value=", null, // no expected value for this, we will check on the output
                                                       "<env name=\"ARTEMIS_INSTANCE\" value=", "\"no-change\"/>",
                                                       "<env name=\"ARTEMIS_INSTANCE_ETC\" value=", "\"" + windowsETC + "\"/>",
                                                       "<env name=\"ARTEMIS_INSTANCE_URI\" value=", "\"file:/no-change\"/>",
                                                       "<env name=\"ARTEMIS_DATA_DIR\" value=", "\"no-change\"/>"
      );

      String home = result.get("<env name=\"ARTEMIS_HOME\" value=");
      Assert.assertNotNull(home);
      Assert.assertFalse("home value must be changed during upgrade", home.contains("must-change"));

      result = checkExpectedValues(windowsETC + "/artemis.profile.cmd",
                                   "set ARTEMIS_HOME=", null, // no expected value for this, we will check on the output
                                   "set ARTEMIS_INSTANCE=", "\"no-change\"",
                                   "set ARTEMIS_DATA_DIR=", "\"no-change\"",
                                   "set ARTEMIS_ETC_DIR=", "\"no-change\"",
                                   "set ARTEMIS_OOME_DUMP=", "\"no-change\"",
                                   "set ARTEMIS_INSTANCE_URI=", "\"file:/no-change/\"",
                                   "set ARTEMIS_INSTANCE_ETC_URI=", "\"file:/no-change/\"");

      home = result.get("set ARTEMIS_HOME=");
      Assert.assertNotNull(home);
      Assert.assertFalse("home value must be changed during upgrade", home.contains("must-change"));

      checkExpectedValues(windowsETC + "/bootstrap.xml",
                          "<server configuration=", "\"file:/no-change/broker.xml\"/>");

      File oldLogging = new File(windowsETC + "/logging.properties");
      File newLogging = new File(windowsETC + "/log4j2.properties");

      Assert.assertFalse("Old logging must be removed by upgrade", oldLogging.exists());
      Assert.assertTrue("New Logging must be installed by upgrade", newLogging.exists());
   }


   @Test
   public void testLinux() throws Exception {

      String instanceDir = basedir + "/target/classes/servers/toUpgradeTest";
      String bin = instanceDir + "/bin";
      String etc = basedir + "/target/classes/servers/toUpgradeETC";

      checkExpectedValues(bin + "/artemis", "ARTEMIS_INSTANCE_ETC=", "'" + etc + "'");

      Map<String, String> result = checkExpectedValues(etc + "/artemis.profile",
                                                       "ARTEMIS_HOME=", null, // no expected value, will check on result
                                                       "ARTEMIS_INSTANCE=", "'" + instanceDir + "'",
                                                       "ARTEMIS_DATA_DIR=", "'" + instanceDir + "/data'",
                                                       "ARTEMIS_ETC_DIR=", "'" + etc + "'",
                                                       "ARTEMIS_OOME_DUMP=", "'" + instanceDir + "/log/oom_dump.hprof'",
                                                       "ARTEMIS_INSTANCE_URI=", "'file:" + instanceDir + "'",
                                                       "ARTEMIS_INSTANCE_ETC_URI=", "'file:" + etc + "'");

      String home = result.get("ARTEMIS_HOME=");
      Assert.assertNotNull(home);
      Assert.assertNotEquals("'must-change'", home);

      File oldLogging = new File(etc + "/logging.properties");
      File newLogging = new File(etc + "/log4j2.properties");

      Assert.assertFalse("Old logging must be removed by upgrade", oldLogging.exists());
      Assert.assertTrue("New Logging must be installed by upgrade", newLogging.exists());
   }


   private Map<String, String> checkExpectedValues(String fileName, String... expectedPairs) throws Exception {
      Assert.assertTrue("You must pass a pair of expected values", expectedPairs.length > 0 && expectedPairs.length % 2 == 0);
      HashMap<String, String> expectedValues = new HashMap<>();
      HashMap<String, String> matchingValues = new HashMap<>();

      for (int i = 0; i < expectedPairs.length; i += 2) {
         Assert.assertFalse("Value duplicated on pairs ::" + expectedPairs[i], expectedValues.containsKey(expectedPairs[i]));
         expectedValues.put(expectedPairs[i], expectedPairs[i + 1]);
      }

      File file = new File(fileName);
      Stream<String> lines = Files.lines(file.toPath());
      lines.forEach(line -> {
         String trimmedLine = line.trim();
         expectedValues.forEach((key, value) -> {
            if (trimmedLine.startsWith(key)) {
               String actualValue = trimmedLine.substring(key.length());
               logger.debug("match = {}", line);
               matchingValues.put(key, actualValue);

               if (value == null) {
                  logger.debug("no expected value was defined for {}, we will just fill out the matchingValues for further evaluation", key);
               } else {
                  logger.debug("prefix={}, expecting={}, actualValue={}", key, value, actualValue);
                  Assert.assertEquals(key + " did not match", value, actualValue);
               }
            }
         });
      });

      Assert.assertEquals("Some elements were not found in the output of " + fileName, matchingValues.size(), expectedValues.size());

      return matchingValues;
   }
}
