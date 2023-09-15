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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.Create;
import org.apache.activemq.artemis.cli.commands.Upgrade;
import org.apache.activemq.artemis.utils.FileUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This test will compare expected values at the upgraded servers. */
public class CompareUpgradeTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String basedir = System.getProperty("basedir");


   @Test
   public void testCompareWindowsFreshInstance() throws Exception {
      String windows = basedir + "/target/classes/servers/windowsUpgrade";
      String windowsBin = windows + "/bin";
      String windowsETC = basedir + "/target/classes/servers/windowsUpgradeETC";

      String windowsExpected = basedir + "/target/classes/servers/windowsUpgradeExpected";
      String windowsExpectedBin = windowsExpected + "/bin";
      String windowsExpectedETC = basedir + "/target/classes/servers/windowsUpgradeETCExpected";

      compareDirectories(true, windowsExpectedBin, windowsBin);
      compareDirectories(true, windowsExpectedETC, windowsETC, "broker.xml", "artemis-users.properties", "management.xml");

      String referenceBin = basedir + "/target/reference-for-backup-check/servers/windowsUpgrade/bin";
      String referenceEtc = basedir + "/target/reference-for-backup-check/servers/windowsUpgradeETC";

      verifyBackupFiles(windows + "/old-config-bkp.0/bin", referenceBin, Create.ARTEMIS_CMD, Create.ARTEMIS_SERVICE_EXE, Create.ARTEMIS_SERVICE_EXE_CONFIG, Create.ARTEMIS_SERVICE_XML);
      verifyBackupFiles(windows + "/old-config-bkp.0/etc", referenceEtc, Create.ETC_ARTEMIS_PROFILE_CMD, Create.ETC_BOOTSTRAP_XML, Upgrade.OLD_LOGGING_PROPERTIES);
   }

   @Test
   public void testCompareLinuxFreshInstance() throws Exception {
      String linux = basedir + "/target/classes/servers/linuxUpgrade";
      String linuxBin = linux + "/bin";
      String linuxETC = basedir + "/target/classes/servers/linuxUpgradeETC";

      String linuxExpected = basedir + "/target/classes/servers/linuxUpgradeExpected";
      String linuxExpectedBin = linuxExpected + "/bin";
      String linuxExpectedETC = basedir + "/target/classes/servers/linuxUpgradeETCExpected";

      compareDirectories(true, linuxExpectedBin, linuxBin);
      compareDirectories(true, linuxExpectedETC, linuxETC, "broker.xml", "artemis-users.properties", "management.xml");

      String referenceBin = basedir + "/target/reference-for-backup-check/servers/linuxUpgrade/bin";
      String referenceEtc = basedir + "/target/reference-for-backup-check/servers/linuxUpgradeETC";

      verifyBackupFiles(linux + "/old-config-bkp.0/bin", referenceBin, Create.ARTEMIS, Create.ARTEMIS_SERVICE);
      verifyBackupFiles(linux + "/old-config-bkp.0/etc", referenceEtc, Create.ETC_ARTEMIS_PROFILE, Create.ETC_BOOTSTRAP_XML, Upgrade.OLD_LOGGING_PROPERTIES);
   }

   private void verifyBackupFiles(String backupFolder, String referenceFolder, String... files) throws Exception {
      assertTrue("Files to check must be specified", files.length > 0);

      File bck = new File(backupFolder);
      if (!(bck.exists() && bck.isDirectory())) {
         Assert.fail("Backup folder does not exist at: " + bck.getAbsolutePath());
      }

      File[] backupFiles = bck.listFiles();
      assertNotNull("Some backup files must exist", backupFiles);
      int backupFilesCount = backupFiles.length;
      assertTrue("Some backup files must exist", backupFilesCount > 0);
      assertEquals("Different number of backup files found than specified for inspection, update test if backup procedure changed", files.length, backupFilesCount);

      for (String f : files) {
         File bf = new File(backupFolder, f);
         if (!bf.exists()) {
            Assert.fail("Expected backup file does not exist at: " + bf.getAbsolutePath());
         }

         File reference = new File(referenceFolder, bf.getName());
         if (!reference.exists()) {
            Assert.fail("Reference file does not exist at: " + reference.getAbsolutePath());
         }

         Assert.assertArrayEquals(bf.getName() + " backup contents do not match reference file", Files.readAllBytes(bf.toPath()), Files.readAllBytes(reference.toPath()));
      }
   }

   private void compareDirectories(boolean allowExpectedWord, String expectedFolder, String upgradeFolder, String... ignoredFiles) throws Exception {
      File expectedFolderFile = new File(expectedFolder);
      File[] foundFiles = expectedFolderFile.listFiles(pathname -> {
         for (String i :ignoredFiles) {
            if (pathname.getName().contains(i)) {
               return false;
            }
         }
         return true;
      });


      File upgradeFolderFile = new File(upgradeFolder);

      for (File f : foundFiles) {
         File upgradeFile = new File(upgradeFolderFile, f.getName());
         if (!upgradeFile.exists()) {
            Assert.fail(upgradeFile.getAbsolutePath() + " not does exist");
         }


         if (f.getName().endsWith(".exe")) {
            Assert.assertArrayEquals(f.getName() + " is different after upgrade", Files.readAllBytes(f.toPath()), Files.readAllBytes(upgradeFile.toPath()));
         } else {
            compareFiles(allowExpectedWord, f, upgradeFile);
         }
      }
   }

   private static void compareFiles(boolean allowExpectedWord, File expectedFile, File upgradeFile) throws IOException {
      logger.debug("comparing: {} to {}", upgradeFile, upgradeFile);
      try (Stream<String> expectedStream = Files.lines(expectedFile.toPath());
           Stream<String> upgradeStream = Files.lines(upgradeFile.toPath())) {

         Iterator<String> expectedIterator = expectedStream.iterator();
         Iterator<String> upgradeIterator = upgradeStream.iterator();

         int line = 1;

         while (expectedIterator.hasNext()) {
            Assert.assertTrue(upgradeIterator.hasNext());

            String expectedString = expectedIterator.next().trim();
            String upgradeString = upgradeIterator.next().trim();

            // there's a test in this class that will use a different name ID. on that case we replace Expected by ""
            // on the comparison
            if (allowExpectedWord) {
               expectedString = expectedString.replace("Expected", "");
            }

            Assert.assertEquals("error on line " + line + " at " + upgradeFile, expectedString, upgradeString);
            line++;
         }

         Assert.assertFalse(upgradeIterator.hasNext());
      }
   }

   @Test
   public void testWindows() throws Exception {
      String windows = basedir + "/target/classes/servers/windowsUpgrade";
      String windowsBin = windows + "/bin";
      String windowsETC = basedir + "/target/classes/servers/windowsUpgradeETC";

      checkExpectedValues(windowsBin + "/artemis.cmd", "set ARTEMIS_INSTANCE_ETC=", "\"" + windowsETC + "\"");
      Map<String, String> result = checkExpectedValues(windowsBin + "/artemis-service.xml",
                                                       "<env name=\"ARTEMIS_HOME\" value=", null, // no expected value for this, we will check on the output
                                                       "<env name=\"ARTEMIS_INSTANCE\" value=", "\"" + windows  + "\"/>",
                                                       "<env name=\"ARTEMIS_INSTANCE_ETC\" value=", "\"" + windowsETC + "\"/>",
                                                       "<env name=\"ARTEMIS_INSTANCE_URI\" value=", "\"file:" + windows + "/\"/>",
                                                       "<env name=\"ARTEMIS_DATA_DIR\" value=", "\"" + windows + "/data\"/>"
      );

      String home = result.get("<env name=\"ARTEMIS_HOME\" value=");
      Assert.assertNotNull(home);
      Assert.assertFalse("home value must be changed during upgrade", home.contains("must-change"));

      result = checkExpectedValues(windowsETC + "/artemis.profile.cmd",
                                   "set ARTEMIS_HOME=", null, // no expected value for this, we will check on the output
                                   "set ARTEMIS_INSTANCE=", "\"" + windows + "\"",
                                   "set ARTEMIS_DATA_DIR=","\"" + windows + "/data\"",
                                   "set ARTEMIS_ETC_DIR=", "\"" + windowsETC + "\"",
                                   "set ARTEMIS_OOME_DUMP=", "\"" + windows + "/log/oom_dump.hprof\"",
                                   "set ARTEMIS_INSTANCE_URI=", "\"file:" + windows + "/\"",
                                   "set ARTEMIS_INSTANCE_ETC_URI=", "\"file:" + windowsETC + "/\"");

      home = result.get("set ARTEMIS_HOME=");
      Assert.assertNotNull(home);
      Assert.assertFalse("home value must be changed during upgrade", home.contains("must-change"));

      checkExpectedValues(windowsETC + "/bootstrap.xml",
                          "<server configuration=", "\"file:" + windowsETC + "//broker.xml\"/>");

      File oldLogging = new File(windowsETC + "/logging.properties");
      File newLogging = new File(windowsETC + "/log4j2.properties");

      Assert.assertFalse("Old logging must be removed by upgrade", oldLogging.exists());
      Assert.assertTrue("New Logging must be installed by upgrade", newLogging.exists());
   }


   @Test
   public void testLinux() throws Exception {

      String instanceDir = basedir + "/target/classes/servers/linuxUpgrade";
      String bin = instanceDir + "/bin";
      String etc = basedir + "/target/classes/servers/linuxUpgradeETC";

      checkExpectedValues(bin + "/artemis", "ARTEMIS_INSTANCE_ETC=", "'" + etc + "'");

      Map<String, String> result = checkExpectedValues(etc + "/artemis.profile",
                                                       "ARTEMIS_HOME=", null, // no expected value, will check on result
                                                       "ARTEMIS_INSTANCE=", "'" + instanceDir + "'",
                                                       "ARTEMIS_DATA_DIR=", "'" + instanceDir + "/data'",
                                                       "ARTEMIS_ETC_DIR=", "'" + etc + "'",
                                                       "ARTEMIS_OOME_DUMP=", "'" + instanceDir + "/log/oom_dump.hprof'",
                                                       "ARTEMIS_INSTANCE_URI=", "'file:" + instanceDir + "/'",
                                                       "ARTEMIS_INSTANCE_ETC_URI=", "'file:" + etc + "/'");

      String home = result.get("ARTEMIS_HOME=");
      Assert.assertNotNull(home);
      Assert.assertNotEquals("'must-change'", home);

      File oldLogging = new File(etc + "/logging.properties");
      File newLogging = new File(etc + "/log4j2.properties");

      Assert.assertFalse("Old logging must be removed by upgrade", oldLogging.exists());
      Assert.assertTrue("New Logging must be installed by upgrade", newLogging.exists());
   }

   @Test
   public void testCompareUpgradeCurrentVersion() throws Throwable {
      File upgradeConfig = new File(basedir + "/target/upgrade/currentVersion");
      File originalConfig = new File(basedir + "/target/upgrade/currentVersionCopy");
      FileUtil.deleteDirectory(originalConfig); // removing eventual previous runs

      // for previous runs
      removeBackups(upgradeConfig);

      // I'm keeping the current configuration as originalConfig, to make a comparisson after upgrade is called
      FileUtil.copyDirectory(upgradeConfig, originalConfig);

      // looking up for the ARTEMIS_HOME from the profile file
      Properties properties = new Properties();
      properties.load(new FileInputStream(new File(originalConfig, "/etc/artemis.profile")));
      File home = new File(parseProperty(properties, "ARTEMIS_HOME"));

      Upgrade upgrade = new Upgrade();
      upgrade.setHomeValues(home, null, null);
      upgrade.setDirectory(upgradeConfig);
      upgrade.run(new ActionContext());

      // for current run
      removeBackups(upgradeConfig);

      // Calling upgrade on itself should not cause any changes to *any* file
      // output should be exactly the same
      Assert.assertTrue(compareDirectories(false, originalConfig.toPath(), upgradeConfig.toPath()));
   }

   private void removeBackups(File upgradeConfig) {
      File[] bkpFiles = upgradeConfig.listFiles(f -> f.isDirectory() && f.getName().contains("config-bkp"));
      for (File f : bkpFiles) {
         logger.debug("Removing {}", f);
         FileUtil.deleteDirectory(f);
      }
   }

   private static String parseProperty(Properties properties, String name) {
      String property = properties.getProperty(name);
      if (property == null) {
         return null;
      }
      // the property value might have quotes needed for bash. We need to remove those here
      if (property.startsWith("'") && property.endsWith("'")) {
         property = property.substring(1, property.length() - 1);
      }
      return property;
   }


   public static boolean compareDirectories(boolean allowExpectedWord, Path expected, Path upgrade) throws IOException {
      Files.walkFileTree(expected, new SimpleFileVisitor<Path>() {
         @Override
         public FileVisitResult visitFile(Path expectedFile, BasicFileAttributes attrs) throws IOException {
            FileVisitResult result = super.visitFile(expectedFile, attrs);

            Path relativize = expected.relativize(expectedFile);
            Path upgradeFile = upgrade.resolve(relativize);

            compareFiles(allowExpectedWord, expectedFile.toFile(), upgradeFile.toFile());

            return result;
         }
      });
      return true;
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
      try (Stream<String> lines = Files.lines(file.toPath())) {
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
                     if (logger.isDebugEnabled()) {
                        logger.debug("prefix={}, expecting={}, actualValue={}", key, value, actualValue);
                     }
                     Assert.assertEquals(key + " did not match", value, actualValue);
                  }
               }
            });
         });
      }

      Assert.assertEquals("Some elements were not found in the output of " + fileName, matchingValues.size(), expectedValues.size());

      return matchingValues;
   }
}
