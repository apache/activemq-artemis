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
package org.apache.activemq.artemis.cli.commands;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import io.airlift.airline.Command;

@Command(name = "upgrade", description = "Update an artemis instance to the current artemis.home, keeping all the data and broker.xml. Warning: backup your instance before using this command and compare the files.")
public class Upgrade extends InstallAbstract {

   protected static final String OLD_LOGGING_PROPERTIES = "logging.properties";

   /**
    * Checks that the directory provided either exists and is writable or doesn't exist but can be created.
    */
   protected void checkDirectory() {
      if (!directory.exists()) {
         throw new RuntimeException(String.format("Could not find path '%s' to upgrade.", directory));
      } else if (!directory.canWrite()) {
         throw new RuntimeException(String.format("The path '%s' is not writable.", directory));
      }
   }


   @Override
   public Object execute(ActionContext context) throws Exception {
      this.checkDirectory();
      super.execute(context);

      return run(context);
   }

   @Override
   public Object run(ActionContext context) throws Exception {
      super.run(context);
      context.out.println("*******************************************************************************************************************************");
      context.out.println("Upgrading broker instance " + directory + " to use artemis.home=" + getBrokerHome());

      File bkpFolder = findBackup(context);
      File binBkp = new File(bkpFolder, "bin");
      File etcBkp = new File(bkpFolder, "etc");
      File tmp = new File(bkpFolder, "tmp");
      binBkp.mkdirs();
      etcBkp.mkdirs();
      tmp.mkdirs();

      File bin = new File(directory, "bin");
      File etcFolder = new File(directory, etc);

      if (etc == null || etc.equals("etc")) {
         if (IS_WINDOWS && !IS_CYGWIN) {
            File cmd = new File(bin, Create.ARTEMIS_CMD);
            String pattern = "set ARTEMIS_INSTANCE_ETC=";
            etcFolder = getETC(context, etcFolder, cmd, pattern);
         } else {
            File cmd = new File(bin, Create.ARTEMIS);
            String pattern = "ARTEMIS_INSTANCE_ETC=";
            etcFolder = getETC(context, etcFolder, cmd, pattern);
         }
      }

      if (bin == null || !bin.exists()) { // it can't be null, just being cautious
         throw new IOException(bin + " does not exist for binary");
      }

      if (etcFolder == null || !etcFolder.exists()) { // it can't be null, just being cautious
         throw new IOException(etcFolder + " does not exist for etc");
      }

      HashMap<String, String> filters = new HashMap<>();
      Create.addScriptFilters(filters, getHome(), getInstance(), etcFolder, new File(getInstance(), "notUsed"), new File(getInstance(), "om-not-used.dmp"), javaMemory, javaOptions, "NA");

      if (IS_WINDOWS) {
         write(Create.BIN_ARTEMIS_CMD, new File(tmp, Create.ARTEMIS_CMD), filters, false, false);
         upgrade(new File(tmp, Create.ARTEMIS_CMD), new File(bin, Create.ARTEMIS_CMD), binBkp, "set ARTEMIS_INSTANCE_ETC=");

         write(Create.BIN_ARTEMIS_SERVICE_XML, new File(tmp, Create.ARTEMIS_SERVICE_XML), filters, false, false);
         upgrade(new File(tmp, Create.ARTEMIS_SERVICE_XML), new File(bin, Create.ARTEMIS_SERVICE_XML), binBkp,
                 "<env name=\"ARTEMIS_INSTANCE\"", "<env name=\"ARTEMIS_INSTANCE_ETC\"",
                 "<env name=\"ARTEMIS_INSTANCE_URI\"", "<env name=\"ARTEMIS_INSTANCE_ETC_URI\"",
                 "<env name=\"ARTEMIS_DATA_DIR\"", "<logpath>", "<startargument>-Xmx", "<stopargument>-Xmx");

         write("etc/" + Create.ETC_ARTEMIS_PROFILE_CMD, new File(tmp, Create.ETC_ARTEMIS_PROFILE_CMD), filters, false, false);
         upgrade(new File(tmp, Create.ETC_ARTEMIS_PROFILE_CMD), new File(etcFolder, Create.ETC_ARTEMIS_PROFILE_CMD), binBkp,
                 "set ARTEMIS_INSTANCE=\"", "set ARTEMIS_DATA_DIR=", "set ARTEMIS_ETC_DIR=", "set ARTEMIS_OOME_DUMP=", "set ARTEMIS_INSTANCE_URI=", "set ARTEMIS_INSTANCE_ETC_URI=");
      }

      if (!IS_WINDOWS || IS_CYGWIN) {
         write(Create.BIN_ARTEMIS, new File(tmp, Create.ARTEMIS), filters, false, false);
         upgrade(new File(tmp, Create.ARTEMIS), new File(bin, Create.ARTEMIS), binBkp, "ARTEMIS_INSTANCE_ETC=");

         write(Create.BIN_ARTEMIS_SERVICE, new File(tmp, Create.ARTEMIS_SERVICE), filters, false, false);
         upgrade(new File(tmp, Create.ARTEMIS_SERVICE), new File(bin, Create.ARTEMIS_SERVICE), binBkp); // we replace the whole thing

         write("etc/" + Create.ETC_ARTEMIS_PROFILE, new File(tmp, Create.ETC_ARTEMIS_PROFILE), filters, false, false);
         upgrade(new File(tmp, Create.ETC_ARTEMIS_PROFILE), new File(etcFolder, Create.ETC_ARTEMIS_PROFILE), etcBkp, "ARTEMIS_INSTANCE=",
                 "ARTEMIS_DATA_DIR=", "ARTEMIS_ETC_DIR=", "ARTEMIS_OOME_DUMP=", "ARTEMIS_INSTANCE_URI=", "ARTEMIS_INSTANCE_ETC_URI=", "HAWTIO_ROLE=");
      }

      upgradeLogging(context, etcBkp, etcFolder);

      context.out.println();
      context.out.println("*******************************************************************************************************************************");

      return null;
   }

   private File getETC(ActionContext context, File etcFolder, File cmd, String pattern) throws IOException {
      String etcLine = getLine(cmd, pattern);
      if (etcLine != null) {
         etcLine = etcLine.trim();
         etcLine = etcLine.substring(pattern.length() + 1, etcLine.length() - 1);
         etcFolder = new File(etcLine);
         context.out.println("ETC found at " + etcFolder);
      }
      return etcFolder;
   }

   private String getLine(File cmd, String pattern) throws IOException {
      try (Stream<String> lines = Files.lines(cmd.toPath())) {
         Iterator<String> iterator = lines.iterator();
         while (iterator.hasNext()) {
            String line = iterator.next();

            if (line.trim().startsWith(pattern)) {
               return line;
            }
         }
      }

      return null;
   }

   private void upgrade(File tmpFile, File targetFile, File bkp, String... keepingPrefixes) throws Exception {
      HashMap<String, String> replaceMatrix = new HashMap<>();

      doUpgrade(tmpFile, targetFile, bkp,
              oldLine -> {
                 for (String prefix : keepingPrefixes) {
                    if (oldLine.trim().startsWith(prefix)) {
                       replaceMatrix.put(prefix, oldLine);
                    }
                 }
              },
            newLine -> {
               for (String prefix : keepingPrefixes) {
                  if (newLine.trim().startsWith(prefix)) {
                     String originalLine = replaceMatrix.get(prefix);
                     return originalLine;
                  }
               }
               return newLine;
            });
   }

   private void doUpgrade(File tmpFile, File targetFile, File bkp, Consumer<String> originalConsumer, Function<String, String> targetFunction) throws Exception {
      Files.copy(targetFile.toPath(), bkp.toPath(), StandardCopyOption.REPLACE_EXISTING);

      // we first scan the original lines on the originalConsumer, giving a chance to the caller to fill out the original matrix
      if (originalConsumer != null) {
         try (Stream<String> originalLines = Files.lines(targetFile.toPath())) {
            originalLines.forEach(line -> {
               originalConsumer.accept(line);
            });
         }
      }

      // now we open the new file from the tmp, and we will give a chance for the targetFunction to replace lines from a matrix
      try (Stream<String> lines = Files.lines(tmpFile.toPath());
           PrintStream streamOutput = new PrintStream(new FileOutputStream(targetFile))) {

         Iterator<String> linesIterator = lines.iterator();
         while (linesIterator.hasNext()) {
            String line = linesIterator.next();
            line = targetFunction.apply(line);
            if (line != null) {
               streamOutput.println(line);
            }
         }
      }
   }

   private void upgradeLogging(ActionContext context, File bkpFolder, File etc) throws Exception {
      File oldLogging = new File(etc, OLD_LOGGING_PROPERTIES);

      if (oldLogging.exists()) {
         File oldLoggingCopy = new File(bkpFolder, OLD_LOGGING_PROPERTIES);
         context.out.println("Copying " + oldLogging.toPath() + " to " + oldLoggingCopy.toPath());

         Files.copy(oldLogging.toPath(), bkpFolder.toPath(), StandardCopyOption.REPLACE_EXISTING);

         context.out.println("Removing " + oldLogging.toPath());
         if (!oldLogging.delete()) {
            context.out.println(oldLogging.toPath() + " could not be removed!");
         }

         File newLogging = new File(etc, Create.ETC_LOG4J2_PROPERTIES);
         if (!newLogging.exists()) {
            context.out.println("Creating " + newLogging);
            try (InputStream inputStream = openStream("etc/" + Create.ETC_LOG4J2_PROPERTIES);
                 OutputStream outputStream = new FileOutputStream(newLogging);) {
               copy(inputStream, outputStream);
            }
         }
      }
   }

   protected File findBackup(ActionContext context) {
      for (int bkp = 0; bkp < 10; bkp++) {
         File bkpFolder = new File(directory, "old-config-bkp." + bkp);
         if (!bkpFolder.exists()) {
            bkpFolder.mkdirs();
            context.out.println("Using " + bkpFolder.getAbsolutePath() + " as a backup folder for the modified files");
            return bkpFolder;
         }
      }
      throw new RuntimeException("Too many backup folders in place already. Please remove some of the old-config-bkp.* folders");
   }
}
