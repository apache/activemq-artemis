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
import org.apache.activemq.artemis.util.JVMArgumentParser;

@Command(name = "upgrade", description = "Update an artemis instance to the current artemis.home, keeping all the data and broker.xml. Warning: backup your instance before using this command and compare the files.")
public class Upgrade extends InstallAbstract {

   // these are the JVM argumnents we must keep between upgrades
   private static final String[] KEEPING_JVM_ARGUMENTS = new String[]{"-Xmx", "-Djava.security.auth.login.config", "-Dhawtio.role="};

   // this is the prefix where we can find the JDK arguments in Windows script
   private static final String JDK_PREFIX_WINDOWS = "IF \"%JAVA_ARGS%\"==\"\" (set JAVA_ARGS=";

   // this is the prefix where we can find the JDK arguments in Linux script
   private static final String JDK_PREFIX_LINUX = "JAVA_ARGS=";

   public static final String OLD_LOGGING_PROPERTIES = "logging.properties";

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

      final File bkpFolder = findBackup(context);

      final File binBkp = new File(bkpFolder, "bin");
      final File etcBkp = new File(bkpFolder, "etc");
      final File tmp = new File(bkpFolder, "tmp");
      Files.createDirectory(binBkp.toPath());
      Files.createDirectory(etcBkp.toPath());
      Files.createDirectory(tmp.toPath());

      final File bin = new File(directory, "bin");
      File etcFolder = new File(directory, etc);

      final File artemisCmdScript = new File(bin, Create.ARTEMIS_CMD);
      final File artemisScript = new File(bin, Create.ARTEMIS);

      if (etc == null || etc.equals("etc")) {
         if (IS_WINDOWS && !IS_CYGWIN) {
            String pattern = "set ARTEMIS_INSTANCE_ETC=";
            etcFolder = getETC(context, etcFolder, artemisCmdScript, pattern);
         } else {
            String pattern = "ARTEMIS_INSTANCE_ETC=";
            etcFolder = getETC(context, etcFolder, artemisScript, pattern);
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
         // recreating the service.exe and config in case we ever upgrade it
         final File serviceExe = new File(directory, Create.BIN_ARTEMIS_SERVICE_EXE);
         final File serviceExeBkp = new File(bkpFolder, Create.BIN_ARTEMIS_SERVICE_EXE);

         context.out.println("Copying " + serviceExe + " to " + serviceExeBkp);
         Files.copy(serviceExe.toPath(), serviceExeBkp.toPath(), StandardCopyOption.REPLACE_EXISTING);

         context.out.println("Updating " + serviceExe.toPath());
         write(Create.BIN_ARTEMIS_SERVICE_EXE, true);

         final File serviceExeConfig = new File(directory, Create.BIN_ARTEMIS_SERVICE_EXE_CONFIG);
         final File serviceExeConfigBkp = new File(bkpFolder, Create.BIN_ARTEMIS_SERVICE_EXE_CONFIG);
         if (serviceExeConfig.exists()) {
            // It didnt exist until more recently
            context.out.println("Copying " + serviceExeConfig + " to " + serviceExeConfigBkp);
            Files.copy(serviceExeConfig.toPath(), serviceExeConfigBkp.toPath(), StandardCopyOption.REPLACE_EXISTING);
         }

         context.out.println("Updating " + serviceExeConfig);
         write(Create.BIN_ARTEMIS_SERVICE_EXE_CONFIG, true);

         final File artemisCmdScriptTmp = new File(tmp, Create.ARTEMIS_CMD);
         final File artemisCmdScriptBkp = new File(binBkp, Create.ARTEMIS_CMD);

         write(Create.BIN_ARTEMIS_CMD, artemisCmdScriptTmp, filters, false, false);
         upgrade(context, artemisCmdScriptTmp, artemisCmdScript, artemisCmdScriptBkp, "set ARTEMIS_INSTANCE_ETC=");

         final File serviceXmlTmp = new File(tmp, Create.ARTEMIS_SERVICE_XML);
         final File serviceXml = new File(bin, Create.ARTEMIS_SERVICE_XML);
         final File serviceXmlBkp = new File(binBkp, Create.ARTEMIS_SERVICE_XML);

         write(Create.BIN_ARTEMIS_SERVICE_XML, serviceXmlTmp, filters, false, false);
         upgrade(context, serviceXmlTmp, serviceXml, serviceXmlBkp,
                 "<env name=\"ARTEMIS_INSTANCE\"", "<env name=\"ARTEMIS_INSTANCE_ETC\"",
                 "<env name=\"ARTEMIS_INSTANCE_URI\"", "<env name=\"ARTEMIS_INSTANCE_ETC_URI\"",
                 "<env name=\"ARTEMIS_DATA_DIR\"", "<logpath>", "<startargument>-Xmx", "<stopargument>-Xmx",
                 "<name>", "<id>", "<startargument>-Dhawtio.role=");

         final File artemisProfileCmdTmp = new File(tmp, Create.ETC_ARTEMIS_PROFILE_CMD);
         final File artemisProfileCmd = new File(etcFolder, Create.ETC_ARTEMIS_PROFILE_CMD);
         final File artemisProfileCmdBkp = new File(etcBkp, Create.ETC_ARTEMIS_PROFILE_CMD);

         write("etc/" + Create.ETC_ARTEMIS_PROFILE_CMD, artemisProfileCmdTmp, filters, false, false);
         upgradeJDK(context, JDK_PREFIX_WINDOWS, "", KEEPING_JVM_ARGUMENTS, artemisProfileCmdTmp, artemisProfileCmd, artemisProfileCmdBkp,
                    "set ARTEMIS_INSTANCE=\"", "set ARTEMIS_DATA_DIR=", "set ARTEMIS_ETC_DIR=", "set ARTEMIS_OOME_DUMP=", "set ARTEMIS_INSTANCE_URI=", "set ARTEMIS_INSTANCE_ETC_URI=");
      }

      if (!IS_WINDOWS || IS_CYGWIN) {
         final File artemisScriptTmp = new File(tmp, Create.ARTEMIS);
         final File artemisScriptBkp = new File(binBkp, Create.ARTEMIS);

         write(Create.BIN_ARTEMIS, artemisScriptTmp, filters, false, false);
         upgrade(context, artemisScriptTmp, artemisScript, artemisScriptBkp, "ARTEMIS_INSTANCE_ETC=");

         final File artemisService = new File(bin, Create.ARTEMIS_SERVICE);
         final File artemisServiceTmp = new File(tmp, Create.ARTEMIS_SERVICE);
         final File artemisServiceBkp = new File(binBkp, Create.ARTEMIS_SERVICE);

         write(Create.BIN_ARTEMIS_SERVICE, artemisServiceTmp, filters, false, false);
         upgrade(context, artemisServiceTmp, artemisService, artemisServiceBkp); // we replace the whole thing

         write("etc/" + Create.ETC_ARTEMIS_PROFILE, new File(tmp, Create.ETC_ARTEMIS_PROFILE), filters, false, false);
         upgradeJDK(context, JDK_PREFIX_LINUX, "\"", KEEPING_JVM_ARGUMENTS,
                    new File(tmp, Create.ETC_ARTEMIS_PROFILE), new File(etcFolder, Create.ETC_ARTEMIS_PROFILE), new File(etcBkp, Create.ETC_ARTEMIS_PROFILE), "ARTEMIS_INSTANCE=",
                    "ARTEMIS_DATA_DIR=", "ARTEMIS_ETC_DIR=", "ARTEMIS_OOME_DUMP=", "ARTEMIS_INSTANCE_URI=", "ARTEMIS_INSTANCE_ETC_URI=", "HAWTIO_ROLE=");
      }

      final File bootstrapXml = new File(etcFolder, Create.ETC_BOOTSTRAP_XML);
      final File bootstrapXmlTmp = new File(tmp, Create.ETC_BOOTSTRAP_XML);
      final File bootstrapXmlBkp = new File(etcBkp, Create.ETC_BOOTSTRAP_XML);

      Files.copy(bootstrapXml.toPath(), bootstrapXmlTmp.toPath());
      replaceLines(context, bootstrapXmlTmp, bootstrapXml, bootstrapXmlBkp, "<web path", "   <web path=\"web\" rootRedirectLocation=\"console\">");

      upgradeLogging(context, etcFolder, etcBkp);

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


   private void upgradeJDK(ActionContext context, String jdkPrefix, String endOfLine, String[] keepArguments, File tmpFile, File targetFile, File bkpFile, String... keepingPrefixes) throws Exception {

      final HashMap<String, String> replaceMatrix = new HashMap<>();
      final HashMap<String, String> currentArguments = new HashMap<>();

      doUpgrade(context, tmpFile, targetFile, bkpFile,
                oldLine -> {
                   if (oldLine.trim().startsWith(jdkPrefix)) {
                      JVMArgumentParser.parseOriginalArgs(jdkPrefix, endOfLine, oldLine, keepArguments, currentArguments);
                      return;
                   } else {
                      for (String prefix : keepingPrefixes) {
                         if (oldLine.trim().startsWith(prefix)) {
                            replaceMatrix.put(prefix, oldLine);
                         }
                      }
                   }
                },
                newLine -> {
                   if (newLine.trim().startsWith(jdkPrefix)) {
                      String result =  JVMArgumentParser.parseNewLine(jdkPrefix, endOfLine, newLine, keepArguments, currentArguments);
                      return result;
                   } else {
                      for (String prefix : keepingPrefixes) {
                         if (newLine.trim().startsWith(prefix)) {
                            String originalLine = replaceMatrix.get(prefix);
                            return originalLine;
                         }
                      }
                      return newLine;
                   }
                });
   }

   private void replaceLines(ActionContext context, File tmpFile, File targetFile, File bkpFile, String... replacePairs) throws Exception {
      doUpgrade(context, tmpFile, targetFile, bkpFile,
                null,
                newLine -> {
                   for (int i = 0; i < replacePairs.length; i += 2) {
                      if (newLine.trim().startsWith(replacePairs[i])) {
                         return replacePairs[i + 1];
                      }
                   }
                   return newLine;
                });
   }

   private void upgrade(ActionContext context, File tmpFile, File targetFile, File bkpFile, String... keepingPrefixes) throws Exception {
      HashMap<String, String> replaceMatrix = new HashMap<>();

      doUpgrade(context, tmpFile, targetFile, bkpFile,
              oldLine -> {
                 if (keepingPrefixes.length > 0) {
                    for (String prefix : keepingPrefixes) {
                       if (oldLine.trim().startsWith(prefix)) {
                          replaceMatrix.put(prefix, oldLine);
                       }
                    }
                 }
              },
            newLine -> {
               if (keepingPrefixes.length > 0) {
                  for (String prefix : keepingPrefixes) {
                     if (newLine.trim().startsWith(prefix)) {
                        String originalLine = replaceMatrix.get(prefix);
                        return originalLine;
                     }
                  }
               }
               return newLine;
            });
   }

   private void doUpgrade(ActionContext context, File tmpFile, File targetFile, File bkpFile, Consumer<String> originalConsumer, Function<String, String> targetFunction) throws Exception {
      context.out.println("Copying " + targetFile + " to " + bkpFile);
      Files.copy(targetFile.toPath(), bkpFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

      // we first scan the original lines on the originalConsumer, giving a chance to the caller to fill out the original matrix
      if (originalConsumer != null) {
         try (Stream<String> originalLines = Files.lines(targetFile.toPath())) {
            originalLines.forEach(line -> {
               originalConsumer.accept(line);
            });
         }
      }

      context.out.println("Updating " + targetFile);

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

   private void upgradeLogging(ActionContext context, File etcFolder, File bkpFolder) throws Exception {
      File oldLogging = new File(etcFolder, OLD_LOGGING_PROPERTIES);

      if (oldLogging.exists()) {
         File oldLoggingCopy = new File(bkpFolder, OLD_LOGGING_PROPERTIES);
         context.out.println("Copying " + oldLogging.toPath() + " to " + oldLoggingCopy.toPath());

         Files.copy(oldLogging.toPath(), oldLoggingCopy.toPath(), StandardCopyOption.REPLACE_EXISTING);

         context.out.println("Removing " + oldLogging.toPath());
         if (!oldLogging.delete()) {
            context.out.println(oldLogging.toPath() + " could not be removed!");
         }

         File newLogging = new File(etcFolder, Create.ETC_LOG4J2_PROPERTIES);
         if (!newLogging.exists()) {
            context.out.println("Creating " + newLogging);
            try (InputStream inputStream = openStream("etc/" + Create.ETC_LOG4J2_PROPERTIES);
                 OutputStream outputStream = new FileOutputStream(newLogging);) {
               copy(inputStream, outputStream);
            }
         }
      }
   }

   protected File findBackup(ActionContext context) throws IOException {
      for (int bkp = 0; bkp < 10; bkp++) {
         File bkpFolder = new File(directory, "old-config-bkp." + bkp);
         if (!bkpFolder.exists()) {
            Files.createDirectory(bkpFolder.toPath());
            context.out.println("Using " + bkpFolder.getAbsolutePath() + " as a backup folder for the modified files");
            return bkpFolder;
         }
      }
      throw new RuntimeException("Too many backup folders in place already. Please remove some of the old-config-bkp.* folders");
   }
}
