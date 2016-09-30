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
package org.apache.activemq.artemis.maven;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.boot.Artemis;
import org.apache.activemq.artemis.utils.FileUtil;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

@Mojo(name = "create", defaultPhase = LifecyclePhase.VERIFY)
public class ArtemisCreatePlugin extends ArtemisAbstractPlugin {

   @Parameter
   String name;

   /**
    * The plugin descriptor
    */
   private PluginDescriptor descriptor;

   /**
    * Directory to replace the configuration with
    */
   @Parameter(defaultValue = "${basedir}/target/classes/activemq/server0", required = true)
   private File configuration;

   @Parameter(defaultValue = "${activemq.basedir}", required = true)
   private File home;

   @Parameter(defaultValue = "${activemq.basedir}/artemis-distribution/target/apache-artemis-${project.version}-bin/apache-artemis-${project.version}/", required = true)
   private File alternateHome;

   @Parameter(defaultValue = "${basedir}/target/server0", required = true)
   private File instance;

   @Parameter(defaultValue = "true")
   private boolean noWeb;

   @Parameter(defaultValue = "guest")
   private String user;

   @Parameter(defaultValue = "guest")
   private String password;

   @Parameter(defaultValue = "guest")
   private String role;

   @Parameter(defaultValue = "")
   private String javaOptions = "";

   @Parameter(defaultValue = "0")
   private int portOffset = 0;

   @Parameter(defaultValue = "true")
   private boolean allowAnonymous;

   @Parameter(defaultValue = "false")
   private boolean replicated;

   @Parameter(defaultValue = "false")
   private boolean sharedStore;

   @Parameter(defaultValue = "false")
   private boolean clustered;

   @Parameter(defaultValue = "false")
   private boolean slave;

   @Parameter(defaultValue = "./data")
   String dataFolder;

   @Parameter(defaultValue = "false")
   private boolean failoverOnShutdown;

   /**
    * it will disable auto-tune
    */
   @Parameter(defaultValue = "true")
   private boolean noAutoTune;

   @Parameter(defaultValue = "ON_DEMAND")
   private String messageLoadBalancing;

   /**
    * For extra stuff not covered by the properties
    */
   @Parameter
   ArrayList<String> args = new ArrayList<>();

   /**
    * Deprecated, use dependencyList and individualList
    */
   @Parameter
   private String[] libList;

   @Parameter
   private String[] libListWithDeps;

   @Parameter(defaultValue = "${localRepository}")
   private org.apache.maven.artifact.repository.ArtifactRepository localRepository;

   @Parameter(defaultValue = "${noServer}")
   boolean ignore;

   /**
    * Validate if the directory is a artemis.home *
    *
    * @param path
    * @return
    */
   private boolean lookupHome(Path path) {

      if (path == null) {
         return false;
      }

      Path binFolder = path.resolve("bin");

      if (binFolder == null && Files.exists(binFolder, LinkOption.NOFOLLOW_LINKS)) {
         return false;
      }

      Path artemisScript = binFolder.resolve("artemis");

      return artemisScript != null && Files.exists(artemisScript, LinkOption.NOFOLLOW_LINKS);

   }

   private void add(List<String> list, String... str) {
      for (String s : str) {
         list.add(s);
      }
   }

   @Override
   protected boolean isIgnore() {
      return ignore;
   }

   @Override
   protected void doExecute() throws MojoExecutionException, MojoFailureException {
      getLog().info("Local " + localRepository);
      MavenProject project = (MavenProject) getPluginContext().get("project");

      if (!lookupHome(home.toPath())) {
         if (lookupHome(alternateHome.toPath())) {
            home = alternateHome;
         } else {
            getLog().error("********************************************************************************************");
            getLog().error("Could not locate suitable Artemis.home on either " + home + " or " + alternateHome);
            getLog().error("Use the binary distribution or build the distribution before running the examples");
            getLog().error("********************************************************************************************");

            throw new MojoExecutionException("Couldn't find artemis.home");
         }
      }

      Map properties = getPluginContext();

      Set<Map.Entry> entries = properties.entrySet();

      getLog().info("Entries.size " + entries.size());
      for (Map.Entry entry : entries) {
         getLog().info("... key=" + entry.getKey() + " = " + entry.getValue());
      }

      ArrayList<String> listCommands = new ArrayList<>();

      add(listCommands, "create", "--allow-anonymous", "--silent", "--force", "--no-web", "--user", user, "--password", password, "--role", role, "--port-offset", "" + portOffset, "--data", dataFolder);

      if (allowAnonymous) {
         add(listCommands, "--allow-anonymous");
      } else {
         add(listCommands, "--require-login");
      }

      if (!javaOptions.isEmpty()) {
         add(listCommands, "--java-options", javaOptions);
      }

      if (slave) {
         add(listCommands, "--slave");
      }

      if (replicated) {
         add(listCommands, "--replicated");
      }

      if (sharedStore) {
         add(listCommands, "--shared-store");
      }

      if (clustered) {
         add(listCommands, "--clustered");
         add(listCommands, "--message-load-balancing", messageLoadBalancing);
      }

      if (failoverOnShutdown) {
         add(listCommands, "--failover-on-shutdown");
      }

      if (noAutoTune) {
         add(listCommands, "--no-autotune");
      }

      add(listCommands, "--verbose");

      if ("Linux".equals(System.getProperty("os.name"))) {
         add(listCommands, "--aio");
      }

      for (String str : args) {
         add(listCommands, str);
      }

      add(listCommands, instance.getAbsolutePath());

      getLog().debug("***** Server created at " + instance + " with home=" + home + " *****");

      File commandLine = new File(instance.getParentFile(), "create-" + instance.getName() + ".sh");
      FileOutputStream outputStream;
      try {
         outputStream = new FileOutputStream(commandLine);
      } catch (Exception e) {
         e.printStackTrace();
         throw new MojoExecutionException(e.getMessage(), e);
      }

      PrintStream commandLineStream = new PrintStream(outputStream);
      commandLineStream.println("# These are the commands used to create " + instance.getName());
      commandLineStream.println(getCommandline(listCommands));

      try {
         Artemis.execute(home, null, listCommands);

         if (configuration != null) {
            String[] list = configuration.list();

            if (list != null) {
               getLog().debug("************************************************");
               getLog().debug("Copying configuration files:");

               copyConfigurationFiles(list, configuration.toPath(), instance.toPath().resolve("etc"), commandLineStream);
            }
         }

         Set<File> files = resolveDependencies(libListWithDeps, libList);

         if (!files.isEmpty()) {
            commandLineStream.println();
            commandLineStream.println("# This is a list of files that need to be installed under ./lib.");
            commandLineStream.println("# We are copying them from your maven lib home");
            for (File file : files) {
               copyToLib(file, commandLineStream);
            }
         }

         commandLineStream.close();

         FileUtil.makeExec(commandLine);

         getLog().info("###################################################################################################");
         getLog().info(commandLine.getName() + " created with commands to reproduce " + instance.getName());
         getLog().info("under " + commandLine.getParent());
         getLog().info("###################################################################################################");

      } catch (Throwable e) {
         getLog().error(e);
         throw new MojoFailureException(e.getMessage());
      }
   }

   private void copyConfigurationFiles(String[] list,
                                       Path sourcePath,
                                       Path targetPath,
                                       PrintStream commandLineStream) throws IOException {
      for (String file : list) {
         Path target = targetPath.resolve(file);

         Path originalFile = sourcePath.resolve(file);
         Files.copy(originalFile, target, StandardCopyOption.REPLACE_EXISTING);

         commandLineStream.println("");

         if (originalFile.toFile().isDirectory()) {
            getLog().debug("Creating directory " + target);
            commandLineStream.println("# creating directory " + originalFile.getFileName());
            commandLineStream.println("mkdir " + target);

            copyConfigurationFiles(originalFile.toFile().list(), originalFile, target, commandLineStream);
         } else {
            getLog().debug("Copying " + file + " to " + target);
            commandLineStream.println("# copying config file " + originalFile.getFileName());
            commandLineStream.println("cp " + originalFile + " " + target);
         }
      }
   }

   private String getCommandline(ArrayList<String> listCommands) {
      StringBuffer buffer = new StringBuffer();
      buffer.append(home.getAbsolutePath() + "/bin/artemis ");
      for (String string : listCommands) {
         buffer.append(string + " ");
      }
      return buffer.toString();
   }

   private void copyToLib(File projectLib, PrintStream commandLineStream) throws IOException {
      Path target = instance.toPath().resolve("lib").resolve(projectLib.getName());
      File file = target.toFile();
      File parent = file.getParentFile();
      if (!parent.exists()) {
         parent.mkdirs();
         commandLineStream.println("mkdir " + file.getParent());
      }

      commandLineStream.println("cp " + projectLib.getAbsolutePath() + " " + target);
      getLog().debug("Copying " + projectLib.getName() + " as " + target.toFile().getAbsolutePath());
      Files.copy(projectLib.toPath(), target, StandardCopyOption.REPLACE_EXISTING);
   }
}
