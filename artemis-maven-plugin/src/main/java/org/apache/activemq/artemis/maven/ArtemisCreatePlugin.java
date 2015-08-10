/**
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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.cli.Artemis;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;

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

   @Component
   private RepositorySystem repositorySystem;

   @Parameter(defaultValue = "${repositorySystemSession}")
   private RepositorySystemSession repoSession;

   @Parameter(defaultValue = "${project.remoteProjectRepositories}")
   private List<RemoteRepository> remoteRepos;

   /**
    * For extra stuff not covered by the properties
    */
   @Parameter
   ArrayList<String> args = new ArrayList<>();

   @Parameter
   private String[] libList;

   @Parameter(defaultValue = "${localRepository}")
   private org.apache.maven.artifact.repository.ArtifactRepository localRepository;

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
   protected void doExecute() throws MojoExecutionException, MojoFailureException {
      if (System.getProperty("bypassAddress") != null) {
         System.out.println("BYPASSADDRESS");
      }
      getLog().info("Local " + localRepository);
      MavenProject project = (MavenProject) getPluginContext().get("project");

      if (!lookupHome(home.toPath())) {
         if (lookupHome(alternateHome.toPath())) {
            home = alternateHome;
         }
         else {
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
      }
      else {
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

      for (String str : args) {
         add(listCommands, str);
      }

      add(listCommands, instance.getAbsolutePath());

      getLog().debug("***** Server created at " + instance + " with home=" + home + " *****");

      try {
         Artemis.execute(home, null, listCommands);

         if (configuration != null) {
            String[] list = configuration.list();

            if (list != null) {
               getLog().debug("************************************************");
               getLog().debug("Replacing configuration files:");

               for (String file : configuration.list()) {
                  Path target = instance.toPath().resolve("etc").resolve(file);
                  getLog().debug("Replacing " + file + " into " + target);

                  Files.copy(configuration.toPath().resolve(file), target, StandardCopyOption.REPLACE_EXISTING);
               }
            }
         }

         if (libList != null) {
            for (int i = 0; i < libList.length; i++) {
               String[] splitString = libList[i].split(":");

               getLog().debug("********************" + splitString[0] + "/" + splitString[1] + "/" + splitString[2]);

               Artifact artifact;
               try {
                  artifact = new DefaultArtifact(libList[i]);
               }
               catch (IllegalArgumentException e) {
                  throw new MojoFailureException(e.getMessage(), e);
               }

               ArtifactRequest request = new ArtifactRequest();
               request.setArtifact(artifact);
               request.setRepositories(remoteRepos);

               getLog().debug("Resolving artifact " + artifact + " from " + remoteRepos);

               ArtifactResult result;
               try {
                  result = repositorySystem.resolveArtifact(repoSession, request);
               }
               catch (ArtifactResolutionException e) {
                  throw new MojoExecutionException(e.getMessage(), e);
               }

               File artifactFile = result.getArtifact().getFile();

               getLog().debug("Artifact:: " + artifact + " file = " + artifactFile);

               copyToLib(artifactFile);

            }
         }

      }
      catch (Exception e) {
         getLog().error(e);
         throw new MojoFailureException(e.getMessage());
      }
   }

   private void copyToLib(File projectLib) throws IOException {
      Path target = instance.toPath().resolve("lib").resolve(projectLib.getName());
      target.toFile().mkdirs();
      getLog().debug("Copying " + projectLib.getName() + " as " + target.toFile().getAbsolutePath());
      Files.copy(projectLib.toPath(), target, StandardCopyOption.REPLACE_EXISTING);
   }
}
