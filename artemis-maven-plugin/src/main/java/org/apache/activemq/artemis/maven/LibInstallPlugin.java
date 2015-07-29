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
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;

import org.apache.maven.plugin.AbstractMojo;
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

@Mojo(name = "lib-install", defaultPhase = LifecyclePhase.VERIFY)
public class LibInstallPlugin extends AbstractMojo

{

   @Parameter
   String name;

   /**
    * The plugin descriptor
    */
   private PluginDescriptor descriptor;

   @Parameter(defaultValue = "${basedir}/target/server0", required = true)
   private File instance;

   @Component
   private RepositorySystem repositorySystem;

   @Parameter(defaultValue = "${repositorySystemSession}")
   private RepositorySystemSession repoSession;

   @Parameter(defaultValue = "${project.remoteProjectRepositories}")
   private List<RemoteRepository> remoteRepos;


   @Parameter
   private String[] libList;

   @Parameter(defaultValue = "${localRepository}")
   private org.apache.maven.artifact.repository.ArtifactRepository localRepository;

   public void execute() throws MojoExecutionException, MojoFailureException
   {
      MavenProject project = (MavenProject) getPluginContext().get("project");

      Map properties = getPluginContext();

      try
      {

         File projectLib = project.getArtifact().getFile();
         copyToLib(projectLib);

         if (libList != null)
         {
            for (int i = 0; i < libList.length; i++)
            {
               String[] splitString = libList[i].split(":");

               getLog().info("********************" + splitString[0] + "/" + splitString[1] + "/" + splitString[2]);

               Artifact artifact;
               try
               {
                  artifact = new DefaultArtifact(libList[i]);
               }
               catch (IllegalArgumentException e)
               {
                  throw new MojoFailureException(e.getMessage(), e);
               }

               ArtifactRequest request = new ArtifactRequest();
               request.setArtifact(artifact);
               request.setRepositories(remoteRepos);

               getLog().info("Resolving artifact " + artifact + " from " + remoteRepos);

               ArtifactResult result;
               try
               {
                  result = repositorySystem.resolveArtifact(repoSession, request);
               }
               catch (ArtifactResolutionException e)
               {
                  throw new MojoExecutionException(e.getMessage(), e);
               }

               File artifactFile = result.getArtifact().getFile();

               getLog().info("Artifact:: " + artifact + " file = " + artifactFile);

               copyToLib(artifactFile);

            }
         }

      }
      catch (Exception e)
      {
         getLog().error(e);
         throw new MojoFailureException(e.getMessage());
      }
   }

   private void copyToLib(File projectLib) throws IOException
   {
      Path target = instance.toPath().resolve("lib").resolve(projectLib.getName());
      target.toFile().mkdirs();
      getLog().info("Copying " + projectLib.getName() + " as " + target.toFile().getAbsolutePath());
      Files.copy(projectLib.toPath(), target, StandardCopyOption.REPLACE_EXISTING);
   }
}
