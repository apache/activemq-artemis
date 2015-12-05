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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Set;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

@Mojo(name = "dependency-scan", defaultPhase = LifecyclePhase.VERIFY)
public class ArtemisDependencyScanPlugin extends ArtemisAbstractPlugin {

   @Parameter
   String name;

   /**
    * The plugin descriptor
    */
   private PluginDescriptor descriptor;

   @Parameter
   private String[] libListWithDeps;

   @Parameter
   private String[] libList;

   @Parameter
   private String variableName;

   @Parameter
   private String pathSeparator = File.pathSeparator;

   /** Where to copy the exploded dependencies. */
   @Parameter
   private File targetFolder;

   @Override
   protected boolean isIgnore() {
      return false;
   }

   @Override
   protected void doExecute() throws MojoExecutionException, MojoFailureException {
      getLog().info("Local " + localRepository);
      MavenProject project = (MavenProject) getPluginContext().get("project");

      Map properties = getPluginContext();

      Set<Map.Entry> entries = properties.entrySet();

      getLog().info("Entries.size " + entries.size());
      for (Map.Entry entry : entries) {
         getLog().info("... key=" + entry.getKey() + " = " + entry.getValue());
      }


      try {
         StringBuffer buffer = new StringBuffer();
         Set<File> filesSet = resolveDependencies(libListWithDeps, libList);

         if (variableName != null) {
            String separatorUsed = "";
            for (File f : filesSet) {
               buffer.append(separatorUsed);
               buffer.append(f.getAbsolutePath());
               separatorUsed = pathSeparator;
            }

            String classPathGenerated = buffer.toString();
            project.getProperties().setProperty(variableName, classPathGenerated);
            getLog().info("dependency-scan setting: " + variableName + "=" + classPathGenerated);
         }

         if (targetFolder != null) {
            targetFolder.mkdirs();
            for (File file : filesSet) {
               Files.copy(file.toPath(), targetFolder.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
         }
      }
      catch (Throwable e) {
         getLog().error(e);
         throw new MojoFailureException(e.getMessage());
      }
   }

}
