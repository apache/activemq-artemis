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
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Map;

import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

@Mojo(name = "cli", defaultPhase = LifecyclePhase.VERIFY)
public class ArtemisCLIPlugin extends AbstractMojo

{

   @Parameter
   String name;

   /**
    * The plugin descriptor
    */
   private PluginDescriptor descriptor;

   @Parameter(defaultValue = "${activemq.basedir}", required = true)
   private File home;

   @Parameter(defaultValue = "${activemq.basedir}/artemis-distribution/target/apache-artemis-${project.version}-bin/apache-artemis-${project.version}/", required = true)
   private File alternateHome;

   @Parameter(defaultValue = "${basedir}/target/server0", required = true)
   private File location;

   @Parameter
   private String[] args;

   @Parameter
   private boolean spawn = false;

   @Parameter
   private boolean testServer;


   /**
    * Validate if the directory is a artemis.home *
    *
    * @param path
    * @return
    */
   private boolean lookupHome(Path path)
   {

      if (path == null)
      {
         return false;
      }

      Path binFolder = path.resolve("bin");

      if (binFolder == null && Files.exists(binFolder, LinkOption.NOFOLLOW_LINKS))
      {
         return false;
      }

      Path artemisScript = binFolder.resolve("artemis");


      return artemisScript != null && Files.exists(artemisScript, LinkOption.NOFOLLOW_LINKS);


   }

   public void execute() throws MojoExecutionException, MojoFailureException
   {
      // This is to avoid the Run issuing a kill at any point
      Run.setEmbedded(true);

      MavenProject project = (MavenProject) getPluginContext().get("project");


      if (!lookupHome(home.toPath()))
      {
         if (lookupHome(alternateHome.toPath()))
         {
            home = alternateHome;
         }
         else
         {
            getLog().error("********************************************************************************************");
            getLog().error("Could not locate suitable Artemis.home on either " + home + " or " + alternateHome);
            getLog().error("Use the binary distribution or build the distribution before running the examples");
            getLog().error("********************************************************************************************");

            throw new MojoExecutionException("Couldn't find artemis.home");
         }
      }

      Map properties = getPluginContext();

      try
      {
         if (spawn)
         {
            final Process process = org.apache.activemq.artemis.cli.process.ProcessBuilder.build("server", location, true, args);
            Runtime.getRuntime().addShutdownHook(new Thread()
            {
               public void run()
               {
                  process.destroy();
               }
            });

            if (testServer)
            {
               for (int tryNr = 0; tryNr < 20; tryNr++)
               {
                  try
                  {
                     ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory();
                     cf.createConnection().close();
                     getLog().info("Server started");
                  }
                  catch (Exception e)
                  {
                     getLog().info("awaiting server to start");
                     Thread.sleep(500);
                     continue;
                  }
                  break;
               }
            }
         }
         else
         {
            Artemis.execute(home, location, args);
         }

         Thread.sleep(600);

         org.apache.activemq.artemis.cli.process.ProcessBuilder.cleanupProcess();
      }
      catch (Exception e)
      {
         throw new MojoExecutionException(e.getMessage(), e);
      }
   }
}
