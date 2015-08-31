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

import org.apache.activemq.artemis.cli.commands.Configurable;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

public abstract class ArtemisAbstractPlugin extends AbstractMojo {


   public void execute() throws MojoExecutionException, MojoFailureException {
      if (isIgnore()) {
         getLog().debug("******************************************************************************************************");
         getLog().debug("Execution of " + getClass().getSimpleName() + " is being ignored as ignore has been set to true");
         getLog().debug("******************************************************************************************************");
      }
      else {
         doExecute();
         // We could execute the maven plugins over and over on examples
         // For that reason we just unlock the server here
         // Notice this has no implementations if you are using spawn
         Configurable.unlock();
      }
   }

   protected abstract boolean isIgnore();

   protected abstract void doExecute() throws MojoExecutionException, MojoFailureException;
}
