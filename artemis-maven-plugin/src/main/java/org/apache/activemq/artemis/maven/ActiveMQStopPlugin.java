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

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

import java.io.File;
import java.io.IOException;

/**
 * @phase verify
 * @goal stop
 */
public class ActiveMQStopPlugin extends AbstractMojo
{

   /**
    * @parameter
    */
   private String configurationDir;

   public void execute() throws MojoExecutionException, MojoFailureException
   {
      String property = System.getProperty(ActiveMQStartPlugin.SKIPBROKERSTART);
      if (property != null)
      {
         return;
      }
      try
      {
         String dirName = configurationDir != null ? configurationDir : ".";
         final File file = new File(dirName + "/" + "/STOP_ME");
         file.createNewFile();
         long time = System.currentTimeMillis();
         while (System.currentTimeMillis() < time + 60000)
         {
            if (!file.exists())
            {
               break;
            }
            try
            {
               Thread.sleep(200);
            }
            catch (InterruptedException e)
            {
               //ignore
            }
         }
         if (file.exists())
         {
            throw new MojoExecutionException("looks like the server hasn't been stopped");
         }
      }
      catch (IOException e)
      {
         e.printStackTrace();
         throw new MojoExecutionException(e.getMessage());
      }
   }
}
