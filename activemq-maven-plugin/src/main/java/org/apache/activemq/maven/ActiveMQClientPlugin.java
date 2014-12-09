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
package org.apache.activemq.maven;

import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *
 *         Allows a Java Client to be run which must hve a static main(String[] args) method
 */

/**
 * @phase verify
 * @goal runClient
 */
public class ActiveMQClientPlugin extends AbstractMojo
{

   /**
    * @parameter
    */
   String clientClass;

   /**
    * @parameter
    */
   String[] args;

   /**
    * @parameter
    */
   private Properties systemProperties;

   public void execute() throws MojoExecutionException, MojoFailureException
   {
      try
      {
         if (systemProperties != null && !systemProperties.isEmpty())
         {
            System.getProperties().putAll(systemProperties);
         }
         Class aClass = Class.forName(clientClass);
         Method method = aClass.getDeclaredMethod("main", new Class[]{String[].class});
         method.invoke(null, new Object[]{args});
      }
      catch (Exception e)
      {
         e.printStackTrace();
         throw new MojoFailureException(e.getMessage());
      }
   }
}
