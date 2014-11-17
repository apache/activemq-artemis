/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.cli;

import io.airlift.command.Cli;
import io.airlift.command.ParseArgumentsUnexpectedException;
import org.apache.activemq.cli.commands.Action;
import org.apache.activemq.cli.commands.ActionContext;
import org.apache.activemq.cli.commands.HelpAction;
import org.apache.activemq.cli.commands.Run;
import org.apache.activemq.cli.commands.Stop;

import java.io.InputStream;
import java.io.OutputStream;

public class ActiveMQ
{

   public static void main(String[] args) throws Exception
   {
      Cli.CliBuilder<Action> builder = Cli.<Action>builder("activemq")
               .withDefaultCommand(HelpAction.class)
               .withCommand(Run.class)
               .withCommand(Stop.class)
               .withCommand(HelpAction.class)
               .withDescription("ActiveMQ Command Line");

      Cli<Action> parser = builder.build();

      try
      {
         parser.parse(args).execute(ActionContext.system());
      }
      catch (ParseArgumentsUnexpectedException e)
      {
         System.err.println(e.getMessage());
         System.out.println();
         parser.parse("help").execute(ActionContext.system());
      }
      catch (ConfigurationException configException)
      {
         System.err.println(configException.getMessage());
         System.out.println();
         System.out.println("Configuration should be specified as 'scheme:location'. Default configuration is 'xml:${ACTIVEMQ_HOME}/config/non-clustered/bootstrap.xml'");
      }

   }

   public static void printBanner() throws Exception
   {
      copy(ActiveMQ.class.getResourceAsStream("banner.txt"), System.out);
   }

   private static long copy(InputStream in, OutputStream out) throws Exception
   {
      byte[] buffer = new byte[1024];
      int len = in.read(buffer);
      while (len != -1)
      {
         out.write(buffer, 0, len);
         len = in.read(buffer);
      }
      return len;
   }

}
