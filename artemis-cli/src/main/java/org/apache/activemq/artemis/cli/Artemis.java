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
package org.apache.activemq.artemis.cli;

import java.io.InputStream;
import java.io.OutputStream;

import io.airlift.airline.Cli;
import io.airlift.airline.ParseArgumentsMissingException;
import io.airlift.airline.ParseArgumentsUnexpectedException;
import org.apache.activemq.artemis.cli.commands.Action;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.Create;
import org.apache.activemq.artemis.cli.commands.HelpAction;
import org.apache.activemq.artemis.cli.commands.Kill;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.cli.commands.Stop;
import org.apache.activemq.artemis.cli.commands.tools.DecodeJournal;
import org.apache.activemq.artemis.cli.commands.tools.EncodeJournal;
import org.apache.activemq.artemis.cli.commands.tools.HelpData;
import org.apache.activemq.artemis.cli.commands.tools.PrintData;
import org.apache.activemq.artemis.cli.commands.tools.XmlDataExporter;
import org.apache.activemq.artemis.cli.commands.tools.XmlDataImporter;

public class Artemis
{
   @SuppressWarnings("unchecked")
   public static void main(String[] args) throws Exception
   {
      String instance = System.getProperty("artemis.instance");
      Cli.CliBuilder<Action> builder = Cli.<Action>builder("artemis")
         .withDescription("ActiveMQ Artemis Command Line")
         .withCommand(HelpAction.class)
         .withDefaultCommand(HelpAction.class);


      builder.withGroup("data")
              .withDescription("data tools group (print|exp|imp|exp|encode|decode) (example ./artemis data print)").
              withDefaultCommand(HelpData.class).withCommands(PrintData.class, XmlDataExporter.class,
              XmlDataImporter.class,DecodeJournal.class, EncodeJournal.class);

      if (instance != null)
      {
         builder = builder.withCommands(Run.class, Stop.class, Kill.class);
      }
      else
      {
         builder = builder.withCommand(Create.class);
      }

      Cli<Action> parser = builder.build();
      try
      {
         parser.parse(args).execute(ActionContext.system());
      }
      catch (ParseArgumentsUnexpectedException | ParseArgumentsMissingException e)
      {
         System.err.println(e.getMessage());
         System.out.println();
         parser.parse("help").execute(ActionContext.system());
      }
      catch (ConfigurationException configException)
      {
         System.err.println(configException.getMessage());
         System.out.println();
         System.out.println("Configuration should be specified as 'scheme:location'. Default configuration is 'xml:${ARTEMIS_INSTANCE}/etc/bootstrap.xml'");
      }

   }

   public static void printBanner() throws Exception
   {
      copy(Artemis.class.getResourceAsStream("banner.txt"), System.out);
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
