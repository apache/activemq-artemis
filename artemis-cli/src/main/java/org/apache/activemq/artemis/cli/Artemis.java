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

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;

import io.airlift.airline.Cli;
import org.apache.activemq.artemis.cli.commands.Action;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.Create;
import org.apache.activemq.artemis.cli.commands.HelpAction;
import org.apache.activemq.artemis.cli.commands.InputAbstract;
import org.apache.activemq.artemis.cli.commands.InvalidOptionsError;
import org.apache.activemq.artemis.cli.commands.Kill;
import org.apache.activemq.artemis.cli.commands.Mask;
import org.apache.activemq.artemis.cli.commands.check.HelpCheck;
import org.apache.activemq.artemis.cli.commands.check.NodeCheck;
import org.apache.activemq.artemis.cli.commands.check.QueueCheck;
import org.apache.activemq.artemis.cli.commands.messages.Transfer;
import org.apache.activemq.artemis.cli.commands.queue.StatQueue;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.cli.commands.Stop;
import org.apache.activemq.artemis.cli.commands.address.CreateAddress;
import org.apache.activemq.artemis.cli.commands.address.DeleteAddress;
import org.apache.activemq.artemis.cli.commands.address.HelpAddress;
import org.apache.activemq.artemis.cli.commands.address.ShowAddress;
import org.apache.activemq.artemis.cli.commands.address.UpdateAddress;
import org.apache.activemq.artemis.cli.commands.messages.Browse;
import org.apache.activemq.artemis.cli.commands.messages.Consumer;
import org.apache.activemq.artemis.cli.commands.messages.Producer;
import org.apache.activemq.artemis.cli.commands.migration1x.Migrate1X;
import org.apache.activemq.artemis.cli.commands.queue.CreateQueue;
import org.apache.activemq.artemis.cli.commands.queue.DeleteQueue;
import org.apache.activemq.artemis.cli.commands.queue.HelpQueue;
import org.apache.activemq.artemis.cli.commands.queue.PurgeQueue;
import org.apache.activemq.artemis.cli.commands.queue.UpdateQueue;
import org.apache.activemq.artemis.cli.commands.tools.HelpData;
import org.apache.activemq.artemis.cli.commands.tools.PrintData;
import org.apache.activemq.artemis.cli.commands.tools.journal.CompactJournal;
import org.apache.activemq.artemis.cli.commands.tools.journal.DecodeJournal;
import org.apache.activemq.artemis.cli.commands.tools.journal.EncodeJournal;
import org.apache.activemq.artemis.cli.commands.tools.journal.PerfJournal;
import org.apache.activemq.artemis.cli.commands.tools.xml.XmlDataExporter;
import org.apache.activemq.artemis.cli.commands.tools.xml.XmlDataImporter;
import org.apache.activemq.artemis.cli.commands.user.AddUser;
import org.apache.activemq.artemis.cli.commands.user.HelpUser;
import org.apache.activemq.artemis.cli.commands.user.ListUser;
import org.apache.activemq.artemis.cli.commands.user.RemoveUser;
import org.apache.activemq.artemis.cli.commands.user.ResetUser;

/**
 * Artemis is the main CLI entry point for managing/running a broker.
 *
 * Want to start or debug a broker from an IDE?  This is probably the best class to
 * run.  Make sure set the -Dartemis.instance=path/to/instance system property.
 * You should also use the 'apache-artemis' module for the class path since that
 * includes all artemis modules.
 */
public class Artemis {

   public static void main(String... args) throws Exception {
      String home = System.getProperty("artemis.home");
      File fileHome = home != null ? new File(home) : null;
      String instance = System.getProperty("artemis.instance");
      File fileInstance = instance != null ? new File(instance) : null;

      execute(true, fileHome, fileInstance, args);
   }

   public static Object internalExecute(String... args) throws Exception {
      return internalExecute(null, null, args);
   }

   public static Object execute(File artemisHome, File artemisInstance, List<String> args) throws Exception {
      return execute(false, artemisHome, artemisInstance, args.toArray(new String[args.size()]));
   }

   public static Object execute(boolean inputEnabled, File artemisHome, File artemisInstance, ActionContext context, String... args) throws Exception {

      if (inputEnabled) {
         InputAbstract.enableInput();
      }
      try {
         return internalExecute(artemisHome, artemisInstance, args, context);
      } catch (ConfigurationException configException) {
         System.err.println(configException.getMessage());
         System.out.println();
         System.out.println("Configuration should be specified as 'scheme:location'. Default configuration is 'xml:${ARTEMIS_INSTANCE}/etc/bootstrap.xml'");
         return configException;
      } catch (CLIException cliException) {
         System.err.println(cliException.getMessage());
         return cliException;
      } catch (NullPointerException e) {
         // Yeah.. I really meant System.err..
         // this is the CLI and System.out and System.err are common places for interacting with the user
         // this is a programming error that must be visualized and corrected
         e.printStackTrace();
         return e;
      } catch (RuntimeException | InvalidOptionsError re) {
         System.err.println(re.getMessage());
         System.out.println();

         Cli<Action> parser = builder(null).build();

         parser.parse("help").execute(context);
         return re;
      }
   }

   public static Object execute(boolean inputEnabled, File artemisHome, File artemisInstance, String... args) throws Exception {
      return execute(inputEnabled, artemisHome, artemisInstance, ActionContext.system(), args);
   }

   /**
    * This method is used to validate exception returns.
    * Useful on test cases
    */
   private static Object internalExecute(File artemisHome, File artemisInstance, String[] args) throws Exception {
      return internalExecute(artemisHome, artemisInstance, args, ActionContext.system());
   }

   public static Object internalExecute(File artemisHome, File artemisInstance, String[] args, ActionContext context) throws Exception {
      Action action = builder(artemisInstance).build().parse(args);
      action.setHomeValues(artemisHome, artemisInstance);

      if (action.isVerbose()) {
         System.out.print("Executing " + action.getClass().getName() + " ");
         for (String arg : args) {
            System.out.print(arg + " ");
         }
         System.out.println();
         System.out.println("Home::" + action.getBrokerHome() + ", Instance::" + action.getBrokerInstance());
      }

      action.checkOptions(args);
      return action.execute(context);
   }

   private static Cli.CliBuilder<Action> builder(File artemisInstance) {
      String instance = artemisInstance != null ? artemisInstance.getAbsolutePath() : System.getProperty("artemis.instance");
      Cli.CliBuilder<Action> builder = Cli.<Action>builder("artemis").withDescription("ActiveMQ Artemis Command Line").withCommand(HelpAction.class).withCommand(Producer.class).withCommand(Transfer.class).withCommand(Consumer.class).withCommand(Browse.class).withCommand(Mask.class).withDefaultCommand(HelpAction.class);

      builder.withGroup("check").withDescription("Check tools group (node|queue) (example ./artemis check node)").
         withDefaultCommand(HelpCheck.class).withCommands(NodeCheck.class, QueueCheck.class);

      builder.withGroup("queue").withDescription("Queue tools group (create|delete|update|stat|purge) (example ./artemis queue create)").
         withDefaultCommand(HelpQueue.class).withCommands(CreateQueue.class, DeleteQueue.class, UpdateQueue.class, StatQueue.class, PurgeQueue.class);

      builder.withGroup("address").withDescription("Address tools group (create|delete|update|show) (example ./artemis address create)").
         withDefaultCommand(HelpAddress.class).withCommands(CreateAddress.class, DeleteAddress.class, UpdateAddress.class, ShowAddress.class);

      if (instance != null) {
         builder.withGroup("data").withDescription("data tools group (print|imp|exp|encode|decode|compact) (example ./artemis data print)").
            withDefaultCommand(HelpData.class).withCommands(PrintData.class, XmlDataExporter.class, XmlDataImporter.class, DecodeJournal.class, EncodeJournal.class, CompactJournal.class);
         builder.withGroup("user").withDescription("default file-based user management (add|rm|list|reset) (example ./artemis user list)").
                 withDefaultCommand(HelpUser.class).withCommands(ListUser.class, AddUser.class, RemoveUser.class, ResetUser.class);
         builder = builder.withCommands(Run.class, Stop.class, Kill.class, PerfJournal.class);
      } else {
         builder.withGroup("data").withDescription("data tools group (print) (example ./artemis data print)").
            withDefaultCommand(HelpData.class).withCommands(PrintData.class);
         builder = builder.withCommand(Create.class);
         builder = builder.withCommand(Migrate1X.class);
      }

      return builder;
   }

   public static void printBanner() throws Exception {
      printBanner(System.out);
   }

   public static void printBanner(PrintStream out) throws Exception {
      copy(Artemis.class.getResourceAsStream("banner.txt"), out);
   }

   private static long copy(InputStream in, OutputStream out) throws Exception {
      try {
         byte[] buffer = new byte[1024];
         int len = in.read(buffer);
         while (len != -1) {
            out.write(buffer, 0, len);
            len = in.read(buffer);
         }
         return len;
      } finally {
         in.close();
      }
   }

}
