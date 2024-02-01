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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.activemq.artemis.cli.commands.Connect;
import org.apache.activemq.artemis.cli.commands.messages.ConnectionAbstract;
import org.jline.console.SystemRegistry;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.Parser;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.shell.jline3.PicocliCommands;

@Command(name = "shell", description = "JLine3 shell helping using the CLI")
public class Shell implements Runnable {

   @CommandLine.Option(names = "--url", description = "It will be used for an initial connection if set.")
   protected String brokerURL = ConnectionAbstract.DEFAULT_BROKER_URL;

   @CommandLine.Option(names = "--user", description = "It will be used for an initial connection if set.")
   protected String user;

   @CommandLine.Option(names = "--password", description = "It will be used for an initial connection if set.")
   protected String password;

   public Shell(CommandLine commandLine) {
   }

   @Override
   public void run() {
      setInShell();
      printBanner();
      if (brokerURL != ConnectionAbstract.DEFAULT_BROKER_URL || user != null || password != null) {
         Connect connect = new Connect();
         connect.setUser(user).setPassword(password).setBrokerURL(brokerURL);
         connect.run();
      }
      runShell(false);
   }

   private static ThreadLocal<AtomicBoolean> IN_SHELL = ThreadLocal.withInitial(() -> new AtomicBoolean(false));

   public static boolean inShell() {
      return IN_SHELL.get().get();
   }

   public static void setInShell() {
      IN_SHELL.get().set(true);
   }

   public static void runShell(boolean printBanner) {
      try {
         setInShell();

         boolean isInstance = System.getProperty("artemis.instance") != null;

         Supplier<Path> workDir = () -> Paths.get(System.getProperty("user.dir"));

         PicocliCommands.PicocliCommandsFactory factory = new PicocliCommands.PicocliCommandsFactory();

         CommandLine commandLine = Artemis.buildCommand(isInstance, !isInstance, false);

         PicocliCommands picocliCommands = new PicocliCommands(commandLine);

         Parser parser = new DefaultParser();
         try (Terminal terminal = TerminalBuilder.terminal()) {
            SystemRegistry systemRegistry = new SystemRegistryImpl(parser, terminal, workDir, null);
            systemRegistry.setCommandRegistries(picocliCommands);
            systemRegistry.register("help", picocliCommands);

            LineReader reader = LineReaderBuilder.builder()
               .terminal(terminal)
               .completer(systemRegistry.completer())
               .parser(parser)
               .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
               .build();
            factory.setTerminal(terminal);

            String prompt = org.apache.activemq.artemis.cli.Terminal.YELLOW_UNICODE + Artemis.getNameFromBanner() + " > " + org.apache.activemq.artemis.cli.Terminal.CLEAR_UNICODE;
            String rightPrompt = null;

            if (printBanner) {
               printBanner();
            }

            System.out.println("For a list of commands, type " + org.apache.activemq.artemis.cli.Terminal.RED_UNICODE + "help" + org.apache.activemq.artemis.cli.Terminal.CLEAR_UNICODE + " or press " + org.apache.activemq.artemis.cli.Terminal.RED_UNICODE + "<TAB>" + org.apache.activemq.artemis.cli.Terminal.CLEAR_UNICODE + ":");
            System.out.println("Type " + org.apache.activemq.artemis.cli.Terminal.RED_UNICODE + "exit" + org.apache.activemq.artemis.cli.Terminal.CLEAR_UNICODE + " or press " + org.apache.activemq.artemis.cli.Terminal.RED_UNICODE + "<CTRL-D>" + org.apache.activemq.artemis.cli.Terminal.CLEAR_UNICODE + " to leave the session:");

            // start the shell and process input until the user quits with Ctrl-D
            String line;
            while (true) {
               try {
                  // We build a new command every time, as they could have state from previous executions
                  systemRegistry.setCommandRegistries(new PicocliCommands(Artemis.buildCommand(isInstance, !isInstance, false)));
                  systemRegistry.cleanUp();
                  line = reader.readLine(prompt, rightPrompt, (MaskingCallback) null, null);
                  systemRegistry.execute(line);
               } catch (InterruptedException e) {
                  e.printStackTrace();
                  // Ignore
               } catch (UserInterruptException userInterruptException) {
                  // ignore
               } catch (EndOfFileException e) {
                  return;
               } catch (Exception e) {
                  systemRegistry.trace(e);
               }
            }
         }
      } catch (Throwable t) {
         t.printStackTrace();
      } finally {
         IN_SHELL.get().set(false);
      }

   }

   private static void printBanner() {
      System.out.print(org.apache.activemq.artemis.cli.Terminal.YELLOW_UNICODE);
      try {
         Artemis.printBanner(System.out);
      } catch (Exception e) {
         System.out.println("Error recovering the banner:");
         e.printStackTrace();
      }
      System.out.print(org.apache.activemq.artemis.cli.Terminal.CLEAR_UNICODE);
   }

}
