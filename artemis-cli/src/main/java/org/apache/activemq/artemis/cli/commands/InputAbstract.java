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

package org.apache.activemq.artemis.cli.commands;

import java.util.Scanner;

import io.airlift.airline.Option;

public class InputAbstract extends ActionAbstract
{

   private Scanner scanner;

   @Option(name = "--silent-input", description = "It will disable all the inputs, and it would make a best guess for any required input")
   private boolean silentInput = false;

   public boolean isSilentInput()
   {
      return silentInput;
   }

   public void setSilentInput(boolean silentInput)
   {
      this.silentInput = silentInput;
   }

   protected String input(String propertyName, String prompt, String silentDefault)
   {
      if (silentInput)
      {
         return silentDefault;
      }

      String inputStr;
      boolean valid = false;
      System.out.println();
      do
      {
         context.out.println(propertyName + ": is mandatory with this configuration:");
         context.out.println(prompt);
         inputStr = scanner.nextLine();
         if (inputStr.trim().equals(""))
         {
            System.out.println("Invalid Entry!");
         }
         else
         {
            valid = true;
         }
      }
      while (!valid);

      return inputStr.trim();
   }

   protected String inputPassword(String propertyName, String prompt, String silentDefault)
   {
      if (silentInput)
      {
         return silentDefault;
      }

      String inputStr;
      boolean valid = false;
      System.out.println();
      do
      {
         context.out.println(propertyName + ": is mandatory with this configuration:");
         context.out.println(prompt);
         inputStr = new String(System.console().readPassword());

         if (inputStr.trim().equals(""))
         {
            System.out.println("Invalid Entry!");
         }
         else
         {
            valid = true;
         }
      }
      while (!valid);

      return inputStr.trim();
   }

   @Override
   public Object execute(ActionContext context) throws Exception
   {
      super.execute(context);

      this.scanner = new Scanner(context.in);

      return null;
   }

}
