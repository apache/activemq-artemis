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

public abstract class ActionAbstract implements Action
{

   protected ActionContext context;

   private Scanner scanner;

   private boolean noInput = false;

   protected void disableInputs()
   {
      noInput = true;

   }

   protected String input(String propertyName, String prompt, String silentDefault)
   {
      if (noInput)
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
      if (noInput)
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

   public Object execute(ActionContext context) throws Exception
   {
      this.context = context;

      scanner = new Scanner(context.in);

      return null;
   }

}
