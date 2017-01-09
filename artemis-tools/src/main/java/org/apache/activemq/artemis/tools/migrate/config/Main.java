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
package org.apache.activemq.artemis.tools.migrate.config;

import java.io.File;

public class Main {

   public static void main(String[] args) throws Exception {

      if (args.length == 0) {
         System.err.println("Invalid args");
         printUsage();
      } else {
         File input = new File(args[0]);
         if (input.isDirectory()) {
            System.out.println("Scanning directory: " + input.getAbsolutePath());
            scanAndTransform(input);
         } else {
            if (args.length != 2) {
               System.err.println("Invalid args");
               printUsage();
            } else {
               try {
                  XMLConfigurationMigration migration = new XMLConfigurationMigration(input, new File(args[1]));
                  migration.transform();
               } catch (Exception e) {
                  // Unable to process file, move on.
               }
            }
         }
      }
   }

   private static void recursiveTransform(File root) throws Exception {
      for (File file : root.listFiles()) {
         scanAndTransform(file);
      }
   }

   public static void scanAndTransform(File f) throws Exception {
      try {
         if (f.isDirectory()) {
            recursiveTransform(f);
         } else {
            try {
               if (f.getName().endsWith("xml")) {
                  File file = new File(f.getAbsolutePath() + ".new");
                  XMLConfigurationMigration migration = new XMLConfigurationMigration(f, file);
                  if (migration.transform()) {
                     File r = new File(f.getAbsolutePath());
                     f.renameTo(new File(f.getAbsolutePath() + ".bk"));
                     file.renameTo(r);
                     System.out.println(f + " converted, old file renamed as " + f.getAbsolutePath() + ".bk");
                  }
               }
            } catch (Exception e) {
               //Unable to process file, continue
            }
         }
      } catch (NullPointerException e) {
         System.out.println(f.getAbsoluteFile());
      }
   }

   public static void printUsage() {
      System.out.println("Please specify a directory to scan, or input and output file");
   }

}
