/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.component;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.jboss.logging.Logger;

/**
 * This class is used to remove the jar files
 * in temp web dir on Windows platform where
 * handles of the jar files are never released
 * by URLClassLoader until the whole VM exits.
 */
public class WebTmpCleaner {

   private static final Logger logger = Logger.getLogger(WebTmpCleaner.class);

   public static void main(String[] filesToClean) throws Exception {
      //It needs to retry a bit as we are not sure
      //when the main VM exists.
      cleanupFilesWithRetry(filesToClean, 100);
   }

   private static boolean cleanupFilesWithRetry(String[] filesToClean, int maxRetries) throws Exception {
      boolean allCleaned = false;
      while (!allCleaned && maxRetries-- > 0) {
         allCleaned = true;
         for (String f : filesToClean) {
            if (!f.trim().isEmpty()) {
               URI url = new URI(f);
               File file = new File(url);
               if (file.exists()) {
                  deleteFolder(file);
                  allCleaned = false;
               }
            }
         }
         Thread.sleep(200);
      }
      if (!allCleaned) {
         logger.warn("Some files in web temp dir are not cleaned up after " + maxRetries + " retries.");
      }
      return allCleaned;
   }

   public static Process cleanupTmpFiles(File libFolder, List<File> temporaryFiles) throws Exception {
      return cleanupTmpFiles(libFolder, temporaryFiles, false);
   }

   public static Process cleanupTmpFiles(File libFolder, List<File> temporaryFiles, boolean invm) throws Exception {
      ArrayList<String> files = new ArrayList<>(temporaryFiles.size());
      for (File f : temporaryFiles) {
         files.add(f.toURI().toString());
      }

      if (!invm) {
         String classPath = SpawnedVMSupport.getClassPath(libFolder);
         return SpawnedVMSupport.spawnVM(classPath, WebTmpCleaner.class.getName(), false, (String[]) files.toArray(new String[files.size()]));
      }
      cleanupFilesWithRetry(files.toArray(new String[files.size()]), 2);
      return null;
   }

   public static final void deleteFolder(final File file) {
      if (file.isDirectory()) {
         String[] files = file.list();
         if (files != null) {
            for (String path : files) {
               File f = new File(file, path);
               deleteFolder(f);
            }
         }
      }
      file.delete();
   }

}
