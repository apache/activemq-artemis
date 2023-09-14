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
package org.apache.activemq.artemis.utils;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.activemq.artemis.logs.ActiveMQUtilLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

public class FileUtil {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static void makeExec(File file) throws IOException {
      try {
         Files.setPosixFilePermissions(file.toPath(), new HashSet<>(Arrays.asList(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE, GROUP_READ, GROUP_WRITE, GROUP_EXECUTE, OTHERS_READ, OTHERS_EXECUTE)));
      } catch (Throwable ignore) {
         // Our best effort was not good enough :)
      }
   }

   public static final boolean deleteDirectory(final File directory) {
      if (directory.isDirectory()) {
         String[] files = directory.list();
         int num = 5;
         int attempts = 0;
         while (files == null && (attempts < num)) {
            try {
               Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            files = directory.list();
            attempts++;
         }

         if (files == null) {
            ActiveMQUtilLogger.LOGGER.failedListFilesToCleanup(directory.getAbsolutePath());
         } else {
            for (String file : files) {
               File f = new File(directory, file);
               if (!deleteDirectory(f)) {
                  ActiveMQUtilLogger.LOGGER.failedToCleanupFile(f.getAbsolutePath());
               }
            }
         }
      }

      return directory.delete();
   }

   public static final void copyDirectory(final File directorySource, final File directoryTarget) throws Exception {
      Path sourcePath = directorySource.toPath();
      Path targetPath = directoryTarget.toPath();

      try {
         Files.walkFileTree(sourcePath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
               Path targetDir = targetPath.resolve(sourcePath.relativize(dir));
               Files.createDirectories(targetDir);
               return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
               Files.copy(file, targetPath.resolve(sourcePath.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
               return FileVisitResult.CONTINUE;
            }
         });
      } catch (IOException e) {
         e.printStackTrace();
      }
   }
}
