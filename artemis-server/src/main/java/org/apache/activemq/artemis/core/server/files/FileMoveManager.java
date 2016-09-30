/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.files;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.jboss.logging.Logger;

/**
 * Used to move files away.
 * Each time a backup starts its formeter data will be moved to a backup folder called bkp.1, bkp.2, ... etc
 * We may control the maximum number of folders so we remove old ones.
 */
public class FileMoveManager {

   private static final Logger logger = Logger.getLogger(FileMoveManager.class);

   private final File folder;
   private int maxFolders;
   public static final String PREFIX = "oldreplica.";

   private static final FilenameFilter isPrefix = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
         boolean prefixed = name.contains(PREFIX);

         if (prefixed) {
            try {
               Integer.parseInt(name.substring(PREFIX.length()));
            } catch (NumberFormatException e) {
               // This function is not really used a lot
               // so I don't really mind about performance here
               // this is good enough for what we need
               prefixed = false;
            }
         }

         return prefixed;
      }
   };

   private static final FilenameFilter notPrefix = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
         return !isPrefix.accept(dir, name);
      }
   };

   public FileMoveManager(File folder) {
      this(folder, -1);
   }

   public FileMoveManager(File folder, int maxFolders) {
      this.folder = folder;
      this.maxFolders = maxFolders;
   }

   public int getMaxFolders() {
      return maxFolders;
   }

   public FileMoveManager setMaxFolders(int maxFolders) {
      this.maxFolders = maxFolders;
      return this;
   }

   public void doMove() throws IOException {
      String[] files = getFiles();

      if (files == null || files.length == 0) {
         // if no files, nothing to be done, no backup, no deletes... nothing!
         return;
      }

      // Since we will create one folder, we are already taking that one into consideration
      internalCheckOldFolders(1);

      int whereToMove = getMaxID() + 1;

      if (maxFolders == 0) {
         ActiveMQServerLogger.LOGGER.backupDeletingData(folder.getPath());
         for (String fileMove : files) {
            File fileFrom = new File(folder, fileMove);
            logger.tracef("deleting %s", fileFrom);
            deleteTree(fileFrom);
         }
      } else {
         File folderTo = getFolder(whereToMove);
         folderTo.mkdirs();

         ActiveMQServerLogger.LOGGER.backupMovingDataAway(folder.getPath(), folderTo.getPath());

         for (String fileMove : files) {
            File fileFrom = new File(folder, fileMove);
            File fileTo = new File(folderTo, fileMove);
            logger.tracef("doMove:: moving %s as %s", fileFrom, fileTo);
            Files.move(fileFrom.toPath(), fileTo.toPath());
         }
      }

   }

   public void checkOldFolders() {
      internalCheckOldFolders(0);
   }

   private void internalCheckOldFolders(int creating) {
      if (maxFolders >= 0) {
         int folders = getNumberOfFolders();

         if (folders == 0) {
            // no folders.. nothing to be done
            return;
         }

         // We are counting the next one to be created
         int foldersToDelete = folders + creating - maxFolders;

         if (foldersToDelete > 0) {
            logger.tracef("There are %d folders to delete", foldersToDelete);
            int[] ids = getIDlist();
            for (int i = 0; i < foldersToDelete; i++) {
               File file = getFolder(ids[i]);
               ActiveMQServerLogger.LOGGER.removingBackupData(file.getPath());
               deleteTree(file);
            }
         }
      }
   }

   /**
    * It will return non backup folders
    */
   public String[] getFiles() {
      return folder.list(notPrefix);
   }

   public int getNumberOfFolders() {
      return getFolders().length;
   }

   public String[] getFolders() {
      String[] list = folder.list(isPrefix);

      if (list == null) {
         list = new String[0];
      }

      return list;
   }

   public int getMinID() {
      int[] list = getIDlist();

      if (list.length == 0) {
         return 0;
      }

      return list[0];
   }

   public int getMaxID() {
      int[] list = getIDlist();

      if (list.length == 0) {
         return 0;
      }

      return list[list.length - 1];
   }

   public int[] getIDlist() {
      String[] list = getFolders();
      int[] ids = new int[list.length];
      for (int i = 0; i < ids.length; i++) {
         ids[i] = getID(list[i]);
      }

      Arrays.sort(ids);

      return ids;
   }

   public int getID(String folderName) {
      return Integer.parseInt(folderName.substring(PREFIX.length()));
   }

   public File getFolder(int id) {
      return new File(folder, PREFIX + id);
   }

   private void deleteTree(File file) {
      File[] files = file.listFiles();

      if (files != null) {
         for (File fileDelete : files) {
            deleteTree(fileDelete);
         }
      }

      file.delete();
   }

}
