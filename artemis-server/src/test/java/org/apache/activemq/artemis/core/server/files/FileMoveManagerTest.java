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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreFactoryNIO;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileMoveManagerTest {

   @Rule
   public TemporaryFolder temporaryFolder;

   @Rule
   public ThreadLeakCheckRule leakCheckRule = new ThreadLeakCheckRule();

   private File dataLocation;
   private FileMoveManager manager;

   @Before
   public void setUp() {
      dataLocation = new File(temporaryFolder.getRoot(), "data");
      dataLocation.mkdirs();
      manager = new FileMoveManager(dataLocation, 10);
   }

   public FileMoveManagerTest() {
      File parent = new File("./target/tmp");
      parent.mkdirs();
      temporaryFolder = new TemporaryFolder(parent);
   }

   @Test
   public void testBackupFiles() {
      int[] originalFiles = new int[12];
      int count = 0;

      // It will fake folders creation
      for (int i = 0; i < 12; i++) {
         originalFiles[count++] = i;
         File bkp = new File(dataLocation, FileMoveManager.PREFIX + i);
         bkp.mkdirs();
      }

      Assert.assertEquals(12, manager.getFolders().length);
      Assert.assertEquals(12, manager.getNumberOfFolders());

      assertIDs(originalFiles, manager.getIDlist());
   }

   @Test
   public void testMinMax() {
      int[] originalFiles = new int[12];
      int count = 0;

      // It will fake folders creation
      for (int i = 0; i < 5; i++) {
         originalFiles[count++] = i;
         File bkp = new File(dataLocation, FileMoveManager.PREFIX + i);
         bkp.mkdirs();
      }

      // simulates a hole where someone removed a folder by hand

      // It will fake folders creation
      for (int i = 7; i < 14; i++) {
         originalFiles[count++] = i;
         File bkp = new File(dataLocation, FileMoveManager.PREFIX + i);
         bkp.mkdirs();
      }

      Assert.assertEquals(12, manager.getFolders().length);
      Assert.assertEquals(12, manager.getNumberOfFolders());

      int[] ids = manager.getIDlist();

      assertIDs(originalFiles, ids);

      Assert.assertEquals(0, manager.getMinID());
      Assert.assertEquals(13, manager.getMaxID());

      manager.setMaxFolders(3).checkOldFolders();

      Assert.assertEquals(3, manager.getNumberOfFolders());
      Assert.assertEquals(13, manager.getMaxID());
      Assert.assertEquals(11, manager.getMinID());

   }

   @Test
   public void testGarbageCreated() {
      // I'm pretending an admin created a folder here
      File garbage = new File(dataLocation, "bkp.zzz");
      garbage.mkdirs();

      testMinMax();

      resetTmp();
      // the admin renamed a folder maybe
      garbage = new File(dataLocation, "bkp.001.old");
      garbage.mkdirs();

      resetTmp();

      // the admin renamed a folder maybe
      garbage = new File(dataLocation, "bkp.1.5");
      garbage.mkdirs();

      testMinMax();
   }

   @Test
   public void testNoFolders() {
      Assert.assertEquals(0, manager.getFolders().length);
      Assert.assertEquals(0, manager.getNumberOfFolders());

      Assert.assertTrue(dataLocation.delete());

      Assert.assertEquals(0, manager.getFolders().length);
      Assert.assertEquals(0, manager.getNumberOfFolders());
   }

   @Test
   public void testNoFiles() throws Exception {
      // nothing to be moved, so why to do a backup
      manager.doMove();

      Assert.assertEquals(0, manager.getNumberOfFolders());
   }

   @Test
   public void testMoveFiles() throws Exception {
      manager.setMaxFolders(3);

      for (int bkp = 1; bkp <= 10; bkp++) {
         for (int i = 0; i < 100; i++) {
            createFile(dataLocation, i);
         }

         manager.doMove();

         // We will always have maximum of 3 folders
         Assert.assertEquals(Math.min(bkp, manager.getMaxFolders()), manager.getNumberOfFolders());

         File bkpFolder = manager.getFolder(bkp);

         FileMoveManager bkp1Manager = new FileMoveManager(bkpFolder, 10);
         String[] filesAfterMove = bkp1Manager.getFiles();

         for (String file : filesAfterMove) {
            checkFile(bkpFolder, file);
         }
      }

      Assert.assertEquals(manager.getMaxFolders(), manager.getNumberOfFolders());

      manager.setMaxFolders(-1).checkOldFolders();

      Assert.assertEquals(3, manager.getNumberOfFolders());

      manager.setMaxFolders(1).checkOldFolders();
      Assert.assertEquals(1, manager.getNumberOfFolders());

      Assert.assertEquals(10, manager.getMaxID());
      Assert.assertEquals(10, manager.getMinID());
   }

   @Test
   public void testMoveFolders() throws Exception {
      manager.setMaxFolders(3);

      int NUMBER_OF_FOLDERS = 10;
      int FILES_PER_FOLDER = 10;

      for (int bkp = 1; bkp <= 10; bkp++) {
         for (int f = 0; f < NUMBER_OF_FOLDERS; f++) {
            File folderF = new File(dataLocation, "folder" + f);
            folderF.mkdirs();

            // FILES_PER_FOLDER + f, I'm just creating more files as f grows.
            // this is just to make each folder unique somehow
            for (int i = 0; i < FILES_PER_FOLDER + f; i++) {
               createFile(folderF, i);
            }
         }

         manager.doMove();

         // We will always have maximum of 3 folders
         Assert.assertEquals(Math.min(bkp, manager.getMaxFolders()), manager.getNumberOfFolders());

         File bkpFolder = manager.getFolder(bkp);

         for (int f = 0; f < NUMBER_OF_FOLDERS; f++) {
            File fileTmp = new File(bkpFolder, "folder" + f);

            String[] filesOnFolder = fileTmp.list();

            Assert.assertEquals(FILES_PER_FOLDER + f, filesOnFolder.length);

            for (String file : filesOnFolder) {
               checkFile(fileTmp, file);
            }
         }

      }

      Assert.assertEquals(manager.getMaxFolders(), manager.getNumberOfFolders());

      manager.setMaxFolders(-1).checkOldFolders();

      Assert.assertEquals(3, manager.getNumberOfFolders());

      manager.setMaxFolders(1).checkOldFolders();
      Assert.assertEquals(1, manager.getNumberOfFolders());

      Assert.assertEquals(10, manager.getMaxID());
      Assert.assertEquals(10, manager.getMinID());
   }

   @Test
   public void testMaxZero() throws Exception {
      manager.setMaxFolders(0);

      int NUMBER_OF_FOLDERS = 10;
      int FILES_PER_FOLDER = 10;

      for (int bkp = 1; bkp <= 10; bkp++) {
         for (int f = 0; f < NUMBER_OF_FOLDERS; f++) {
            File folderF = new File(dataLocation, "folder" + f);
            folderF.mkdirs();

            // FILES_PER_FOLDER + f, I'm just creating more files as f grows.
            // this is just to make each folder unique somehow
            for (int i = 0; i < FILES_PER_FOLDER + f; i++) {
               createFile(folderF, i);
            }
         }

         manager.doMove();

         // We will always have maximum of 3 folders
         Assert.assertEquals(0, manager.getNumberOfFolders());
      }

      Assert.assertEquals(0, manager.getMaxID());
   }

   @Test
   public void testMoveOverPaging() throws Exception {
      AssertionLoggerHandler.startCapture();

      ExecutorService threadPool = Executors.newCachedThreadPool();
      try {
         manager.setMaxFolders(3);
         for (int i = 1; i <= 10; i++) {
            HierarchicalRepository<AddressSettings> addressSettings = new HierarchicalObjectRepository<>();
            AddressSettings settings = new AddressSettings();
            settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
            addressSettings.setDefault(settings);

            final StorageManager storageManager = new NullStorageManager();

            PagingStoreFactoryNIO storeFactory = new PagingStoreFactoryNIO(storageManager, dataLocation, 100, null, new OrderedExecutorFactory(threadPool), true, null);

            PagingManagerImpl managerImpl = new PagingManagerImpl(storeFactory, addressSettings, -1);

            managerImpl.start();

            PagingStore store = managerImpl.getPageStore(new SimpleString("simple-test"));

            store.startPaging();

            store.stop();

            managerImpl.stop();

            manager.doMove();

            Assert.assertEquals(Math.min(i, manager.getMaxFolders()), manager.getNumberOfFolders());
         }

         Assert.assertFalse("The loggers are complaining about address.txt", AssertionLoggerHandler.findText("address.txt"));
      } finally {
         AssertionLoggerHandler.stopCapture();
         threadPool.shutdown();
      }

   }

   private void assertIDs(int[] originalFiles, int[] ids) {
      Assert.assertEquals(originalFiles.length, ids.length);
      for (int i = 0; i < ids.length; i++) {
         Assert.assertEquals(originalFiles[i], ids[i]);
      }
   }

   private void resetTmp() {
      temporaryFolder.delete();
      temporaryFolder.getRoot().mkdirs();
      Assert.assertEquals(0, manager.getNumberOfFolders());
   }

   private void createFile(File folder, int i) throws FileNotFoundException {
      File dataFile = new File(folder, i + ".jrn");
      PrintWriter outData = new PrintWriter(new FileOutputStream(dataFile));
      outData.print(i);
      outData.close();
   }

   private void checkFile(File bkpFolder, String file) throws IOException {
      File fileRead = new File(bkpFolder, file);
      InputStreamReader stream = new InputStreamReader(new FileInputStream(fileRead));
      BufferedReader reader = new BufferedReader(stream);
      String valueRead = reader.readLine();
      int id = Integer.parseInt(file.substring(0, file.indexOf('.')));
      Assert.assertEquals("content of the file wasn't the expected", id, Integer.parseInt(valueRead));
   }

}
