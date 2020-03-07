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
package org.apache.activemq.artemis.core.io;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;

/**
 * A SequentialFileFactory
 */
public interface SequentialFileFactory {

   default CriticalAnalyzer getCriticalAnalyzer() {
      return null;
   }

   SequentialFile createSequentialFile(String fileName);

   int getMaxIO();

   /**
    * Lists files that end with the given extension.
    * <p>
    * This method inserts a ".' before the extension.
    *
    * @param extension
    * @return
    * @throws Exception
    */
   List<String> listFiles(String extension) throws Exception;

   boolean isSupportsCallbacks();

   /**
    * The SequentialFile will call this method when a disk IO Error happens during the live phase.
    */
   void onIOError(Exception exception, String message, SequentialFile file);

   /**
    * used for cases where you need direct buffer outside of the journal context.
    * This is because the native layer has a method that can be reused in certain cases like paging
    */
   ByteBuffer allocateDirectBuffer(int size);

   /**
    * used for cases where you need direct buffer outside of the journal context.
    * This is because the native layer has a method that can be reused in certain cases like paging
    */
   void releaseDirectBuffer(ByteBuffer buffer);

   /**
    * Note: You need to release the buffer if is used for reading operations. You don't need to do
    * it if using writing operations (AIO Buffer Lister will take of writing operations)
    *
    * @param size
    * @return the allocated ByteBuffer
    */
   ByteBuffer newBuffer(int size);

   void releaseBuffer(ByteBuffer buffer);

   void activateBuffer(SequentialFile file);

   void deactivateBuffer();

   // To be used in tests only
   ByteBuffer wrapBuffer(byte[] bytes);

   int getAlignment();

   SequentialFileFactory setAlignment(int alignment);

   int calculateBlockSize(int bytes);

   File getDirectory();

   void clearBuffer(ByteBuffer buffer);

   void start();

   void stop();

   /**
    * Creates the directory if it does not exist yet.
    */
   void createDirs() throws Exception;

   void flush();

   SequentialFileFactory setDatasync(boolean enabled);

   boolean isDatasync();

   long getBufferSize();
}
