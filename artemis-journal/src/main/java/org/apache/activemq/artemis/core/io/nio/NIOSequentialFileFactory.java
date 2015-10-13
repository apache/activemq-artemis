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
package org.apache.activemq.artemis.core.io.nio;

import java.io.File;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;

import org.apache.activemq.artemis.ArtemisConstants;
import org.apache.activemq.artemis.core.io.AbstractSequentialFileFactory;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;

public class NIOSequentialFileFactory extends AbstractSequentialFileFactory {

   public NIOSequentialFileFactory(final File journalDir, final int maxIO) {
      this(journalDir, null, maxIO);
   }

   public NIOSequentialFileFactory(final File journalDir, final IOCriticalErrorListener listener, final int maxIO) {
      this(journalDir, false, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO, maxIO, false, listener);
   }

   public NIOSequentialFileFactory(final File journalDir, final boolean buffered, final int maxIO) {
      this(journalDir, buffered, null, maxIO);
   }

   public NIOSequentialFileFactory(final File journalDir,
                                   final boolean buffered,
                                   final IOCriticalErrorListener listener,
                                   final int maxIO) {
      this(journalDir, buffered, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO, maxIO, false, listener);
   }

   public NIOSequentialFileFactory(final File journalDir,
                                   final boolean buffered,
                                   final int bufferSize,
                                   final int bufferTimeout,
                                   final int maxIO,
                                   final boolean logRates) {
      this(journalDir, buffered, bufferSize, bufferTimeout, maxIO, logRates, null);
   }

   public NIOSequentialFileFactory(final File journalDir,
                                   final boolean buffered,
                                   final int bufferSize,
                                   final int bufferTimeout,
                                   final int maxIO,
                                   final boolean logRates,
                                   final IOCriticalErrorListener listener) {
      super(journalDir, buffered, bufferSize, bufferTimeout, maxIO, logRates, listener);
   }

   public SequentialFile createSequentialFile(final String fileName) {
      return new NIOSequentialFile(this, journalDir, fileName, maxIO, writeExecutor);
   }

   public boolean isSupportsCallbacks() {
      return timedBuffer != null;
   }

   public ByteBuffer allocateDirectBuffer(final int size) {
      // Using direct buffer, as described on https://jira.jboss.org/browse/HORNETQ-467
      ByteBuffer buffer2 = null;
      try {
         buffer2 = ByteBuffer.allocateDirect(size);
      }
      catch (OutOfMemoryError error) {
         // This is a workaround for the way the JDK will deal with native buffers.
         // the main portion is outside of the VM heap
         // and the JDK will not have any reference about it to take GC into account
         // so we force a GC and try again.
         WeakReference<Object> obj = new WeakReference<Object>(new Object());
         try {
            long timeout = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() > timeout && obj.get() != null) {
               System.gc();
               Thread.sleep(100);
            }
         }
         catch (InterruptedException e) {
         }

         buffer2 = ByteBuffer.allocateDirect(size);

      }
      return buffer2;
   }

   public void releaseDirectBuffer(ByteBuffer buffer) {
      // nothing we can do on this case. we can just have good faith on GC
   }

   public ByteBuffer newBuffer(final int size) {
      return ByteBuffer.allocate(size);
   }

   public void clearBuffer(final ByteBuffer buffer) {
      final int limit = buffer.limit();
      buffer.rewind();

      for (int i = 0; i < limit; i++) {
         buffer.put((byte) 0);
      }

      buffer.rewind();
   }

   public ByteBuffer wrapBuffer(final byte[] bytes) {
      return ByteBuffer.wrap(bytes);
   }

   public int getAlignment() {
      return 1;
   }

   public int calculateBlockSize(final int bytes) {
      return bytes;
   }

}
