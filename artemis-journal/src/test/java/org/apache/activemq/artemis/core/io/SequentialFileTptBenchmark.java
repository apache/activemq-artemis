/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.io;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.ArtemisConstants;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.io.mapped.MappedSequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.jlibaio.LibaioContext;

/**
 * To benchmark Type.Aio you need to define -Djava.library.path=${project-root}/native/src/.libs when calling the JVM
 */
public class SequentialFileTptBenchmark {

   private static final FastWaitIOCallback CALLBACK = new FastWaitIOCallback();

   public static void main(String[] args) throws Exception {
      final boolean dataSync = false;
      final boolean writeSync = true;
      final Type type = Type.Mapped;
      final int tests = 10;
      final int warmup = 20_000;
      final int measurements = 100_000;
      final int msgSize = 100;
      final byte[] msgContent = new byte[msgSize];
      Arrays.fill(msgContent, (byte) 1);
      final File tmpDirectory = new File("./");
      //using the default configuration when the broker starts!
      final SequentialFileFactory factory;
      switch (type) {

         case Mapped:
            final int fileSize = Math.max(msgSize * measurements, msgSize * warmup);
            factory = new MappedSequentialFileFactory(tmpDirectory, fileSize, true, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, null).setDatasync(dataSync);
            break;
         case Nio:
            factory = new NIOSequentialFileFactory(tmpDirectory, true, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_NIO, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO, 1, false, null, null).setDatasync(dataSync);
            break;
         case Aio:
            factory = new AIOSequentialFileFactory(tmpDirectory, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, ArtemisConstants.DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO, 500, false, null, null).setDatasync(dataSync);
            //disable it when using directly the same buffer: ((AIOSequentialFileFactory)factory).disableBufferReuse();
            if (!LibaioContext.isLoaded()) {
               throw new IllegalStateException("lib AIO not loaded!");
            }
            break;
         default:
            throw new AssertionError("unsupported case");
      }
      factory.start();
      try {
         final EncodingSupport encodingSupport = new EncodingSupport() {
            @Override
            public int getEncodeSize() {
               return msgSize;
            }

            @Override
            public void encode(ActiveMQBuffer buffer) {
               final int writerIndex = buffer.writerIndex();
               buffer.setBytes(writerIndex, msgContent);
               buffer.writerIndex(writerIndex + msgSize);
            }

            @Override
            public void decode(ActiveMQBuffer buffer) {

            }
         };
         final int alignedMessageSize = factory.calculateBlockSize(msgSize);
         final long totalFileSize = Math.max(alignedMessageSize * measurements, alignedMessageSize * warmup);
         if (totalFileSize > Integer.MAX_VALUE)
            throw new IllegalArgumentException("reduce measurements/warmup");
         final int fileSize = (int) totalFileSize;
         final SequentialFile sequentialFile = factory.createSequentialFile("seq.dat");
         sequentialFile.getJavaFile().delete();
         sequentialFile.getJavaFile().deleteOnExit();
         sequentialFile.open();
         final long startZeros = System.nanoTime();
         sequentialFile.fill(fileSize);
         final long elapsedZeros = System.nanoTime() - startZeros;
         System.out.println("Zeroed " + fileSize + " bytes in " + TimeUnit.NANOSECONDS.toMicros(elapsedZeros) + " us");
         try {
            {
               final long elapsed = writeMeasurements(factory, sequentialFile, encodingSupport, warmup, writeSync);
               System.out.println("warmup:" + (measurements * 1000_000_000L) / elapsed + " ops/sec");
            }
            for (int t = 0; t < tests; t++) {
               final long elapsed = writeMeasurements(factory, sequentialFile, encodingSupport, measurements, writeSync);
               System.out.println((measurements * 1000_000_000L) / elapsed + " ops/sec");
            }
         } finally {
            sequentialFile.close();
         }
      } finally {
         factory.stop();
      }
   }

   private static long writeMeasurements(SequentialFileFactory sequentialFileFactory,
                                         SequentialFile sequentialFile,
                                         EncodingSupport encodingSupport,
                                         int measurements,
                                         boolean writeSync) throws Exception {
      //System.gc();
      TimeUnit.SECONDS.sleep(2);
      sequentialFileFactory.activateBuffer(sequentialFile);
      sequentialFile.position(0);
      final long start = System.nanoTime();
      for (int i = 0; i < measurements; i++) {
         write(sequentialFile, encodingSupport, writeSync);
      }
      sequentialFileFactory.deactivateBuffer();
      final long elapsed = System.nanoTime() - start;
      return elapsed;
   }

   private static void write(SequentialFile sequentialFile,
                             EncodingSupport encodingSupport,
                             boolean sync) throws Exception {
      //this pattern is necessary to ensure that NIO's TimedBuffer fill flush the buffer and know the real size of it
      if (sequentialFile.fits(encodingSupport.getEncodeSize())) {
         CALLBACK.reset();
         sequentialFile.write(encodingSupport, sync, CALLBACK);
         CALLBACK.waitCompletion();
      } else {
         throw new IllegalStateException("can't happen!");
      }
   }

   private enum Type {

      Mapped, Nio, Aio

   }

   private static final class FastWaitIOCallback implements IOCallback {

      private final AtomicBoolean done = new AtomicBoolean(false);
      private int errorCode = 0;
      private String errorMessage = null;

      public FastWaitIOCallback reset() {
         errorCode = 0;
         errorMessage = null;
         done.lazySet(false);
         return this;
      }

      @Override
      public void done() {
         errorCode = 0;
         errorMessage = null;
         done.lazySet(true);
      }

      @Override
      public void onError(int errorCode, String errorMessage) {
         this.errorCode = errorCode;
         this.errorMessage = errorMessage;
         done.lazySet(true);
      }

      public void waitCompletion() throws InterruptedException, ActiveMQException {
         while (!done.get()) {
         }
         if (errorMessage != null) {
            throw ActiveMQExceptionType.createException(errorCode, errorMessage);
         }
      }
   }
}
