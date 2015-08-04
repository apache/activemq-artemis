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
package org.apache.activemq.artemis.core.io.aio;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNativeIOError;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.AbstractSequentialFile;
import org.apache.activemq.artemis.core.io.DummyCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.journal.impl.SimpleWaitIOCallback;
import org.apache.activemq.artemis.jlibaio.LibaioFile;
import org.apache.activemq.artemis.utils.ReusableLatch;

public class AIOSequentialFile extends AbstractSequentialFile {

   private boolean opened = false;

   private LibaioFile aioFile;

   private final AIOSequentialFileFactory aioFactory;

   private final ReusableLatch pendingCallbacks = new ReusableLatch();

   /**
    * Used to determine the next writing sequence
    */
   private final AtomicLong nextWritingSequence = new AtomicLong(0);

   /**
    * AIO can't guarantee ordering over callbacks.
    * <br>
    * We use this {@link PriorityQueue} to hold values until they are in order
    */
   final PriorityQueue<AIOSequentialFileFactory.AIOSequentialCallback> pendingCallbackList = new PriorityQueue<>();

   /**
    * Used to determine the next writing sequence.
    * This is accessed from a single thread (the Poller Thread)
    */
   private long nextReadSequence = 0;

   public AIOSequentialFile(final AIOSequentialFileFactory factory,
                            final int bufferSize,
                            final long bufferTimeoutMilliseconds,
                            final File directory,
                            final String fileName,
                            final Executor writerExecutor) {
      super(directory, fileName, factory, writerExecutor);
      this.aioFactory = factory;
   }

   public boolean isOpen() {
      return opened;
   }

   public int getAlignment() {
      checkOpened();

      return aioFile.getBlockSize();
   }

   public int calculateBlockStart(final int position) {
      int alignment = getAlignment();

      int pos = (position / alignment + (position % alignment != 0 ? 1 : 0)) * alignment;

      return pos;
   }

   public SequentialFile cloneFile() {
      return new AIOSequentialFile(aioFactory, -1, -1, getFile().getParentFile(), getFile().getName(), writerExecutor);
   }

   @Override
   public synchronized void close() throws IOException, InterruptedException, ActiveMQException {
      if (!opened) {
         return;
      }

      super.close();

      if (!pendingCallbacks.await(10, TimeUnit.SECONDS)) {
         factory.onIOError(new IOException("Timeout on close"), "Timeout on close", this);
      }

      opened = false;

      timedBuffer = null;

      aioFile.close();
      aioFile = null;
   }

   public synchronized void fill(final int size) throws Exception {
      checkOpened();
      aioFile.fill(size);

      fileSize = aioFile.getSize();
   }

   public void open() throws Exception {
      open(aioFactory.getMaxIO(), true);
   }

   public synchronized void open(final int maxIO, final boolean useExecutor) throws ActiveMQException {
      opened = true;

      try {
         aioFile = aioFactory.libaioContext.openFile(getFile(), true);
      }
      catch (IOException e) {
         factory.onIOError(e, e.getMessage(), this);
         throw new ActiveMQNativeIOError(e.getMessage(), e);
      }

      position.set(0);

      fileSize = aioFile.getSize();
   }

   public int read(final ByteBuffer bytes, final IOCallback callback) throws ActiveMQException {
      checkOpened();
      int bytesToRead = bytes.limit();

      long positionToRead = position.getAndAdd(bytesToRead);

      bytes.rewind();

      try {
         // We don't send the buffer to the callback on read,
         // because we want the buffer available.
         // Sending it through the callback would make it released
         aioFile.read(positionToRead, bytesToRead, bytes, getCallback(callback, null));
      }
      catch (IOException e) {
         factory.onIOError(e, e.getMessage(), this);
         throw new ActiveMQNativeIOError(e.getMessage(), e);
      }

      return bytesToRead;
   }

   public int read(final ByteBuffer bytes) throws Exception {
      SimpleWaitIOCallback waitCompletion = new SimpleWaitIOCallback();

      int bytesRead = read(bytes, waitCompletion);

      waitCompletion.waitCompletion();

      return bytesRead;
   }

   public void writeDirect(final ByteBuffer bytes, final boolean sync) throws Exception {
      if (sync) {
         SimpleWaitIOCallback completion = new SimpleWaitIOCallback();

         writeDirect(bytes, true, completion);

         completion.waitCompletion();
      }
      else {
         writeDirect(bytes, false, DummyCallback.getInstance());
      }
   }

   /**
    * Note: Parameter sync is not used on AIO
    */
   public void writeDirect(final ByteBuffer bytes, final boolean sync, final IOCallback callback) {
      checkOpened();

      final int bytesToWrite = factory.calculateBlockSize(bytes.limit());

      final long positionToWrite = position.getAndAdd(bytesToWrite);

      AIOSequentialFileFactory.AIOSequentialCallback runnableCallback = getCallback(callback, bytes);
      runnableCallback.initWrite(positionToWrite, bytesToWrite);
      if (writerExecutor != null) {
         writerExecutor.execute(runnableCallback);
      }
      else {
         runnableCallback.run();
      }
   }

   AIOSequentialFileFactory.AIOSequentialCallback getCallback(IOCallback originalCallback, ByteBuffer buffer) {
      AIOSequentialFileFactory.AIOSequentialCallback callback = aioFactory.getCallback();
      callback.init(this.nextWritingSequence.getAndIncrement(), originalCallback, aioFile, this, buffer);
      pendingCallbacks.countUp();
      return callback;
   }

   void done(AIOSequentialFileFactory.AIOSequentialCallback callback) {
      if (callback.writeSequence == -1) {
         callback.sequentialDone();
         pendingCallbacks.countDown();
      }

      if (callback.writeSequence == nextReadSequence) {
         nextReadSequence++;
         callback.sequentialDone();
         pendingCallbacks.countDown();
         flushCallbacks();
      }
      else {
         pendingCallbackList.add(callback);
      }

   }

   private void flushCallbacks() {
      while (!pendingCallbackList.isEmpty() && pendingCallbackList.peek().writeSequence == nextReadSequence) {
         AIOSequentialFileFactory.AIOSequentialCallback callback = pendingCallbackList.poll();
         callback.sequentialDone();
         nextReadSequence++;
         pendingCallbacks.countDown();
      }
   }

   public void sync() {
      throw new UnsupportedOperationException("This method is not supported on AIO");
   }

   public long size() throws Exception {
      if (aioFile == null) {
         return getFile().length();
      }
      else {
         return aioFile.getSize();
      }
   }

   @Override
   public String toString() {
      return "AIOSequentialFile:" + getFile().getAbsolutePath();
   }

   // Protected methods
   // -----------------------------------------------------------------------------------------------------

   @Override
   protected ByteBuffer newBuffer(int size, int limit) {
      size = factory.calculateBlockSize(size);
      limit = factory.calculateBlockSize(limit);

      ByteBuffer buffer = factory.newBuffer(size);
      buffer.limit(limit);
      return buffer;
   }

   // Private methods
   // -----------------------------------------------------------------------------------------------------

   private void checkOpened() {
      if (aioFile == null || !opened) {
         throw new NullPointerException("File not opened, file=null");
      }
   }

}
