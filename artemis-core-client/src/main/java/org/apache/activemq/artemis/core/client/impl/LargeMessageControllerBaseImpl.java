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
package org.apache.activemq.artemis.core.client.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.api.core.ActiveMQLargeMessageInterruptedException;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;

/**
 * This class aggregates several {@link org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage}
 * as it was being handled
 * by a single buffer. This buffer can be consumed as messages are arriving, and it will hold the
 * packets until they are read using the ChannelBuffer interface, or the setOutputStream or
 * saveStream are called.
 */
abstract class LargeMessageControllerBaseImpl implements LargeMessageController {

   private final ClientConsumerInternal consumerInternal;

   private final LinkedBlockingQueue<LargeData> largeMessageData = new LinkedBlockingQueue<>();

   private volatile LargeData currentPacket = null;

   private final long totalSize;

   private boolean streamEnded = false;

   private boolean streamClosed = false;

   private final long readTimeout;

   /**
    * This is to control if packets are arriving for a better timeout control
    */
   private boolean packetAdded = false;

   private long packetPosition = -1;

   private long lastIndex = 0;

   private long packetLastPosition = -1;

   private OutputStream outStream;

   // There's no need to wait a synchronization
   // we just set the exception and let other threads to get it as soon as possible
   private volatile Exception handledException;

   private final FileCache fileCache;

   private boolean local = false;
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   protected LargeMessageControllerBaseImpl(final ClientConsumerInternal consumerInternal,
                                         final long totalSize,
                                         final long readTimeout) {
      this(consumerInternal, totalSize, readTimeout, null);
   }

   protected LargeMessageControllerBaseImpl(final ClientConsumerInternal consumerInternal,
                                         final long totalSize,
                                         final long readTimeout,
                                         final File cachedFile) {
      this(consumerInternal, totalSize, readTimeout, cachedFile, 10 * 1024);
   }

   protected LargeMessageControllerBaseImpl(final ClientConsumerInternal consumerInternal,
                                         final long totalSize,
                                         final long readTimeout,
                                         final File cachedFile,
                                         final int bufferSize) {
      this.consumerInternal = consumerInternal;
      this.readTimeout = readTimeout;
      this.totalSize = totalSize;
      if (cachedFile == null) {
         fileCache = null;
      } else {
         fileCache = new FileCache(cachedFile, bufferSize);
      }
   }

   // Public --------------------------------------------------------

   public void setLocal(boolean local) {
      this.local = local;
   }

   @Override
   public void discardUnusedPackets() {
      if (outStream == null) {
         if (local)
            return;
         try {
            checkForPacket(totalSize - 1);
         } catch (Throwable ignored) {
         }
      }
   }

   /**
    * TODO: move this to ConsumerContext as large message is a protocol specific thing
    * Add a buff to the List, or save it to the OutputStream if set
    */
   @Override
   public void addPacket(byte[] chunk, int flowControlSize, boolean isContinues) {
      int flowControlCredit = 0;

      synchronized (this) {
         packetAdded = true;
         if (outStream != null) {
            try {
               if (!isContinues) {
                  streamEnded = true;
               }

               if (fileCache != null) {
                  fileCache.cachePackage(chunk);
               }

               outStream.write(chunk);

               flowControlCredit = flowControlSize;

               notifyAll();

               if (streamEnded) {
                  outStream.close();
               }
            } catch (Exception e) {
               ActiveMQClientLogger.LOGGER.errorAddingPacket(e);
               handledException = e;
            }
         } else {
            if (fileCache != null) {
               try {
                  fileCache.cachePackage(chunk);
               } catch (Exception e) {
                  ActiveMQClientLogger.LOGGER.errorAddingPacket(e);
                  handledException = e;
               }
            }

            largeMessageData.offer(new LargeData(chunk, flowControlSize, isContinues));
         }
      }

      if (flowControlCredit != 0) {
         try {
            consumerInternal.flowControl(flowControlCredit, !isContinues);
         } catch (Exception e) {
            ActiveMQClientLogger.LOGGER.errorAddingPacket(e);
            handledException = e;
         }
      }
   }

   @Override
   public void cancel() {
      this.handledException = ActiveMQClientMessageBundle.BUNDLE.largeMessageInterrupted();

      synchronized (this) {
         int totalSize = 0;
         LargeData polledPacket = null;
         while ((polledPacket = largeMessageData.poll()) != null) {
            totalSize += polledPacket.getFlowControlSize();
         }

         try {
            consumerInternal.flowControl(totalSize, false);
         } catch (Exception ignored) {
            // what else can we do here?
            ActiveMQClientLogger.LOGGER.errorCallingCancel(ignored);
         }

         largeMessageData.offer(new LargeData());
         streamEnded = true;
         streamClosed = true;

         notifyAll();
      }
   }

   @Override
   public synchronized void close() {
      if (fileCache != null) {
         fileCache.close();
      }
   }

   @Override
   public void setOutputStream(final OutputStream output) throws ActiveMQException {

      int totalFlowControl = 0;
      boolean continues = false;

      synchronized (this) {
         if (currentPacket != null) {
            sendPacketToOutput(output, currentPacket);
            currentPacket = null;
         }
         while (handledException == null) {
            LargeData packet = largeMessageData.poll();
            if (packet == null) {
               break;
            }
            totalFlowControl += packet.getFlowControlSize();

            continues = packet.isContinues();
            sendPacketToOutput(output, packet);
         }

         checkException();
         outStream = output;
      }

      if (totalFlowControl > 0) {
         consumerInternal.flowControl(totalFlowControl, !continues);
      }
   }

   @Override
   public synchronized void saveBuffer(final OutputStream output) throws ActiveMQException {
      if (streamClosed) {
         throw ActiveMQClientMessageBundle.BUNDLE.largeMessageLostSession();
      }
      setOutputStream(output);
      waitCompletion(0);
   }

   /**
    * @param timeWait Milliseconds to Wait. 0 means forever
    * @throws ActiveMQException
    */
   @Override
   public synchronized boolean waitCompletion(final long timeWait) throws ActiveMQException {
      if (outStream == null) {
         // There is no stream.. it will never achieve the end of streaming
         return false;
      }

      long timeOut;

      // If timeWait = 0, we will use the readTimeout
      // And we will check if no packets have arrived within readTimeout milliseconds
      if (timeWait != 0) {
         timeOut = System.currentTimeMillis() + timeWait;
      } else {
         timeOut = System.currentTimeMillis() + readTimeout;
      }

      while (!streamEnded && handledException == null) {
         try {
            this.wait(timeWait == 0 ? readTimeout : timeWait);
         } catch (InterruptedException e) {
            throw new ActiveMQInterruptedException(e);
         }

         if (!streamEnded && handledException == null) {
            if (timeWait != 0 && System.currentTimeMillis() > timeOut) {
               throw ActiveMQClientMessageBundle.BUNDLE.timeoutOnLargeMessage();
            } else if (System.currentTimeMillis() > timeOut && !packetAdded) {
               throw ActiveMQClientMessageBundle.BUNDLE.timeoutOnLargeMessage();
            }
         }
      }

      checkException();

      return streamEnded;

   }

   /**
    * @throws ActiveMQException
    */
   private void checkException() throws ActiveMQException {
      // it's not needed to copy it as we never set it back to null
      // once the exception is set, the controller is pretty much useless
      if (handledException != null) {
         if (handledException instanceof ActiveMQException) {
            ActiveMQException nestedException;

            // This is just to be user friendly and give the user a proper exception trace,
            // instead to just where it was canceled.
            if (handledException instanceof ActiveMQLargeMessageInterruptedException) {
               nestedException = new ActiveMQLargeMessageInterruptedException(handledException.getMessage());
            } else {
               nestedException = new ActiveMQException(((ActiveMQException) handledException).getType(), handledException.getMessage());
            }
            nestedException.initCause(handledException);

            throw nestedException;
         } else {
            throw new ActiveMQException(ActiveMQExceptionType.LARGE_MESSAGE_ERROR_BODY, "Error on saving LargeMessageBufferImpl", handledException);
         }
      }
   }

   @Override
   public long getSize() {
      return totalSize;
   }

   /**
    * @param output
    * @param packet
    * @throws ActiveMQException
    */
   private void sendPacketToOutput(final OutputStream output, final LargeData packet) throws ActiveMQException {
      try {
         output.write(packet.getChunk());
         if (!packet.isContinues()) {
            streamEnded = true;
            output.close();
         }
      } catch (IOException e) {
         throw ActiveMQClientMessageBundle.BUNDLE.errorWritingLargeMessage(e);
      }
   }

   private void popPacket() {
      try {

         if (streamEnded) {
            // no more packets, we are over the last one already
            throw new IndexOutOfBoundsException();
         }

         int sizeToAdd = currentPacket != null ? currentPacket.chunk.length : 1;
         currentPacket = largeMessageData.poll(readTimeout, TimeUnit.MILLISECONDS);
         if (currentPacket == null) {
            throw new IndexOutOfBoundsException();
         }

         if (currentPacket.chunk == null) { // Empty packet as a signal to interruption
            currentPacket = null;
            streamEnded = true;
            throw new IndexOutOfBoundsException();
         }

         consumerInternal.flowControl(currentPacket.getFlowControlSize(), !currentPacket.isContinues());

         packetPosition += sizeToAdd;

         packetLastPosition = packetPosition + currentPacket.getChunk().length;
      } catch (IndexOutOfBoundsException e) {
         throw e;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   protected final void checkForPacket(final long index) {
      if (outStream != null) {
         throw new IllegalAccessError("Can't read the messageBody after setting outputStream");
      }

      if (index >= totalSize) {
         throw new IndexOutOfBoundsException();
      }

      if (streamClosed) {
         throw new IllegalAccessError("The consumer associated with this large message was closed before the body was read");
      }

      if (fileCache == null) {
         if (index < lastIndex) {
            throw new IllegalAccessError("LargeMessage have read-only and one-way buffers");
         }
         lastIndex = index;
      }

      while (index >= packetLastPosition && !streamEnded) {
         popPacket();
      }
   }

   protected final byte getByte(final long index) {
      checkForPacket(index);

      if (fileCache != null && index < packetPosition) {
         return fileCache.getByteFromCache(index);
      } else {
         return currentPacket.getChunk()[(int) (index - packetPosition)];
      }
   }

   private static final class FileCache {

      private FileCache(final File cachedFile, final int bufferSize) {
         this.cachedFile = cachedFile;
         this.bufferSize = bufferSize;
      }

      private ByteBuffer readCache;

      private long readCachePositionStart = Integer.MAX_VALUE;

      private long readCachePositionEnd = -1;

      private final File cachedFile;

      private volatile FileChannel cachedChannel;

      private final int bufferSize;

      private synchronized void readCache(final long position) {

         try {
            if (position < readCachePositionStart || position > readCachePositionEnd) {

               final FileChannel cachedChannel = checkOpen();

               if (position > cachedChannel.size()) {
                  throw new ArrayIndexOutOfBoundsException("position > " + cachedChannel.size());
               }

               readCachePositionStart = position / bufferSize * bufferSize;

               cachedChannel.position(readCachePositionStart);

               if (readCache == null) {
                  readCache = ByteBuffer.allocate(bufferSize);
               }

               readCache.clear();

               readCachePositionEnd = readCachePositionStart + cachedChannel.read(readCache) - 1;
            }
         } catch (Exception e) {
            ActiveMQClientLogger.LOGGER.errorReadingCache(e);
            throw new RuntimeException(e.getMessage(), e);
         } finally {
            close();
         }
      }

      private synchronized byte getByteFromCache(final long position) {
         readCache(position);

         return readCache.get((int) (position - readCachePositionStart));

      }

      public void cachePackage(final byte[] body) throws Exception {
         final FileChannel cachedChannel = checkOpen();

         cachedChannel.position(cachedChannel.size());
         cachedChannel.write(ByteBuffer.wrap(body));

         close();
      }

      /**
       * @throws FileNotFoundException
       */
      private FileChannel checkOpen() throws IOException {
         FileChannel channel = cachedChannel;
         if (cachedFile != null || !channel.isOpen()) {
            channel = FileChannel.open(cachedFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            cachedChannel = channel;
         }
         return channel;
      }

      public void close() {
         FileChannel cachedChannel = this.cachedChannel;
         if (cachedChannel != null && cachedChannel.isOpen()) {
            this.cachedChannel = null;
            try {
               cachedChannel.close();
            } catch (Exception e) {
               ActiveMQClientLogger.LOGGER.errorClosingCache(e);
            }
         }
      }

      @Override
      protected void finalize() {
         close();
         if (cachedFile != null && cachedFile.exists()) {
            try {
               cachedFile.delete();
            } catch (Exception e) {
               ActiveMQClientLogger.LOGGER.errorFinalisingCache(e);
            }
         }
      }
   }

   private static final class LargeData {

      private final byte[] chunk;
      private final int flowControlSize;
      private final boolean continues;

      private LargeData() {
         continues = false;
         flowControlSize = 0;
         chunk = null;
      }

      private LargeData(byte[] chunk, int flowControlSize, boolean continues) {
         this.chunk = chunk;
         this.flowControlSize = flowControlSize;
         this.continues = continues;
      }

      public byte[] getChunk() {
         return chunk;
      }

      public int getFlowControlSize() {
         return flowControlSize;
      }

      public boolean isContinues() {
         return continues;
      }
   }
}
