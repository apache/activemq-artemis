/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.core.client.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.api.core.HornetQBuffers;
import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.HornetQExceptionType;
import org.apache.activemq6.api.core.HornetQInterruptedException;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.core.client.HornetQClientLogger;
import org.apache.activemq6.core.client.HornetQClientMessageBundle;
import org.apache.activemq6.utils.DataConstants;
import org.apache.activemq6.utils.UTF8Util;

/**
 * This class aggregates several {@link org.apache.activemq6.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage}
 * as it was being handled
 * by a single buffer. This buffer can be consumed as messages are arriving, and it will hold the
 * packets until they are read using the ChannelBuffer interface, or the setOutputStream or
 * saveStream are called.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class LargeMessageControllerImpl implements LargeMessageController
{
   // Constants -----------------------------------------------------

   private static final String READ_ONLY_ERROR_MESSAGE = "This is a read-only buffer, setOperations are not supported";

   // Attributes ----------------------------------------------------

   private final ClientConsumerInternal consumerInternal;

   private final LinkedBlockingQueue<LargeData> largeMessageData = new LinkedBlockingQueue<>();

   private volatile LargeData currentPacket = null;

   private final long totalSize;

   private final int bufferSize;

   private boolean streamEnded = false;

   private boolean streamClosed = false;

   private final long readTimeout;

   private long readerIndex = 0;

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

   public LargeMessageControllerImpl(final ClientConsumerInternal consumerInternal,
                                     final long totalSize,
                                     final long readTimeout)
   {
      this(consumerInternal, totalSize, readTimeout, null);
   }

   public LargeMessageControllerImpl(final ClientConsumerInternal consumerInternal,
                                     final long totalSize,
                                     final long readTimeout,
                                     final File cachedFile)
   {
      this(consumerInternal, totalSize, readTimeout, cachedFile, 10 * 1024);
   }

   public LargeMessageControllerImpl(final ClientConsumerInternal consumerInternal,
                                     final long totalSize,
                                     final long readTimeout,
                                     final File cachedFile,
                                     final int bufferSize)
   {
      this.consumerInternal = consumerInternal;
      this.readTimeout = readTimeout;
      this.totalSize = totalSize;
      if (cachedFile == null)
      {
         fileCache = null;
      }
      else
      {
         fileCache = new FileCache(cachedFile);
      }
      this.bufferSize = bufferSize;
   }

   // Public --------------------------------------------------------

   public void setLocal(boolean local)
   {
      this.local = local;
   }

   public void discardUnusedPackets()
   {
      if (outStream == null)
      {
         if (local) return;
         try
         {
            checkForPacket(totalSize - 1);
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   /**
    * TODO: move this to ConsumerContext as large message is a protocol specific thing
    * Add a buff to the List, or save it to the OutputStream if set
    */
   public void addPacket(byte[] chunk, int flowControlSize, boolean isContinues)
   {
      int flowControlCredit = 0;

      synchronized (this)
      {
         packetAdded = true;
         if (outStream != null)
         {
            try
            {
               if (!isContinues)
               {
                  streamEnded = true;
               }

               if (fileCache != null)
               {
                  fileCache.cachePackage(chunk);
               }

               outStream.write(chunk);

               flowControlCredit = flowControlSize;

               notifyAll();

               if (streamEnded)
               {
                  outStream.close();
               }
            }
            catch (Exception e)
            {
               HornetQClientLogger.LOGGER.errorAddingPacket(e);
               handledException = e;
            }
         }
         else
         {
            if (fileCache != null)
            {
               try
               {
                  fileCache.cachePackage(chunk);
               }
               catch (Exception e)
               {
                  HornetQClientLogger.LOGGER.errorAddingPacket(e);
                  handledException = e;
               }
            }


            largeMessageData.offer(new LargeData(chunk, flowControlSize, isContinues));
         }
      }

      if (flowControlCredit != 0)
      {
         try
         {
            consumerInternal.flowControl(flowControlCredit, !isContinues);
         }
         catch (Exception e)
         {
            HornetQClientLogger.LOGGER.errorAddingPacket(e);
            handledException = e;
         }
      }
   }

   public void cancel()
   {
      this.handledException = HornetQClientMessageBundle.BUNDLE.largeMessageInterrupted();

      synchronized (this)
      {
         int totalSize = 0;
         LargeData polledPacket = null;
         while ((polledPacket = largeMessageData.poll()) != null)
         {
            totalSize += polledPacket.getFlowControlSize();
         }

         try
         {
            consumerInternal.flowControl(totalSize, false);
         }
         catch (Exception ignored)
         {
            // what else can we do here?
            HornetQClientLogger.LOGGER.errorCallingCancel(ignored);
         }

         largeMessageData.offer(new LargeData());
         streamEnded = true;
         streamClosed = true;

         notifyAll();
      }
   }

   public synchronized void close()
   {
      if (fileCache != null)
      {
         fileCache.close();
      }
   }

   public void setOutputStream(final OutputStream output) throws HornetQException
   {

      int totalFlowControl = 0;
      boolean continues = false;

      synchronized (this)
      {
         if (currentPacket != null)
         {
            sendPacketToOutput(output, currentPacket);
            currentPacket = null;
         }
         while (handledException == null)
         {
            LargeData packet = largeMessageData.poll();
            if (packet == null)
            {
               break;
            }
            totalFlowControl += packet.getFlowControlSize();

            continues = packet.isContinues();
            sendPacketToOutput(output, packet);
         }

         checkException();
         outStream = output;
      }

      if (totalFlowControl > 0)
      {
         consumerInternal.flowControl(totalFlowControl, !continues);
      }
   }

   public synchronized void saveBuffer(final OutputStream output) throws HornetQException
   {
      if (streamClosed)
      {
         throw HornetQClientMessageBundle.BUNDLE.largeMessageLostSession();
      }
      setOutputStream(output);
      waitCompletion(0);
   }

   /**
    * @param timeWait Milliseconds to Wait. 0 means forever
    * @throws HornetQException
    */
   public synchronized boolean waitCompletion(final long timeWait) throws HornetQException
   {
      if (outStream == null)
      {
         // There is no stream.. it will never achieve the end of streaming
         return false;
      }

      long timeOut;

      // If timeWait = 0, we will use the readTimeout
      // And we will check if no packets have arrived within readTimeout milliseconds
      if (timeWait != 0)
      {
         timeOut = System.currentTimeMillis() + timeWait;
      }
      else
      {
         timeOut = System.currentTimeMillis() + readTimeout;
      }

      while (!streamEnded && handledException == null)
      {
         try
         {
            this.wait(timeWait == 0 ? readTimeout : timeWait);
         }
         catch (InterruptedException e)
         {
            throw new HornetQInterruptedException(e);
         }

         if (!streamEnded && handledException == null)
         {
            if (timeWait != 0 && System.currentTimeMillis() > timeOut)
            {

               // TODO: what to do here?
               //throw new HornetQException(HornetQException.LARGE_MESSAGE_ERROR_BODY,
               //         "Timeout waiting for LargeMessage Body");
               throw HornetQClientMessageBundle.BUNDLE.timeoutOnLargeMessage();
            }
            else if (System.currentTimeMillis() > timeOut && !packetAdded)
            {
               // throw new HornetQException(HornetQException.LARGE_MESSAGE_ERROR_BODY,
               //                           "No packets have arrived within " + readTimeout + " milliseconds");
               throw HornetQClientMessageBundle.BUNDLE.timeoutOnLargeMessage();
            }
         }
      }

      checkException();

      return streamEnded;

   }

   /**
    * @throws HornetQException
    */
   private void checkException() throws HornetQException
   {
      // it's not needed to copy it as we never set it back to null
      // once the exception is set, the controller is pretty much useless
      if (handledException != null)
      {
         if (handledException instanceof HornetQException)
         {
            throw (HornetQException)handledException;
         }
         else
         {
            throw new HornetQException(HornetQExceptionType.LARGE_MESSAGE_ERROR_BODY,
                                       "Error on saving LargeMessageBufferImpl",
                                       handledException);
         }
      }
   }

   // Channel Buffer Implementation ---------------------------------

   @Override
   public int capacity()
   {
      return -1;
   }

   public byte readByte()
   {
      return getByte(readerIndex++);
   }

   @Override
   public byte getByte(final int index)
   {
      return getByte((long)index);
   }

   private byte getByte(final long index)
   {
      checkForPacket(index);

      if (fileCache != null && index < packetPosition)
      {
         return fileCache.getByteFromCache(index);
      }
      else
      {
         return currentPacket.getChunk()[(int)(index - packetPosition)];
      }
   }

   @Override
   public void getBytes(final int index, final HornetQBuffer dst, final int dstIndex, final int length)
   {
      byte[] destBytes = new byte[length];
      getBytes(index, destBytes);
      dst.setBytes(dstIndex, destBytes);
   }

   private void getBytes(final long index, final HornetQBuffer dst, final int dstIndex, final int length)
   {
      byte[] destBytes = new byte[length];
      getBytes(index, destBytes);
      dst.setBytes(dstIndex, destBytes);
   }

   @Override
   public void getBytes(final int index, final byte[] dst, final int dstIndex, final int length)
   {
      byte[] bytesToGet = new byte[length];

      getBytes(index, bytesToGet);

      System.arraycopy(bytesToGet, 0, dst, dstIndex, length);
   }

   public void getBytes(final long index, final byte[] dst, final int dstIndex, final int length)
   {
      byte[] bytesToGet = new byte[length];

      getBytes(index, bytesToGet);

      System.arraycopy(bytesToGet, 0, dst, dstIndex, length);
   }

   @Override
   public void getBytes(final int index, final ByteBuffer dst)
   {
      byte[] bytesToGet = new byte[dst.remaining()];
      getBytes(index, bytesToGet);
      dst.put(bytesToGet);
   }

   public void getBytes(final long index, final ByteBuffer dst)
   {
      byte[] bytesToGet = new byte[dst.remaining()];
      getBytes(index, bytesToGet);
      dst.put(bytesToGet);
   }

   public void getBytes(final int index, final OutputStream out, final int length) throws IOException
   {
      byte[] bytesToGet = new byte[length];
      getBytes(index, bytesToGet);
      out.write(bytesToGet);
   }

   public void getBytes(final long index, final OutputStream out, final int length) throws IOException
   {
      byte[] bytesToGet = new byte[length];
      getBytes(index, bytesToGet);
      out.write(bytesToGet);
   }

   public int getBytes(final int index, final GatheringByteChannel out, final int length) throws IOException
   {
      byte[] bytesToGet = new byte[length];
      getBytes(index, bytesToGet);
      return out.write(ByteBuffer.wrap(bytesToGet));
   }

   public int getInt(final int index)
   {
      return (getByte(index) & 0xff) << 24 | (getByte(index + 1) & 0xff) << 16 |
         (getByte(index + 2) & 0xff) << 8 |
         (getByte(index + 3) & 0xff) << 0;
   }

   public int getInt(final long index)
   {
      return (getByte(index) & 0xff) << 24 | (getByte(index + 1) & 0xff) << 16 |
         (getByte(index + 2) & 0xff) << 8 |
         (getByte(index + 3) & 0xff) << 0;
   }

   @Override
   public long getLong(final int index)
   {
      return ((long)getByte(index) & 0xff) << 56 | ((long)getByte(index + 1) & 0xff) << 48 |
         ((long)getByte(index + 2) & 0xff) << 40 |
         ((long)getByte(index + 3) & 0xff) << 32 |
         ((long)getByte(index + 4) & 0xff) << 24 |
         ((long)getByte(index + 5) & 0xff) << 16 |
         ((long)getByte(index + 6) & 0xff) << 8 |
         ((long)getByte(index + 7) & 0xff) << 0;
   }

   public long getLong(final long index)
   {
      return ((long)getByte(index) & 0xff) << 56 | ((long)getByte(index + 1) & 0xff) << 48 |
         ((long)getByte(index + 2) & 0xff) << 40 |
         ((long)getByte(index + 3) & 0xff) << 32 |
         ((long)getByte(index + 4) & 0xff) << 24 |
         ((long)getByte(index + 5) & 0xff) << 16 |
         ((long)getByte(index + 6) & 0xff) << 8 |
         ((long)getByte(index + 7) & 0xff) << 0;
   }

   @Override
   public short getShort(final int index)
   {
      return (short)(getByte(index) << 8 | getByte(index + 1) & 0xFF);
   }

   public short getShort(final long index)
   {
      return (short)(getByte(index) << 8 | getByte(index + 1) & 0xFF);
   }

   private int getUnsignedMedium(final int index)
   {
      return (getByte(index) & 0xff) << 16 | (getByte(index + 1) & 0xff) << 8 | (getByte(index + 2) & 0xff) << 0;
   }

   public int getUnsignedMedium(final long index)
   {
      return (getByte(index) & 0xff) << 16 | (getByte(index + 1) & 0xff) << 8 | (getByte(index + 2) & 0xff) << 0;
   }

   @Override
   public void setByte(final int index, final byte value)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setBytes(final int index, final HornetQBuffer src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setBytes(final int index, final byte[] src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setBytes(final int index, final ByteBuffer src)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setInt(final int index, final int value)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setLong(final int index, final long value)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void setShort(final int index, final short value)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public ByteBuffer toByteBuffer(final int index, final int length)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public int readerIndex()
   {
      return (int)readerIndex;
   }

   public void readerIndex(final int readerIndex)
   {
      try
      {
         checkForPacket(readerIndex);
      }
      catch (Exception e)
      {
         HornetQClientLogger.LOGGER.errorReadingIndex(e);
         throw new RuntimeException(e.getMessage(), e);
      }
      this.readerIndex = readerIndex;
   }

   public int writerIndex()
   {
      return (int)totalSize;
   }

   public long getSize()
   {
      return totalSize;
   }

   public void writerIndex(final int writerIndex)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void setIndex(final int readerIndex, final int writerIndex)
   {
      try
      {
         checkForPacket(readerIndex);
      }
      catch (Exception e)
      {
         HornetQClientLogger.LOGGER.errorSettingIndex(e);
         throw new RuntimeException(e.getMessage(), e);
      }
      this.readerIndex = readerIndex;
   }

   public void clear()
   {
   }

   public boolean readable()
   {
      return true;
   }

   public boolean writable()
   {
      return false;
   }

   public int readableBytes()
   {
      long readableBytes = totalSize - readerIndex;

      if (readableBytes > Integer.MAX_VALUE)
      {
         return Integer.MAX_VALUE;
      }
      else
      {
         return (int)(totalSize - readerIndex);
      }
   }

   public int writableBytes()
   {
      return 0;
   }

   public void markReaderIndex()
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void resetReaderIndex()
   {
      try
      {
         checkForPacket(0);
      }
      catch (Exception e)
      {
         HornetQClientLogger.LOGGER.errorReSettingIndex(e);
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   public void markWriterIndex()
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void resetWriterIndex()
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void discardReadBytes()
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public short getUnsignedByte(final int index)
   {
      return (short)(getByte(index) & 0xFF);
   }

   public int getUnsignedShort(final int index)
   {
      return getShort(index) & 0xFFFF;
   }

   public int getMedium(final int index)
   {
      int value = getUnsignedMedium(index);
      if ((value & 0x800000) != 0)
      {
         value |= 0xff000000;
      }
      return value;
   }

   public long getUnsignedInt(final int index)
   {
      return getInt(index) & 0xFFFFFFFFL;
   }

   public void getBytes(int index, final byte[] dst)
   {
      // TODO: optimize this by using System.arraycopy
      for (int i = 0; i < dst.length; i++)
      {
         dst[i] = getByte(index++);
      }
   }

   public void getBytes(long index, final byte[] dst)
   {
      // TODO: optimize this by using System.arraycopy
      for (int i = 0; i < dst.length; i++)
      {
         dst[i] = getByte(index++);
      }
   }

   public void getBytes(final int index, final HornetQBuffer dst)
   {
      getBytes(index, dst, dst.writableBytes());
   }

   public void getBytes(final int index, final HornetQBuffer dst, final int length)
   {
      if (length > dst.writableBytes())
      {
         throw new IndexOutOfBoundsException();
      }
      getBytes(index, dst, dst.writerIndex(), length);
      dst.writerIndex(dst.writerIndex() + length);
   }

   public void setBytes(final int index, final byte[] src)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void setBytes(final int index, final HornetQBuffer src)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void setBytes(final int index, final HornetQBuffer src, final int length)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void setZero(final int index, final int length)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public short readUnsignedByte()
   {
      return (short)(readByte() & 0xFF);
   }

   public short readShort()
   {
      short v = getShort(readerIndex);
      readerIndex += 2;
      return v;
   }

   public int readUnsignedShort()
   {
      return readShort() & 0xFFFF;
   }

   public int readMedium()
   {
      int value = readUnsignedMedium();
      if ((value & 0x800000) != 0)
      {
         value |= 0xff000000;
      }
      return value;
   }

   public int readUnsignedMedium()
   {
      int v = getUnsignedMedium(readerIndex);
      readerIndex += 3;
      return v;
   }

   public int readInt()
   {
      int v = getInt(readerIndex);
      readerIndex += 4;
      return v;
   }

   public int readInt(final int pos)
   {
      int v = getInt(pos);
      return v;
   }

   public long readUnsignedInt()
   {
      return readInt() & 0xFFFFFFFFL;
   }

   public long readLong()
   {
      long v = getLong(readerIndex);
      readerIndex += 8;
      return v;
   }

   public void readBytes(final byte[] dst, final int dstIndex, final int length)
   {
      getBytes(readerIndex, dst, dstIndex, length);
      readerIndex += length;
   }

   public void readBytes(final byte[] dst)
   {
      readBytes(dst, 0, dst.length);
   }

   public void readBytes(final HornetQBuffer dst)
   {
      readBytes(dst, dst.writableBytes());
   }

   public void readBytes(final HornetQBuffer dst, final int length)
   {
      if (length > dst.writableBytes())
      {
         throw new IndexOutOfBoundsException();
      }
      readBytes(dst, dst.writerIndex(), length);
      dst.writerIndex(dst.writerIndex() + length);
   }

   public void readBytes(final HornetQBuffer dst, final int dstIndex, final int length)
   {
      getBytes(readerIndex, dst, dstIndex, length);
      readerIndex += length;
   }

   public void readBytes(final ByteBuffer dst)
   {
      int length = dst.remaining();
      getBytes(readerIndex, dst);
      readerIndex += length;
   }

   public int readBytes(final GatheringByteChannel out, final int length) throws IOException
   {
      int readBytes = getBytes((int)readerIndex, out, length);
      readerIndex += readBytes;
      return readBytes;
   }

   public void readBytes(final OutputStream out, final int length) throws IOException
   {
      getBytes(readerIndex, out, length);
      readerIndex += length;
   }

   public void skipBytes(final int length)
   {

      long newReaderIndex = readerIndex + length;
      checkForPacket(newReaderIndex);
      readerIndex = newReaderIndex;
   }

   public void writeByte(final byte value)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void writeShort(final short value)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void writeMedium(final int value)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void writeInt(final int value)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void writeLong(final long value)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final byte[] src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final byte[] src)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final HornetQBuffer src)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final HornetQBuffer src, final int length)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final ByteBuffer src)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public int writeBytes(final InputStream in, final int length) throws IOException
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public int writeBytes(final ScatteringByteChannel in, final int length) throws IOException
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void writeZero(final int length)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public ByteBuffer toByteBuffer()
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public ByteBuffer[] toByteBuffers()
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public ByteBuffer[] toByteBuffers(final int index, final int length)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public String toString(final String charsetName)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public Object getUnderlyingBuffer()
   {
      return this;
   }

   @Override
   public boolean readBoolean()
   {
      return readByte() != 0;
   }

   @Override
   public char readChar()
   {
      return (char)readShort();
   }

   public char getChar(final int index)
   {
      return (char)getShort(index);
   }

   public double getDouble(final int index)
   {
      return Double.longBitsToDouble(getLong(index));
   }

   public float getFloat(final int index)
   {
      return Float.intBitsToFloat(getInt(index));
   }

   public HornetQBuffer readBytes(final int length)
   {
      byte[] bytesToGet = new byte[length];
      getBytes(readerIndex, bytesToGet);
      readerIndex += length;
      return HornetQBuffers.wrappedBuffer(bytesToGet);
   }

   @Override
   public double readDouble()
   {
      return Double.longBitsToDouble(readLong());
   }

   @Override
   public float readFloat()
   {
      return Float.intBitsToFloat(readInt());
   }

   @Override
   public SimpleString readNullableSimpleString()
   {
      int b = readByte();
      if (b == DataConstants.NULL)
      {
         return null;
      }
      else
      {
         return readSimpleString();
      }
   }

   @Override
   public String readNullableString()
   {
      int b = readByte();
      if (b == DataConstants.NULL)
      {
         return null;
      }
      else
      {
         return readString();
      }
   }

   @Override
   public SimpleString readSimpleString()
   {
      int len = readInt();
      byte[] data = new byte[len];
      readBytes(data);
      return new SimpleString(data);
   }

   @Override
   public String readString()
   {
      int len = readInt();

      if (len < 9)
      {
         char[] chars = new char[len];
         for (int i = 0; i < len; i++)
         {
            chars[i] = (char)readShort();
         }
         return new String(chars);
      }
      else if (len < 0xfff)
      {
         return readUTF();
      }
      else
      {
         return readSimpleString().toString();
      }
   }

   @Override
   public String readUTF()
   {
      return UTF8Util.readUTF(this);
   }

   @Override
   public void writeBoolean(final boolean val)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeChar(final char val)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeDouble(final double val)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);

   }

   @Override
   public void writeFloat(final float val)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);

   }

   @Override
   public void writeNullableSimpleString(final SimpleString val)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeNullableString(final String val)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeSimpleString(final SimpleString val)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeString(final String val)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   @Override
   public void writeUTF(final String utf)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public HornetQBuffer copy()
   {
      throw new UnsupportedOperationException();
   }

   public HornetQBuffer slice(final int index, final int length)
   {
      throw new UnsupportedOperationException();
   }

   /**
    * @param output
    * @param packet
    * @throws HornetQException
    */
   private void sendPacketToOutput(final OutputStream output, final LargeData packet) throws HornetQException
   {
      try
      {
         output.write(packet.getChunk());
         if (!packet.isContinues())
         {
            streamEnded = true;
            output.close();
         }
      }
      catch (IOException e)
      {
         throw HornetQClientMessageBundle.BUNDLE.errorWritingLargeMessage(e);
      }
   }

   private void popPacket()
   {
      try
      {

         if (streamEnded)
         {
            // no more packets, we are over the last one already
            throw new IndexOutOfBoundsException();
         }

         int sizeToAdd = currentPacket != null ? currentPacket.chunk.length : 1;
         currentPacket = largeMessageData.poll(readTimeout, TimeUnit.SECONDS);
         if (currentPacket == null)
         {
            throw new IndexOutOfBoundsException();
         }

         if (currentPacket.chunk == null) // Empty packet as a signal to interruption
         {
            currentPacket = null;
            streamEnded = true;
            throw new IndexOutOfBoundsException();
         }

         consumerInternal.flowControl(currentPacket.getFlowControlSize(), !currentPacket.isContinues());

         packetPosition += sizeToAdd;

         packetLastPosition = packetPosition + currentPacket.getChunk().length;
      }
      catch (IndexOutOfBoundsException e)
      {
         throw e;
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   private void checkForPacket(final long index)
   {
      if (outStream != null)
      {
         throw new IllegalAccessError("Can't read the messageBody after setting outputStream");
      }

      if (index >= totalSize)
      {
         throw new IndexOutOfBoundsException();
      }

      if (streamClosed)
      {
         throw new IllegalAccessError("The consumer associated with this large message was closed before the body was read");
      }

      if (fileCache == null)
      {
         if (index < lastIndex)
         {
            throw new IllegalAccessError("LargeMessage have read-only and one-way buffers");
         }
         lastIndex = index;
      }

      while (index >= packetLastPosition && !streamEnded)
      {
         popPacket();
      }
   }

   private final class FileCache
   {

      public FileCache(final File cachedFile)
      {
         this.cachedFile = cachedFile;
      }

      ByteBuffer readCache;

      long readCachePositionStart = Integer.MAX_VALUE;

      long readCachePositionEnd = -1;

      private final File cachedFile;

      private volatile RandomAccessFile cachedRAFile;

      private volatile FileChannel cachedChannel;

      private synchronized void readCache(final long position)
      {

         try
         {
            if (position < readCachePositionStart || position > readCachePositionEnd)
            {

               checkOpen();

               if (position > cachedChannel.size())
               {
                  throw new ArrayIndexOutOfBoundsException("position > " + cachedChannel.size());
               }

               readCachePositionStart = position / bufferSize * bufferSize;

               cachedChannel.position(readCachePositionStart);

               if (readCache == null)
               {
                  readCache = ByteBuffer.allocate(bufferSize);
               }

               readCache.clear();

               readCachePositionEnd = readCachePositionStart + cachedChannel.read(readCache) - 1;
            }
         }
         catch (Exception e)
         {
            HornetQClientLogger.LOGGER.errorReadingCache(e);
            throw new RuntimeException(e.getMessage(), e);
         }
         finally
         {
            close();
         }
      }

      public synchronized byte getByteFromCache(final long position)
      {
         readCache(position);

         return readCache.get((int)(position - readCachePositionStart));

      }

      public void cachePackage(final byte[] body) throws Exception
      {
         checkOpen();

         cachedChannel.position(cachedChannel.size());
         cachedChannel.write(ByteBuffer.wrap(body));

         close();
      }

      /**
       * @throws FileNotFoundException
       */
      public void checkOpen() throws FileNotFoundException
      {
         if (cachedFile != null || !cachedChannel.isOpen())
         {
            cachedRAFile = new RandomAccessFile(cachedFile, "rw");

            cachedChannel = cachedRAFile.getChannel();
         }
      }

      public void close()
      {
         if (cachedChannel != null && cachedChannel.isOpen())
         {
            try
            {
               cachedChannel.close();
            }
            catch (Exception e)
            {
               HornetQClientLogger.LOGGER.errorClosingCache(e);
            }
            cachedChannel = null;
         }

         if (cachedRAFile != null)
         {
            try
            {
               cachedRAFile.close();
            }
            catch (Exception e)
            {
               HornetQClientLogger.LOGGER.errorClosingCache(e);
            }
            cachedRAFile = null;
         }

      }

      @Override
      protected void finalize()
      {
         close();
         if (cachedFile != null && cachedFile.exists())
         {
            try
            {
               cachedFile.delete();
            }
            catch (Exception e)
            {
               HornetQClientLogger.LOGGER.errorFinalisingCache(e);
            }
         }
      }

   }

   public ByteBuf byteBuf()
   {
      return null;
   }

   public HornetQBuffer copy(final int index, final int length)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public HornetQBuffer duplicate()
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public HornetQBuffer readSlice(final int length)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void setChar(final int index, final char value)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void setDouble(final int index, final double value)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void setFloat(final int index, final float value)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public HornetQBuffer slice()
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   public void writeBytes(final HornetQBuffer src, final int srcIndex, final int length)
   {
      throw new IllegalAccessError(LargeMessageControllerImpl.READ_ONLY_ERROR_MESSAGE);
   }

   private static class LargeData
   {
      final byte[] chunk;
      final int flowControlSize;
      final boolean continues;

      public LargeData()
      {
         continues = false;
         flowControlSize = 0;
         chunk = null;
      }

      public LargeData(byte[] chunk, int flowControlSize, boolean continues)
      {
         this.chunk = chunk;
         this.flowControlSize = flowControlSize;
         this.continues = continues;
      }

      public byte[] getChunk()
      {
         return chunk;
      }

      public int getFlowControlSize()
      {
         return flowControlSize;
      }

      public boolean isContinues()
      {
         return continues;
      }
   }
}
