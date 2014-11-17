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
package org.apache.activemq6.tests.unit.core.journal.impl.fakes;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.api.core.HornetQBuffers;
import org.apache.activemq6.api.core.HornetQExceptionType;
import org.apache.activemq6.core.asyncio.BufferCallback;
import org.apache.activemq6.core.journal.EncodingSupport;
import org.apache.activemq6.core.journal.IOAsyncTask;
import org.apache.activemq6.core.journal.SequentialFile;
import org.apache.activemq6.core.journal.SequentialFileFactory;
import org.apache.activemq6.core.journal.impl.TimedBuffer;

/**
 * A FakeSequentialFileFactory
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class FakeSequentialFileFactory implements SequentialFileFactory
{

   private final Map<String, FakeSequentialFile> fileMap = new ConcurrentHashMap<String, FakeSequentialFile>();

   private final int alignment;

   private final boolean supportsCallback;

   private volatile boolean holdCallbacks;

   private ListenerHoldCallback holdCallbackListener;

   private volatile boolean generateErrors;

   private final List<CallbackRunnable> callbacksInHold;

   public FakeSequentialFileFactory(final int alignment, final boolean supportsCallback)
   {
      this.alignment = alignment;
      this.supportsCallback = supportsCallback;
      callbacksInHold = new ArrayList<CallbackRunnable>();
   }

   public FakeSequentialFileFactory()
   {
      this(1, false);
   }

   // Public --------------------------------------------------------

   public SequentialFile createSequentialFile(final String fileName, final int maxAIO)
   {
      FakeSequentialFile sf = fileMap.get(fileName);

      if (sf == null || sf.data == null)
      {
         sf = newSequentialFile(fileName);

         fileMap.put(fileName, sf);
      }
      else
      {
         sf.getData().position(0);

         // log.debug("positioning data to 0");
      }

      return sf;
   }

   public void clearBuffer(final ByteBuffer buffer)
   {
      final int limit = buffer.limit();
      buffer.rewind();

      for (int i = 0; i < limit; i++)
      {
         buffer.put((byte) 0);
      }

      buffer.rewind();
   }

   public List<String> listFiles(final String extension)
   {
      List<String> files = new ArrayList<String>();

      for (String s : fileMap.keySet())
      {
         if (s.endsWith("." + extension))
         {
            files.add(s);
         }
      }

      return files;
   }

   public Map<String, FakeSequentialFile> getFileMap()
   {
      return fileMap;
   }

   public void clear()
   {
      fileMap.clear();
   }

   public boolean isSupportsCallbacks()
   {
      return supportsCallback;
   }

   public ByteBuffer newBuffer(int size)
   {
      if (size % alignment != 0)
      {
         size = (size / alignment + 1) * alignment;
      }
      return ByteBuffer.allocate(size);
   }

   public int calculateBlockSize(final int position)
   {
      int alignment = getAlignment();

      int pos = (position / alignment + (position % alignment != 0 ? 1 : 0)) * alignment;

      return pos;
   }

   public ByteBuffer wrapBuffer(final byte[] bytes)
   {
      return ByteBuffer.wrap(bytes);
   }

   public synchronized boolean isHoldCallbacks()
   {
      return holdCallbacks;
   }

   public synchronized void setHoldCallbacks(final boolean holdCallbacks,
                                             final ListenerHoldCallback holdCallbackListener)
   {
      this.holdCallbacks = holdCallbacks;
      this.holdCallbackListener = holdCallbackListener;
   }

   public boolean isGenerateErrors()
   {
      return generateErrors;
   }

   public void setGenerateErrors(final boolean generateErrors)
   {
      this.generateErrors = generateErrors;
   }

   public synchronized void flushAllCallbacks()
   {
      for (Runnable action : callbacksInHold)
      {
         action.run();
      }

      callbacksInHold.clear();
   }

   public synchronized void flushCallback(final int position)
   {
      Runnable run = callbacksInHold.get(position);
      run.run();
      callbacksInHold.remove(run);
   }

   public synchronized void setCallbackAsError(final int position)
   {
      callbacksInHold.get(position).setSendError(true);
   }

   public synchronized int getNumberOfCallbacks()
   {
      return callbacksInHold.size();
   }

   public int getAlignment()
   {
      return alignment;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected FakeSequentialFile newSequentialFile(final String fileName)
   {
      return new FakeSequentialFile(fileName);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   /**
    * This listener will return a message to the test with each callback added
    */
   public interface ListenerHoldCallback
   {
      void callbackAdded(final ByteBuffer bytes);
   }

   private class CallbackRunnable implements Runnable
   {

      final FakeSequentialFile file;

      final ByteBuffer bytes;

      final IOAsyncTask callback;

      volatile boolean sendError;

      CallbackRunnable(final FakeSequentialFile file, final ByteBuffer bytes, final IOAsyncTask callback)
      {
         this.file = file;
         this.bytes = bytes;
         this.callback = callback;
      }

      public void run()
      {

         if (sendError)
         {
            callback.onError(HornetQExceptionType.UNSUPPORTED_PACKET.getCode(), "Fake aio error");
         }
         else
         {
            try
            {
               file.data.put(bytes);
               if (callback != null)
               {
                  callback.done();
               }

               if (file.bufferCallback != null)
               {
                  file.bufferCallback.bufferDone(bytes);
               }
            }
            catch (Throwable e)
            {
               e.printStackTrace();
               callback.onError(HornetQExceptionType.GENERIC_EXCEPTION.getCode(), e.getMessage());
            }
         }
      }

      public void setSendError(final boolean sendError)
      {
         this.sendError = sendError;
      }
   }

   public class FakeSequentialFile implements SequentialFile
   {
      private volatile boolean open;

      private String fileName;

      private ByteBuffer data;

      private BufferCallback bufferCallback;

      public ByteBuffer getData()
      {
         return data;
      }

      public boolean isOpen()
      {
         return open;
      }

      public void flush()
      {
      }

      public FakeSequentialFile(final String fileName)
      {
         this.fileName = fileName;
      }

      public synchronized void close()
      {
         open = false;

         if (data != null)
         {
            data.position(0);
         }

         notifyAll();
      }

      public synchronized void waitForClose() throws Exception
      {
         while (open)
         {
            this.wait();
         }
      }

      public void delete()
      {
         if (open)
         {
            close();
         }

         fileMap.remove(fileName);
      }

      public String getFileName()
      {
         return fileName;
      }

      public void open() throws Exception
      {
         open(1, true);
      }

      public synchronized void open(final int currentMaxIO, final boolean useExecutor) throws Exception
      {
         open = true;
         checkAndResize(0);
      }

      public void setBufferCallback(final BufferCallback callback)
      {
         bufferCallback = callback;
      }

      public void fill(final int pos, final int size, final byte fillCharacter) throws Exception
      {
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }

         checkAndResize(pos + size);

         // log.debug("size is " + size + " pos is " + pos);

         for (int i = pos; i < size + pos; i++)
         {
            byte[] array = data.array();
            array[i] = fillCharacter;

            // log.debug("Filling " + pos + " with char " + fillCharacter);
         }
      }

      public int read(final ByteBuffer bytes) throws Exception
      {
         return read(bytes, null);
      }

      public int read(final ByteBuffer bytes, final IOAsyncTask callback) throws Exception
      {
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }

         byte[] bytesRead = new byte[bytes.limit()];

         data.get(bytesRead);

         bytes.put(bytesRead);

         bytes.rewind();

         if (callback != null)
         {
            callback.done();
         }

         return bytesRead.length;
      }

      @Override
      public void position(final long pos)
      {
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }

         checkAlignment(pos);

         data.position((int) pos);
      }

      public long position()
      {
         return data.position();
      }

      public synchronized void writeDirect(final ByteBuffer bytes, final boolean sync, final IOAsyncTask callback)
      {
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }

         final int position = data == null ? 0 : data.position();

         // checkAlignment(position);

         // checkAlignment(bytes.limit());

         checkAndResize(bytes.limit() + position);

         CallbackRunnable action = new CallbackRunnable(this, bytes, callback);

         if (generateErrors)
         {
            action.setSendError(true);
         }

         if (holdCallbacks)
         {
            addCallback(bytes, action);
         }
         else
         {
            action.run();
         }

      }

      public void sync() throws IOException
      {
         if (supportsCallback)
         {
            throw new IllegalStateException("sync is not supported when supportsCallback=true");
         }
      }

      public long size() throws Exception
      {
         if (data == null)
         {
            return 0;
         }
         else
         {
            return data.limit();
         }
      }

      public void writeDirect(final ByteBuffer bytes, final boolean sync) throws Exception
      {
         writeDirect(bytes, sync, null);
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#writeInternal(java.nio.ByteBuffer)
       */
      public void writeInternal(ByteBuffer bytes) throws Exception
      {
         writeDirect(bytes, true);
      }


      private void checkAndResize(final int size)
      {
         int oldpos = data == null ? 0 : data.position();

         if (data == null || data.array().length < size)
         {
            byte[] newBytes = new byte[size];

            if (data != null)
            {
               System.arraycopy(data.array(), 0, newBytes, 0, data.array().length);
            }

            data = ByteBuffer.wrap(newBytes);

            data.position(oldpos);
         }
      }

      /**
       * @param bytes
       * @param action
       */
      private void addCallback(final ByteBuffer bytes, final CallbackRunnable action)
      {
         synchronized (FakeSequentialFileFactory.this)
         {
            callbacksInHold.add(action);
            if (holdCallbackListener != null)
            {
               holdCallbackListener.callbackAdded(bytes);
            }
         }
      }

      public int getAlignment() throws Exception
      {
         return alignment;
      }

      public int calculateBlockStart(final int position) throws Exception
      {
         int pos = (position / alignment + (position % alignment != 0 ? 1 : 0)) * alignment;

         return pos;
      }

      @Override
      public String toString()
      {
         return "FakeSequentialFile:" + fileName;
      }

      private void checkAlignment(final long position)
      {
         if (position % alignment != 0)
         {
            throw new IllegalStateException("Position is not aligned to " + alignment);
         }
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#renameTo(org.apache.activemq6.core.journal.SequentialFile)
       */
      public void renameTo(final String newFileName) throws Exception
      {
         fileMap.remove(fileName);
         fileName = newFileName;
         fileMap.put(newFileName, this);
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#fits(int)
       */
      public boolean fits(final int size)
      {
         return data.position() + size <= data.limit();
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#setBuffering(boolean)
       */
      public void setBuffering(final boolean buffering)
      {
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#lockBuffer()
       */
      public void disableAutoFlush()
      {
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#unlockBuffer()
       */
      public void enableAutoFlush()
      {
      }

      public SequentialFile cloneFile()
      {
         return null; // To change body of implemented methods use File | Settings | File Templates.
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#write(org.apache.activemq6.spi.core.remoting.HornetQBuffer, boolean, org.apache.activemq6.core.journal.IOCallback)
       */
      public void write(final HornetQBuffer bytes, final boolean sync, final IOAsyncTask callback) throws Exception
      {
         bytes.writerIndex(bytes.capacity());
         bytes.readerIndex(0);
         writeDirect(bytes.toByteBuffer(), sync, callback);

      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#write(org.apache.activemq6.spi.core.remoting.HornetQBuffer, boolean)
       */
      public void write(final HornetQBuffer bytes, final boolean sync) throws Exception
      {
         bytes.writerIndex(bytes.capacity());
         bytes.readerIndex(0);
         writeDirect(bytes.toByteBuffer(), sync);
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#write(org.apache.activemq6.core.journal.EncodingSupport, boolean, org.apache.activemq6.core.journal.IOCompletion)
       */
      public void write(final EncodingSupport bytes, final boolean sync, final IOAsyncTask callback) throws Exception
      {
         ByteBuffer buffer = newBuffer(bytes.getEncodeSize());
         HornetQBuffer outbuffer = HornetQBuffers.wrappedBuffer(buffer);
         bytes.encode(outbuffer);
         write(outbuffer, sync, callback);
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#write(org.apache.activemq6.core.journal.EncodingSupport, boolean)
       */
      public void write(final EncodingSupport bytes, final boolean sync) throws Exception
      {
         ByteBuffer buffer = newBuffer(bytes.getEncodeSize());
         HornetQBuffer outbuffer = HornetQBuffers.wrappedBuffer(buffer);
         bytes.encode(outbuffer);
         write(outbuffer, sync);
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#exists()
       */
      public boolean exists()
      {
         FakeSequentialFile file = fileMap.get(fileName);

         return file != null && file.data != null && file.data.capacity() > 0;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#setTimedBuffer(org.apache.activemq6.core.journal.impl.TimedBuffer)
       */
      public void setTimedBuffer(final TimedBuffer buffer)
      {
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.SequentialFile#copyTo(org.apache.activemq6.core.journal.SequentialFile)
       */
      public void copyTo(SequentialFile newFileName)
      {
         // TODO Auto-generated method stub

      }

      @Override
      public File getJavaFile()
      {
         throw new UnsupportedOperationException();
      }

   }

   @Override
   public void createDirs() throws Exception
   {
      // nothing to be done on the fake Sequential file
   }

   @Override
   public void releaseBuffer(final ByteBuffer buffer)
   {
   }

   @Override
   public void stop()
   {
   }

   @Override
   public void activateBuffer(final SequentialFile file)
   {
   }

   @Override
   public void start()
   {
   }

   @Override
   public void deactivateBuffer()
   {
   }

   @Override
   public void flush()
   {
   }

   @Override
   public void onIOError(Exception exception, String message, SequentialFile file)
   {
   }

   @Override
   public ByteBuffer allocateDirectBuffer(int size)
   {
      return ByteBuffer.allocateDirect(size);
   }

   @Override
   public void releaseDirectBuffer(ByteBuffer buffer)
   {
   }

   @Override
   public String getDirectory()
   {
      // TODO Auto-generated method stub
      return null;
   }

}
