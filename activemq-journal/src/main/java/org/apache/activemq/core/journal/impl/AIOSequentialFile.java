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
package org.apache.activemq6.core.journal.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.core.asyncio.AsynchronousFile;
import org.apache.activemq6.core.asyncio.BufferCallback;
import org.apache.activemq6.core.asyncio.IOExceptionListener;
import org.apache.activemq6.core.asyncio.impl.AsynchronousFileImpl;
import org.apache.activemq6.core.journal.IOAsyncTask;
import org.apache.activemq6.core.journal.SequentialFile;
import org.apache.activemq6.core.journal.SequentialFileFactory;

/**
 *
 * A AIOSequentialFile
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AIOSequentialFile extends AbstractSequentialFile implements IOExceptionListener
{
   private boolean opened = false;

   private final int maxIO;

   private AsynchronousFile aioFile;

   private final BufferCallback bufferCallback;

   /** The pool for Thread pollers */
   private final Executor pollerExecutor;

   public AIOSequentialFile(final SequentialFileFactory factory,
                            final int bufferSize,
                            final long bufferTimeoutMilliseconds,
                            final String directory,
                            final String fileName,
                            final int maxIO,
                            final BufferCallback bufferCallback,
                            final Executor writerExecutor,
                            final Executor pollerExecutor)
   {
      super(directory, new File(directory + "/" + fileName), factory, writerExecutor);
      this.maxIO = maxIO;
      this.bufferCallback = bufferCallback;
      this.pollerExecutor = pollerExecutor;
   }

   public boolean isOpen()
   {
      return opened;
   }

   public int getAlignment()
   {
      checkOpened();

      return aioFile.getBlockSize();
   }

   public int calculateBlockStart(final int position)
   {
      int alignment = getAlignment();

      int pos = (position / alignment + (position % alignment != 0 ? 1 : 0)) * alignment;

      return pos;
   }

   public SequentialFile cloneFile()
   {
      return new AIOSequentialFile(factory,
                                   -1,
                                   -1,
                                   getFile().getParent(),
                                   getFileName(),
                                   maxIO,
                                   bufferCallback,
                                   writerExecutor,
                                   pollerExecutor);
   }

   @Override
   public synchronized void close() throws IOException, InterruptedException, HornetQException
   {
      if (!opened)
      {
         return;
      }

      super.close();

      opened = false;

      timedBuffer = null;

      aioFile.close();
      aioFile = null;

      notifyAll();
   }

   @Override
   public synchronized void waitForClose() throws Exception
   {
      while (isOpen())
      {
         wait();
      }
   }

   public void fill(final int position, final int size, final byte fillCharacter) throws Exception
   {
      checkOpened();

      int fileblockSize = aioFile.getBlockSize();

      int blockSize = fileblockSize;

      if (size % (100 * 1024 * 1024) == 0)
      {
         blockSize = 100 * 1024 * 1024;
      }
      else if (size % (10 * 1024 * 1024) == 0)
      {
         blockSize = 10 * 1024 * 1024;
      }
      else if (size % (1024 * 1024) == 0)
      {
         blockSize = 1024 * 1024;
      }
      else if (size % (10 * 1024) == 0)
      {
         blockSize = 10 * 1024;
      }
      else
      {
         blockSize = fileblockSize;
      }

      int blocks = size / blockSize;

      if (size % blockSize != 0)
      {
         blocks++;
      }

      int filePosition = position;

      if (position % fileblockSize != 0)
      {
         filePosition = (position / fileblockSize + 1) * fileblockSize;
      }

      aioFile.fill(filePosition, blocks, blockSize, fillCharacter);

      fileSize = aioFile.size();
   }

   public void open() throws Exception
   {
      open(maxIO, true);
   }

   public synchronized void open(final int maxIO, final boolean useExecutor) throws HornetQException
   {
      opened = true;

      aioFile = new AsynchronousFileImpl(useExecutor ? writerExecutor : null, pollerExecutor, this);

      try
      {
         aioFile.open(getFile().getAbsolutePath(), maxIO);
      }
      catch (HornetQException e)
      {
         factory.onIOError(e, e.getMessage(), this);
         throw e;
      }

      position.set(0);

      aioFile.setBufferCallback(bufferCallback);

      fileSize = aioFile.size();
   }

   public void setBufferCallback(final BufferCallback callback)
   {
      aioFile.setBufferCallback(callback);
   }

   public int read(final ByteBuffer bytes, final IOAsyncTask callback) throws HornetQException
   {
      int bytesToRead = bytes.limit();

      long positionToRead = position.getAndAdd(bytesToRead);

      bytes.rewind();

      aioFile.read(positionToRead, bytesToRead, bytes, callback);

      return bytesToRead;
   }

   public int read(final ByteBuffer bytes) throws Exception
   {
      SimpleWaitIOCallback waitCompletion = new SimpleWaitIOCallback();

      int bytesRead = read(bytes, waitCompletion);

      waitCompletion.waitCompletion();

      return bytesRead;
   }

   public void sync()
   {
      throw new UnsupportedOperationException("This method is not supported on AIO");
   }

   public long size() throws Exception
   {
      if (aioFile == null)
      {
         return getFile().length();
      }
      else
      {
         return aioFile.size();
      }
   }

   @Override
   public String toString()
   {
      return "AIOSequentialFile:" + getFile().getAbsolutePath();
   }

   // Public methods
   // -----------------------------------------------------------------------------------------------------

   @Override
   public void onIOException(Exception code, String message)
   {
      factory.onIOError(code, message, this);
   }


   public void writeDirect(final ByteBuffer bytes, final boolean sync) throws Exception
   {
      if (sync)
      {
         SimpleWaitIOCallback completion = new SimpleWaitIOCallback();

         writeDirect(bytes, true, completion);

         completion.waitCompletion();
      }
      else
      {
         writeDirect(bytes, false, DummyCallback.getInstance());
      }
   }

   /**
    *
    * @param sync Not used on AIO
    *  */
   public void writeDirect(final ByteBuffer bytes, final boolean sync, final IOAsyncTask callback)
   {
      final int bytesToWrite = factory.calculateBlockSize(bytes.limit());

      final long positionToWrite = position.getAndAdd(bytesToWrite);

      aioFile.write(positionToWrite, bytesToWrite, bytes, callback);
   }

   public void writeInternal(final ByteBuffer bytes) throws HornetQException
   {
      final int bytesToWrite = factory.calculateBlockSize(bytes.limit());

      final long positionToWrite = position.getAndAdd(bytesToWrite);

      aioFile.writeInternal(positionToWrite, bytesToWrite, bytes);
   }

   // Protected methods
   // -----------------------------------------------------------------------------------------------------

   @Override
   protected ByteBuffer newBuffer(int size, int limit)
   {
      size = factory.calculateBlockSize(size);
      limit = factory.calculateBlockSize(limit);

      ByteBuffer buffer = factory.newBuffer(size);
      buffer.limit(limit);
      return buffer;
   }

   // Private methods
   // -----------------------------------------------------------------------------------------------------

   private void checkOpened()
   {
      if (aioFile == null || !opened)
      {
         throw new IllegalStateException("File not opened");
      }
   }

}
