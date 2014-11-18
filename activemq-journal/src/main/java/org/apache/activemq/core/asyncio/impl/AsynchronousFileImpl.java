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
package org.apache.activemq.core.asyncio.impl;

import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.ActiveMQExceptionType;
import org.apache.activemq.core.asyncio.AIOCallback;
import org.apache.activemq.core.asyncio.AsynchronousFile;
import org.apache.activemq.core.asyncio.BufferCallback;
import org.apache.activemq.core.asyncio.IOExceptionListener;
import org.apache.activemq.core.libaio.Native;
import org.apache.activemq.journal.HornetQJournalLogger;
import org.apache.activemq.utils.ReusableLatch;

/**
 * AsynchronousFile implementation
 *
 * @author clebert.suconic@jboss.com
 *         Warning: Case you refactor the name or the package of this class
 *         You need to make sure you also rename the C++ native calls
 */
public class AsynchronousFileImpl implements AsynchronousFile
{
   // Static ----------------------------------------------------------------------------

   private static final AtomicInteger totalMaxIO = new AtomicInteger(0);

   private static boolean loaded = false;

   /**
    * This definition needs to match Version.h on the native sources.
    * <p/>
    * Or else the native module won't be loaded because of version mismatches
    */
   private static final int EXPECTED_NATIVE_VERSION = 52;

   /**
    * Used to determine the next writing sequence
    */
   private final AtomicLong nextWritingSequence = new AtomicLong(0);

   /**
    * Used to determine the next writing sequence.
    * This is accessed from a single thread (the Poller Thread)
    */
   private long nextReadSequence = 0;

   /**
    * AIO can't guarantee ordering over callbacks.
    * <p/>
    * We use this {@link PriorityQueue} to hold values until they are in order
    */
   private final PriorityQueue<CallbackHolder> pendingCallbacks = new PriorityQueue<CallbackHolder>();

   public static void addMax(final int io)
   {
      AsynchronousFileImpl.totalMaxIO.addAndGet(io);
   }

   /**
    * For test purposes
    */
   public static int getTotalMaxIO()
   {
      return AsynchronousFileImpl.totalMaxIO.get();
   }

   public static void resetMaxAIO()
   {
      AsynchronousFileImpl.totalMaxIO.set(0);
   }

   public static int openFile(String fileName)
   {
      return Native.openFile(fileName);
   }

   public static void closeFile(int handle)
   {
      Native.closeFile(handle);
   }

   public static void destroyBuffer(ByteBuffer buffer)
   {
      Native.destroyBuffer(buffer);
   }

   private static boolean loadLibrary(final String name)
   {
      try
      {
         HornetQJournalLogger.LOGGER.trace(name + " being loaded");
         System.loadLibrary(name);
         if (Native.getNativeVersion() != AsynchronousFileImpl.EXPECTED_NATIVE_VERSION)
         {
            HornetQJournalLogger.LOGGER.incompatibleNativeLibrary();
            return false;
         }
         else
         {
            return true;
         }
      }
      catch (Throwable e)
      {
         HornetQJournalLogger.LOGGER.debug(name + " -> error loading the native library", e);
         return false;
      }

   }

   static
   {
      String[] libraries = new String[]{"activemqAIO", "activemqAIO64", "activemqAIO32", "activemqAIO_ia64"};

      for (String library : libraries)
      {
         if (AsynchronousFileImpl.loadLibrary(library))
         {
            AsynchronousFileImpl.loaded = true;
            break;
         }
         else
         {
            HornetQJournalLogger.LOGGER.debug("Library " + library + " not found!");
         }
      }

      if (!AsynchronousFileImpl.loaded)
      {
         HornetQJournalLogger.LOGGER.debug("Couldn't locate LibAIO Wrapper");
      }
   }

   public static boolean isLoaded()
   {
      return AsynchronousFileImpl.loaded;
   }

   // Attributes ------------------------------------------------------------------------

   private boolean opened = false;

   private String fileName;

   /**
    * Used while inside the callbackDone and callbackError
    */
   private final Lock callbackLock = new ReentrantLock();

   private final ReusableLatch pollerLatch = new ReusableLatch();

   private volatile Runnable poller;

   private int maxIO;

   private final Lock writeLock = new ReentrantReadWriteLock().writeLock();

   private final ReusableLatch pendingWrites = new ReusableLatch();

   private Semaphore maxIOSemaphore;

   private BufferCallback bufferCallback;

   /**
    * A callback for IO errors when they happen
    */
   private final IOExceptionListener ioExceptionListener;

   /**
    * Warning: Beware of the C++ pointer! It will bite you! :-)
    */
   private ByteBuffer handler;

   // A context switch on AIO would make it to synchronize the disk before
   // switching to the new thread, what would cause
   // serious performance problems. Because of that we make all the writes on
   // AIO using a single thread.
   private final Executor writeExecutor;

   private final Executor pollerExecutor;

   // AsynchronousFile implementation ---------------------------------------------------

   /**
    * @param writeExecutor  It needs to be a single Thread executor. If null it will use the user thread to execute write operations
    * @param pollerExecutor The thread pool that will initialize poller handlers
    */
   public AsynchronousFileImpl(final Executor writeExecutor, final Executor pollerExecutor, final IOExceptionListener ioExceptionListener)
   {
      this.writeExecutor = writeExecutor;
      this.pollerExecutor = pollerExecutor;
      this.ioExceptionListener = ioExceptionListener;
   }

   public AsynchronousFileImpl(final Executor writeExecutor, final Executor pollerExecutor)
   {
      this(writeExecutor, pollerExecutor, null);
   }

   public void open(final String fileName1, final int maxIOArgument) throws ActiveMQException
   {
      writeLock.lock();

      try
      {
         if (opened)
         {
            throw new IllegalStateException("AsynchronousFile is already opened");
         }

         this.maxIO = maxIOArgument;
         maxIOSemaphore = new Semaphore(this.maxIO);

         this.fileName = fileName1;

         try
         {
            handler = Native.init(AsynchronousFileImpl.class, fileName1, this.maxIO, HornetQJournalLogger.LOGGER);
         }
         catch (ActiveMQException e)
         {
            ActiveMQException ex = null;
            if (e.getType() == ActiveMQExceptionType.NATIVE_ERROR_CANT_INITIALIZE_AIO)
            {
               ex = new ActiveMQException(e.getType(),
                                         "Can't initialize AIO. Currently AIO in use = " + AsynchronousFileImpl.totalMaxIO.get() +
                                            ", trying to allocate more " +
                                            maxIOArgument,
                                         e);
            }
            else
            {
               ex = e;
            }
            throw ex;
         }
         opened = true;
         AsynchronousFileImpl.addMax(this.maxIO);
         nextWritingSequence.set(0);
         nextReadSequence = 0;
      }
      finally
      {
         writeLock.unlock();
      }
   }

   public void close() throws InterruptedException, ActiveMQException
   {
      checkOpened();

      writeLock.lock();

      try
      {

         while (!pendingWrites.await(60000))
         {
            HornetQJournalLogger.LOGGER.couldNotGetLock(fileName);
         }

         while (!maxIOSemaphore.tryAcquire(maxIO, 60, TimeUnit.SECONDS))
         {
            HornetQJournalLogger.LOGGER.couldNotGetLock(fileName);
         }

         maxIOSemaphore = null;
         if (poller != null)
         {
            stopPoller();
         }

         if (handler != null)
         {
            Native.closeInternal(handler);
            AsynchronousFileImpl.addMax(-maxIO);
         }
         opened = false;
         handler = null;
      }
      finally
      {
         writeLock.unlock();
      }
   }


   public void writeInternal(long positionToWrite, long size, ByteBuffer bytes) throws ActiveMQException
   {
      try
      {
         Native.writeInternal(handler, positionToWrite, size, bytes);
      }
      catch (ActiveMQException e)
      {
         fireExceptionListener(e.getType().getCode(), e.getMessage());
         throw e;
      }
      if (bufferCallback != null)
      {
         bufferCallback.bufferDone(bytes);
      }
   }


   public void write(final long position,
                     final long size,
                     final ByteBuffer directByteBuffer,
                     final AIOCallback aioCallback)
   {
      if (aioCallback == null)
      {
         throw new NullPointerException("Null Callback");
      }

      checkOpened();
      if (poller == null)
      {
         startPoller();
      }

      pendingWrites.countUp();

      if (writeExecutor != null)
      {
         maxIOSemaphore.acquireUninterruptibly();

         writeExecutor.execute(new Runnable()
         {
            public void run()
            {
               long sequence = nextWritingSequence.getAndIncrement();

               try
               {
                  Native.write(AsynchronousFileImpl.this, handler, sequence, position, size, directByteBuffer, aioCallback);
               }
               catch (ActiveMQException e)
               {
                  callbackError(aioCallback, sequence, directByteBuffer, e.getType().getCode(), e.getMessage());
               }
               catch (RuntimeException e)
               {
                  callbackError(aioCallback,
                                sequence,
                                directByteBuffer,
                                ActiveMQExceptionType.INTERNAL_ERROR.getCode(),
                                e.getMessage());
               }
            }
         });
      }
      else
      {
         maxIOSemaphore.acquireUninterruptibly();

         long sequence = nextWritingSequence.getAndIncrement();

         try
         {
            Native.write(this, handler, sequence, position, size, directByteBuffer, aioCallback);
         }
         catch (ActiveMQException e)
         {
            callbackError(aioCallback, sequence, directByteBuffer, e.getType().getCode(), e.getMessage());
         }
         catch (RuntimeException e)
         {
            callbackError(aioCallback, sequence, directByteBuffer, ActiveMQExceptionType.INTERNAL_ERROR.getCode(), e.getMessage());
         }
      }

   }

   public void read(final long position,
                    final long size,
                    final ByteBuffer directByteBuffer,
                    final AIOCallback aioPackage) throws ActiveMQException
   {
      checkOpened();
      if (poller == null)
      {
         startPoller();
      }
      pendingWrites.countUp();
      maxIOSemaphore.acquireUninterruptibly();
      try
      {
         Native.read(this, handler, position, size, directByteBuffer, aioPackage);
      }
      catch (ActiveMQException e)
      {
         // Release only if an exception happened
         maxIOSemaphore.release();
         pendingWrites.countDown();
         throw e;
      }
      catch (RuntimeException e)
      {
         // Release only if an exception happened
         maxIOSemaphore.release();
         pendingWrites.countDown();
         throw e;
      }
   }

   public long size() throws ActiveMQException
   {
      checkOpened();
      return Native.size0(handler);
   }

   public void fill(final long position, final int blocks, final long size, final byte fillChar) throws ActiveMQException
   {
      checkOpened();
      try
      {
         Native.fill(handler, position, blocks, size, fillChar);
      }
      catch (ActiveMQException e)
      {
         fireExceptionListener(e.getType().getCode(), e.getMessage());
         throw e;
      }
   }

   public int getBlockSize()
   {
      return 512;
   }

   /**
    * This needs to be synchronized because of
    * http://bugs.sun.com/view_bug.do?bug_id=6791815
    * http://mail.openjdk.java.net/pipermail/hotspot-runtime-dev/2009-January/000386.html
    */
   public static synchronized ByteBuffer newBuffer(final int size)
   {
      if (size % 512 != 0)
      {
         throw new RuntimeException("Buffer size needs to be aligned to 512");
      }

      return Native.newNativeBuffer(size);
   }

   public void setBufferCallback(final BufferCallback callback)
   {
      bufferCallback = callback;
   }

   /**
    * Return the JNI handler used on C++
    */
   public ByteBuffer getHandler()
   {
      return handler;
   }

   public static void clearBuffer(final ByteBuffer buffer)
   {
      Native.resetBuffer(buffer, buffer.limit());
      buffer.position(0);
   }

   // Protected -------------------------------------------------------------------------

   @Override
   protected void finalize()
   {
      if (opened)
      {
         HornetQJournalLogger.LOGGER.fileFinalizedWhileOpen(fileName);
      }
   }

   // Private ---------------------------------------------------------------------------

   private void callbackDone(final AIOCallback callback, final long sequence, final ByteBuffer buffer)
   {
      maxIOSemaphore.release();

      pendingWrites.countDown();

      callbackLock.lock();

      try
      {

         if (sequence == -1)
         {
            callback.done();
         }
         else
         {
            if (sequence == nextReadSequence)
            {
               nextReadSequence++;
               callback.done();
               flushCallbacks();
            }
            else
            {
               pendingCallbacks.add(new CallbackHolder(sequence, callback));
            }
         }

         // The buffer is not sent on callback for read operations
         if (bufferCallback != null && buffer != null)
         {
            bufferCallback.bufferDone(buffer);
         }
      }
      finally
      {
         callbackLock.unlock();
      }
   }

   private void flushCallbacks()
   {
      while (!pendingCallbacks.isEmpty() && pendingCallbacks.peek().sequence == nextReadSequence)
      {
         CallbackHolder holder = pendingCallbacks.poll();
         if (holder.isError())
         {
            ErrorCallback error = (ErrorCallback) holder;
            holder.callback.onError(error.errorCode, error.message);
         }
         else
         {
            holder.callback.done();
         }
         nextReadSequence++;
      }
   }

   // Called by the JNI layer.. just ignore the
   // warning
   private void callbackError(final AIOCallback callback,
                              final long sequence,
                              final ByteBuffer buffer,
                              final int errorCode,
                              final String errorMessage)
   {
      HornetQJournalLogger.LOGGER.callbackError(errorMessage);

      fireExceptionListener(errorCode, errorMessage);

      maxIOSemaphore.release();

      pendingWrites.countDown();

      callbackLock.lock();

      try
      {
         if (sequence == -1)
         {
            callback.onError(errorCode, errorMessage);
         }
         else
         {
            if (sequence == nextReadSequence)
            {
               nextReadSequence++;
               callback.onError(errorCode, errorMessage);
               flushCallbacks();
            }
            else
            {
               pendingCallbacks.add(new ErrorCallback(sequence, callback, errorCode, errorMessage));
            }
         }
      }
      finally
      {
         callbackLock.unlock();
      }

      // The buffer is not sent on callback for read operations
      if (bufferCallback != null && buffer != null)
      {
         bufferCallback.bufferDone(buffer);
      }
   }

   /**
    * This is called by the native layer
    *
    * @param errorCode
    * @param errorMessage
    */
   private void fireExceptionListener(final int errorCode, final String errorMessage)
   {
      HornetQJournalLogger.LOGGER.ioError(errorCode, errorMessage);
      if (ioExceptionListener != null)
      {
         ioExceptionListener.onIOException(ActiveMQExceptionType.getType(errorCode).createException(errorMessage), errorMessage);
      }
   }

   private void pollEvents()
   {
      if (!opened)
      {
         return;
      }
      Native.internalPollEvents(handler);
   }

   private void startPoller()
   {
      writeLock.lock();

      try
      {

         if (poller == null)
         {
            pollerLatch.countUp();
            poller = new PollerRunnable();
            try
            {
               pollerExecutor.execute(poller);
            }
            catch (Exception ex)
            {
               HornetQJournalLogger.LOGGER.errorStartingPoller(ex);
            }
         }
      }
      finally
      {
         writeLock.unlock();
      }
   }

   private void checkOpened()
   {
      if (!opened)
      {
         throw new RuntimeException("File is not opened");
      }
   }

   /**
    * @throws org.apache.activemq.api.core.ActiveMQException
    * @throws InterruptedException
    */
   private void stopPoller() throws ActiveMQException, InterruptedException
   {
      Native.stopPoller(handler);
      // We need to make sure we won't call close until Poller is
      // completely done, or we might get beautiful GPFs
      pollerLatch.await();
   }

   public static FileLock lock(int handle)
   {
      if (Native.flock(handle))
      {
         return new HornetQFileLock(handle);
      }
      else
      {
         return null;
      }
   }

   // Native ----------------------------------------------------------------------------


   /**
    * Explicitly adding a compare to clause that returns 0 for at least the same object.
    * <p/>
    * If {@link Comparable#compareTo(Object)} does not return 0 -for at least the same object- some
    * Collection classes methods will fail (example {@link PriorityQueue#remove(Object)}. If it
    * returns 0, then {@link #equals(Object)} must return {@code true} for the exact same cases,
    * otherwise we will get compatibility problems between Java5 and Java6.
    */
   private static class CallbackHolder implements Comparable<CallbackHolder>
   {
      final long sequence;

      final AIOCallback callback;

      public boolean isError()
      {
         return false;
      }

      public CallbackHolder(final long sequence, final AIOCallback callback)
      {
         this.sequence = sequence;
         this.callback = callback;
      }

      public int compareTo(final CallbackHolder o)
      {
         // It shouldn't be equals in any case
         if (this == o)
            return 0;
         if (sequence <= o.sequence)
         {
            return -1;
         }
         else
         {
            return 1;
         }
      }

      /**
       * See {@link CallbackHolder}.
       */
      @Override
      public int hashCode()
      {
         return super.hashCode();
      }

      /**
       * See {@link CallbackHolder}.
       */
      @Override
      public boolean equals(Object obj)
      {
         return super.equals(obj);
      }
   }

   private static final class ErrorCallback extends CallbackHolder
   {
      final int errorCode;

      final String message;

      @Override
      public boolean isError()
      {
         return true;
      }

      public ErrorCallback(final long sequence, final AIOCallback callback, final int errorCode, final String message)
      {
         super(sequence, callback);

         this.errorCode = errorCode;

         this.message = message;
      }

      /**
       * See {@link CallbackHolder}.
       */
      @Override
      public int hashCode()
      {
         return super.hashCode();
      }

      /**
       * See {@link CallbackHolder}.
       */
      @Override
      public boolean equals(Object obj)
      {
         return super.equals(obj);
      }
   }

   private class PollerRunnable implements Runnable
   {
      PollerRunnable()
      {
      }

      public void run()
      {
         try
         {
            pollEvents();
         }
         finally
         {
            // This gives us extra protection in cases of interruption
            // Case the poller thread is interrupted, this will allow us to
            // restart the thread when required
            poller = null;
            pollerLatch.countDown();
         }
      }
   }

}
