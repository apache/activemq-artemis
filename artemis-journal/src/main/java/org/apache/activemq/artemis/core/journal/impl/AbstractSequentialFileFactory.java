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
package org.apache.activemq.artemis.core.journal.impl;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.core.journal.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.journal.SequentialFile;
import org.apache.activemq.artemis.core.journal.SequentialFileFactory;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;

/**
 * An abstract SequentialFileFactory containing basic functionality for both AIO and NIO SequentialFactories
 */
abstract class AbstractSequentialFileFactory implements SequentialFileFactory
{

   // Timeout used to wait executors to shutdown
   protected static final int EXECUTOR_TIMEOUT = 60;

   protected final String journalDir;

   protected final TimedBuffer timedBuffer;

   protected final int bufferSize;

   protected final long bufferTimeout;

   private final IOCriticalErrorListener critialErrorListener;

   /**
    * Asynchronous writes need to be done at another executor.
    * This needs to be done at NIO, or else we would have the callers thread blocking for the return.
    * At AIO this is necessary as context switches on writes would fire flushes at the kernel.
    *  */
   protected ExecutorService writeExecutor;

   AbstractSequentialFileFactory(final String journalDir,
                                        final boolean buffered,
                                        final int bufferSize,
                                        final int bufferTimeout,
                                        final boolean logRates,
                                        final IOCriticalErrorListener criticalErrorListener)
   {
      this.journalDir = journalDir;

      if (buffered)
      {
         timedBuffer = new TimedBuffer(bufferSize, bufferTimeout, logRates);
      }
      else
      {
         timedBuffer = null;
      }
      this.bufferSize = bufferSize;
      this.bufferTimeout = bufferTimeout;
      this.critialErrorListener = criticalErrorListener;
   }

   public void stop()
   {
      if (timedBuffer != null)
      {
         timedBuffer.stop();
      }

      if (isSupportsCallbacks() && writeExecutor != null)
      {
         writeExecutor.shutdown();

         try
         {
            if (!writeExecutor.awaitTermination(AbstractSequentialFileFactory.EXECUTOR_TIMEOUT, TimeUnit.SECONDS))
            {
               ActiveMQJournalLogger.LOGGER.timeoutOnWriterShutdown(new Exception("trace"));
            }
         }
         catch (InterruptedException e)
         {
            throw new ActiveMQInterruptedException(e);
         }
      }
   }

   public String getDirectory()
   {
      return journalDir;
   }

   public void start()
   {
      if (timedBuffer != null)
      {
         timedBuffer.start();
      }

      if (isSupportsCallbacks())
      {
         writeExecutor = Executors.newSingleThreadExecutor(new ActiveMQThreadFactory("ActiveMQ-Asynchronous-Persistent-Writes" + System.identityHashCode(this),
                                                                                    true,
                                                                                    AbstractSequentialFileFactory.getThisClassLoader()));
      }

   }

   @Override
   public void onIOError(Exception exception, String message, SequentialFile file)
   {
      if (critialErrorListener != null)
      {
         critialErrorListener.onIOException(exception, message, file);
      }
   }

   @Override
   public void activateBuffer(final SequentialFile file)
   {
      if (timedBuffer != null)
      {
         file.setTimedBuffer(timedBuffer);
      }
   }

   public void flush()
   {
      if (timedBuffer != null)
      {
         timedBuffer.flush();
      }
   }

   public void deactivateBuffer()
   {
      if (timedBuffer != null)
      {
         // When moving to a new file, we need to make sure any pending buffer will be transferred to the buffer
         timedBuffer.flush();
         timedBuffer.setObserver(null);
      }
   }

   public void releaseBuffer(final ByteBuffer buffer)
   {
   }

   /**
    * Create the directory if it doesn't exist yet
    */
   public void createDirs() throws Exception
   {
      File file = new File(journalDir);
      boolean ok = file.mkdirs();
      if (!ok)
      {
         throw new IOException("Failed to create directory " + journalDir);
      }
   }

   public List<String> listFiles(final String extension) throws Exception
   {
      File dir = new File(journalDir);

      FilenameFilter fnf = new FilenameFilter()
      {
         public boolean accept(final File file, final String name)
         {
            return name.endsWith("." + extension);
         }
      };

      String[] fileNames = dir.list(fnf);

      if (fileNames == null)
      {
         return Collections.EMPTY_LIST;
      }

      return Arrays.asList(fileNames);
   }

   private static ClassLoader getThisClassLoader()
   {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
      {
         public ClassLoader run()
         {
            return AbstractSequentialFileFactory.class.getClassLoader();
         }
      });

   }

}
