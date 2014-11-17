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
package org.apache.activemq.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.activemq.api.core.HornetQBuffer;

/**
 * Used to send large messages
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class HornetQBufferInputStream extends InputStream
{

   /* (non-Javadoc)
    * @see java.io.InputStream#read()
    */
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   private HornetQBuffer bb;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public HornetQBufferInputStream(final HornetQBuffer paramByteBuffer)
   {
      bb = paramByteBuffer;
   }

   @Override
   public int read() throws IOException
   {
      if (bb == null)
      {
         throw new IOException("read on a closed InputStream");
      }

      if (remainingBytes() == 0)
      {
         return -1;
      }
      else
      {
         return bb.readByte() & 0xFF;
      }
   }

   @Override
   public int read(final byte[] byteArray) throws IOException
   {
      if (bb == null)
      {
         throw new IOException("read on a closed InputStream");
      }

      return read(byteArray, 0, byteArray.length);
   }

   @Override
   public int read(final byte[] byteArray, final int off, final int len) throws IOException
   {
      if (bb == null)
      {
         throw new IOException("read on a closed InputStream");
      }

      if (byteArray == null)
      {
         throw new NullPointerException();
      }
      if (off < 0 || off > byteArray.length || len < 0 || off + len > byteArray.length || off + len < 0)
      {
         throw new IndexOutOfBoundsException();
      }
      if (len == 0)
      {
         return 0;
      }

      int size = Math.min(remainingBytes(), len);

      if (size == 0)
      {
         return -1;
      }

      bb.readBytes(byteArray, off, size);
      return size;
   }

   @Override
   public long skip(final long len) throws IOException
   {
      if (bb == null)
      {
         throw new IOException("skip on a closed InputStream");
      }

      if (len <= 0L)
      {
         return 0L;
      }

      int size = Math.min(remainingBytes(), (int) len);

      bb.skipBytes(size);

      return size;
   }

   @Override
   public int available() throws IOException
   {
      if (bb == null)
      {
         throw new IOException("available on a closed InputStream");
      }

      return remainingBytes();
   }

   @Override
   public void close() throws IOException
   {
      bb = null;
   }

   @Override
   public synchronized void mark(final int paramInt)
   {
   }

   @Override
   public synchronized void reset() throws IOException
   {
      throw new IOException("mark/reset not supported");
   }

   @Override
   public boolean markSupported()
   {
      return false;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * @return
    */
   private int remainingBytes()
   {
      return bb.writerIndex() - bb.readerIndex();
   }


   // Inner classes -------------------------------------------------

}
