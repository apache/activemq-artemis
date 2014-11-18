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
package org.apache.activemq.core.libaio;

import java.nio.ByteBuffer;

import org.apache.activemq.api.core.ActiveMQException;

/**
 * @author Clebert Suconic
 */

public class Native
{
   // Functions used for locking files .....
   public static native int openFile(String fileName);

   public static native void closeFile(int handle);

   public static native boolean flock(int handle);
   // Functions used for locking files ^^^^^^^^

   public static native void resetBuffer(ByteBuffer directByteBuffer, int size);

   public static native void destroyBuffer(ByteBuffer buffer);

   public static native ByteBuffer newNativeBuffer(long size);

   public static native void newInit(Class someClass);

   public static native ByteBuffer init(Class controllerClass, String fileName, int maxIO, Object logger) throws ActiveMQException;

   public static native long size0(ByteBuffer handle);

   public static native void write(Object thisObject, ByteBuffer handle,
                             long sequence,
                             long position,
                             long size,
                             ByteBuffer buffer,
                             Object aioPackageCallback) throws ActiveMQException;

   /** a direct write to the file without the use of libaio's submit. */
   public static native void writeInternal(ByteBuffer handle, long positionToWrite, long size, ByteBuffer bytes) throws ActiveMQException;

   /**
    *This is using org.apache.activemq.core.asyncio.AIOCallback
     */
   public static native void read(Object thisObject, ByteBuffer handle, long position, long size, ByteBuffer buffer, Object aioPackageCallback) throws ActiveMQException;

   public static native void fill(ByteBuffer handle, long position, int blocks, long size, byte fillChar) throws ActiveMQException;

   public static native void closeInternal(ByteBuffer handler);

   public static native void stopPoller(ByteBuffer handler);

   /** A native method that does nothing, and just validate if the ELF dependencies are loaded and on the correct platform as this binary format */
   public static native int getNativeVersion();

   /** Poll asynchronous events from internal queues */
   public static native void internalPollEvents(ByteBuffer handler);

   // Inner classes ---------------------------------------------------------------------

}
