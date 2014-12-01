/**
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
package org.apache.activemq.core.asyncio;

import java.nio.ByteBuffer;

import org.apache.activemq.api.core.ActiveMQException;

/**
 *
 * @author clebert.suconic@jboss.com
 *
 */
public interface AsynchronousFile
{
   void close() throws InterruptedException, ActiveMQException;

   /**
    *
    * Note: If you are using a native Linux implementation, maxIO can't be higher than what's defined on /proc/sys/fs/aio-max-nr, or you would get an error
    * @param fileName
    * @param maxIO The number of max concurrent asynchronous IO operations. It has to be balanced between the size of your writes and the capacity of your disk.
    * @throws org.apache.activemq.api.core.ActiveMQException
    */
   void open(String fileName, int maxIO) throws ActiveMQException;

   /**
    * Warning: This function will perform a synchronous IO, probably translating to a fstat call
    * @throws org.apache.activemq.api.core.ActiveMQException
    * */
   long size() throws ActiveMQException;

   /** Any error will be reported on the callback interface */
   void write(long position, long size, ByteBuffer directByteBuffer, AIOCallback aioCallback);

   /**
    * Performs an internal direct write.
    * @throws org.apache.activemq.api.core.ActiveMQException
    */
   void writeInternal(long positionToWrite, long size, ByteBuffer bytes) throws ActiveMQException;

   void read(long position, long size, ByteBuffer directByteBuffer, AIOCallback aioCallback) throws ActiveMQException;

   void fill(long position, int blocks, long size, byte fillChar) throws ActiveMQException;

   void setBufferCallback(BufferCallback callback);

   int getBlockSize();
}
