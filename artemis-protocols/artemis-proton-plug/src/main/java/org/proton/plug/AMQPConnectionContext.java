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
package org.proton.plug;

import io.netty.buffer.ByteBuf;

public interface AMQPConnectionContext
{

   void close();

   Object getLock();

   boolean checkDataReceived();

   long getCreationTime();

   SASLResult getSASLResult();

   /**
    * Even though we are currently always sending packets asynchronsouly
    * we have a possibility to start trusting on the network flow control
    * and always sync on the send of the packet
    *
    * This is for future use and should be kept returning false.
    *
    * We will have to do some testing before we make this return true
    */
   boolean isSyncOnFlush();

   int capacity();

   /**
    * This is for the Remoting layer to push bytes on the AMQP Connection
    * The buffer readerIndex should be at the latest read byte after this method is called
    */
   void inputBuffer(ByteBuf buffer);

   void flush();

   /**
    * To be called when the bytes were sent down the stream (flushed on the socket for example)
    *
    * @param numberOfBytes
    */
   void outputDone(int numberOfBytes);


}
