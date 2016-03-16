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
package org.proton.plug.handler;

import java.util.concurrent.Executor;

import io.netty.buffer.ByteBuf;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Transport;
import org.proton.plug.ClientSASL;
import org.proton.plug.ServerSASL;
import org.proton.plug.SASLResult;
import org.proton.plug.handler.impl.ProtonHandlerImpl;

/**
 * This is a definition of the public interface for {@link org.proton.plug.handler.impl.ProtonHandlerImpl}
 */
public interface ProtonHandler {

   long tick(boolean firstTick);

   final class Factory {
      public static ProtonHandler create(Executor dispatchExecutor) {
         return new ProtonHandlerImpl(dispatchExecutor);
      }
   }

   /**
    * It returns true if the transport connection has any capacity available
    *
    * @return
    */
   int capacity();

   Transport getTransport();

   Connection getConnection();

   /**
    * Add an event handler to the chain
    *
    * @param handler
    * @return
    */
   ProtonHandler addEventHandler(EventHandler handler);

   void createClientSasl(ClientSASL clientSASL);

   /**
    * To be used on server connections. To define SASL integration.
    *
    * @param handlers
    */
   void createServerSASL(ServerSASL[] handlers);

   /**
    * To return the SASL Mechanism that was successful with the connection.
    * This should contain any state such as user and password
    *
    * @return
    */
   SASLResult getSASLResult();

   /**
    * The input on the Handler.
    * Notice that buffer will be positioned up to where we needed
    *
    * @param buffer
    */
   void inputBuffer(ByteBuf buffer);

   /**
    * To be used at your discretion to verify if the client was active since you last checked
    * it can be used to implement server TTL cleanup and verifications
    *
    * @return
    */
   boolean checkDataReceived();

   /**
    * Return the creation time of the handler
    *
    * @return
    */
   long getCreationTime();

   /**
    * To be called after you used the outputBuffer
    *
    * @param bytes number of bytes you used already on the output
    */
   void outputDone(int bytes);

   /**
    * it will return pending bytes you have on the Transport
    * after you are done with it you must call {@link #outputDone(int)}
    *
    * @return
    */
   ByteBuf outputBuffer();

   /**
    * It will process the transport and cause events to be called
    */
   void flush();

   /**
    * It will close the connection and flush events
    */
   void close();

   /**
    * Get the object used to lock transport, connection and events operations
    *
    * @return
    */
   Object getLock();

}
