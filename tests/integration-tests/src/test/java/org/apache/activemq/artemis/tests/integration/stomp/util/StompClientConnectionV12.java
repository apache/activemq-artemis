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
package org.apache.activemq.artemis.tests.integration.stomp.util;

import java.io.IOException;

public class StompClientConnectionV12 extends AbstractStompClientConnection
{

   public StompClientConnectionV12(String host, int port) throws IOException
   {
      super("1.2", host, port);
   }

   @Override
   public ClientStompFrame createFrame(String command)
   {
      return factory.newFrame(command);
   }

   public ClientStompFrame connect(String username, String passcode) throws IOException, InterruptedException
   {
      ClientStompFrame frame = factory.newFrame(CONNECT_COMMAND);
      frame.addHeader(ACCEPT_HEADER, "1.2");
      frame.addHeader(HOST_HEADER, "localhost");
      if (username != null)
      {
         frame.addHeader(LOGIN_HEADER, username);
         frame.addHeader(PASSCODE_HEADER, passcode);
      }

      ClientStompFrame response = this.sendFrame(frame);

      if (response.getCommand().equals(CONNECTED_COMMAND))
      {
         String version = response.getHeader(VERSION_HEADER);
         if (!version.equals("1.2")) throw new IllegalStateException("incorrect version!");

         this.username = username;
         this.passcode = passcode;
         this.connected = true;
      }
      else
      {
         connected = false;
      }
      return response;
   }

   @Override
   public void disconnect() throws IOException, InterruptedException
   {
      stopPinger();

      ClientStompFrame frame = factory.newFrame(DISCONNECT_COMMAND);
      frame.addHeader("receipt", "1");

      ClientStompFrame result = this.sendFrame(frame);

      if (result == null || (!"RECEIPT".equals(result.getCommand())) || (!"1".equals(result.getHeader("receipt-id"))))
      {
         throw new IOException("Disconnect failed! " + result);
      }

      close();

      connected = false;
   }

   @Override
   public void connect(String username, String passcode, String clientID) throws Exception
   {
      ClientStompFrame frame = factory.newFrame(CONNECT_COMMAND);
      frame.addHeader(ACCEPT_HEADER, "1.2");
      frame.addHeader(HOST_HEADER, "localhost");
      frame.addHeader(CLIENT_ID_HEADER, clientID);

      if (username != null)
      {
         frame.addHeader(LOGIN_HEADER, username);
         frame.addHeader(PASSCODE_HEADER, passcode);
      }

      ClientStompFrame response = this.sendFrame(frame);

      if (response.getCommand().equals(CONNECTED_COMMAND))
      {
         String version = response.getHeader(VERSION_HEADER);
         if (!version.equals("1.2")) throw new IllegalStateException("incorrect version!");

         this.username = username;
         this.passcode = passcode;
         this.connected = true;
      }
      else
      {
         connected = false;
      }
   }

   public ClientStompFrame createAnyFrame(String command)
   {
      return factory.newAnyFrame(command);
   }

}
