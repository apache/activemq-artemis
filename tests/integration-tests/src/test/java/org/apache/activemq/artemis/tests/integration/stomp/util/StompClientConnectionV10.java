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
package org.apache.activemq.artemis.tests.integration.stomp.util;

import java.io.IOException;

/**
 * pls use factory to create frames.
 */
public class StompClientConnectionV10 extends AbstractStompClientConnection {

   public StompClientConnectionV10(String host, int port) throws IOException {
      super("1.0", host, port);
   }

   @Override
   public ClientStompFrame connect(String username, String passcode) throws IOException, InterruptedException {
      ClientStompFrame frame = factory.newFrame(CONNECT_COMMAND);
      frame.addHeader(LOGIN_HEADER, username);
      frame.addHeader(PASSCODE_HEADER, passcode);

      ClientStompFrame response = this.sendFrame(frame);

      if (response.getCommand().equals(CONNECTED_COMMAND)) {
         connected = true;
      }
      else {
         System.out.println("Connection failed with: " + response);
         connected = false;
      }
      return response;
   }

   @Override
   public void connect(String username, String passcode, String clientID) throws IOException, InterruptedException {
      ClientStompFrame frame = factory.newFrame(CONNECT_COMMAND);
      frame.addHeader(LOGIN_HEADER, username);
      frame.addHeader(PASSCODE_HEADER, passcode);
      frame.addHeader(CLIENT_ID_HEADER, clientID);

      ClientStompFrame response = this.sendFrame(frame);

      if (response.getCommand().equals(CONNECTED_COMMAND)) {
         connected = true;
      }
      else {
         System.out.println("Connection failed with: " + response);
         connected = false;
      }
   }

   @Override
   public void disconnect() throws IOException, InterruptedException {
      ClientStompFrame frame = factory.newFrame(DISCONNECT_COMMAND);
      this.sendFrame(frame);

      close();

      connected = false;
   }

   @Override
   public ClientStompFrame createFrame(String command) {
      return new ClientStompFrameV10(command);
   }

   @Override
   public void startPinger(long interval) {
   }

   @Override
   public void stopPinger() {
   }

   @Override
   public int getServerPingNumber() {
      return 0;
   }
}
