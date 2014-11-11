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
package org.apache.activemq6.tests.integration.stomp.util;

import java.io.IOException;

/**
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 * pls use factory to create frames.
 */
public interface StompClientConnection
{
   ClientStompFrame sendFrame(ClientStompFrame frame) throws IOException, InterruptedException;

   ClientStompFrame receiveFrame() throws InterruptedException;

   ClientStompFrame receiveFrame(long timeout) throws InterruptedException;

   ClientStompFrame connect() throws Exception;

   void disconnect() throws IOException, InterruptedException;

   ClientStompFrame connect(String defUser, String defPass) throws Exception;

   void connect(String defUser, String defPass, String clientId) throws Exception;

   boolean isConnected();

   String getVersion();

   ClientStompFrame createFrame(String command);

   //number of frames at the queue
   int getFrameQueueSize();

   void startPinger(long interval);

   void stopPinger();

   void destroy();

   ClientStompFrame sendWickedFrame(ClientStompFrame frame) throws IOException, InterruptedException;

   int getServerPingNumber();

}

