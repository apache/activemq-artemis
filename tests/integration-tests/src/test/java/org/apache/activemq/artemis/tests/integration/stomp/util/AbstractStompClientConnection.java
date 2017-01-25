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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.protocol.stomp.Stomp;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;

public abstract class AbstractStompClientConnection implements StompClientConnection {

   protected Pinger pinger;
   protected String version;
   protected String host;
   protected int port;
   protected String username;
   protected String passcode;
   protected StompFrameFactory factory;
   protected final SocketChannel socketChannel;
   protected ByteBuffer readBuffer;
   protected List<Byte> receiveList;
   protected BlockingQueue<ClientStompFrame> frameQueue = new LinkedBlockingQueue<>();
   protected boolean connected = false;
   protected int serverPingCounter;
   protected ReaderThread readerThread;

   public AbstractStompClientConnection(String version, String host, int port) throws IOException {
      this.version = version;
      this.host = host;
      this.port = port;
      this.factory = StompFrameFactoryFactory.getFactory(version);
      socketChannel = SocketChannel.open();
      initSocket();
   }

   private void initSocket() throws IOException {
      socketChannel.configureBlocking(true);
      InetSocketAddress remoteAddr = new InetSocketAddress(host, port);
      socketChannel.connect(remoteAddr);

      startReaderThread();
   }

   private void startReaderThread() {
      readBuffer = ByteBuffer.allocateDirect(10240);
      receiveList = new ArrayList<>(10240);

      readerThread = new ReaderThread();
      readerThread.start();
   }

   public void killReaderThread() {
      readerThread.stop();
   }

   private ClientStompFrame sendFrameInternal(ClientStompFrame frame, boolean wicked) throws IOException, InterruptedException {
      ClientStompFrame response = null;
      IntegrationTestLogger.LOGGER.info("Sending " + (wicked ? "*wicked* " : "") + "frame:\n" + frame);
      ByteBuffer buffer;
      if (wicked) {
         buffer = frame.toByteBufferWithExtra("\n");
      } else {
         buffer = frame.toByteBuffer();
      }
      while (buffer.remaining() > 0) {
         socketChannel.write(buffer);
      }

      //now response
      if (frame.needsReply()) {
         response = receiveFrame();

         //filter out server ping
         while (response != null) {
            if (response.getCommand().equals(Stomp.Commands.STOMP)) {
               response = receiveFrame();
            } else {
               break;
            }
         }
      }

      IntegrationTestLogger.LOGGER.info("Received:\n" + response);

      return response;
   }

   @Override
   public ClientStompFrame sendFrame(ClientStompFrame frame) throws IOException, InterruptedException {
      return sendFrameInternal(frame, false);
   }

   @Override
   public ClientStompFrame sendWickedFrame(ClientStompFrame frame) throws IOException, InterruptedException {
      return sendFrameInternal(frame, true);
   }

   @Override
   public ClientStompFrame receiveFrame() throws InterruptedException {
      return frameQueue.poll(10, TimeUnit.SECONDS);
   }

   @Override
   public ClientStompFrame receiveFrame(long timeout) throws InterruptedException {
      return frameQueue.poll(timeout, TimeUnit.MILLISECONDS);
   }

   //put bytes to byte array.
   private void receiveBytes(int n) {
      readBuffer.rewind();
      for (int i = 0; i < n; i++) {
         byte b = readBuffer.get();
         if (b == 0) {
            //a new frame got.
            int sz = receiveList.size();
            if (sz > 0) {
               byte[] frameBytes = new byte[sz];
               for (int j = 0; j < sz; j++) {
                  frameBytes[j] = receiveList.get(j);
               }
               ClientStompFrame frame = factory.createFrame(new String(frameBytes, StandardCharsets.UTF_8));

               if (validateFrame(frame)) {
                  frameQueue.offer(frame);
                  receiveList.clear();
               } else {
                  receiveList.add(b);
               }
            }
         } else {
            if (b == 10 && receiveList.size() == 0) {
               //may be a ping
               incrementServerPing();
            } else {
               receiveList.add(b);
            }
         }
      }
      //clear readbuffer
      readBuffer.rewind();
   }

   protected void incrementServerPing() {
      serverPingCounter++;
   }

   private boolean validateFrame(ClientStompFrame f) {
      String h = f.getHeader(Stomp.Headers.CONTENT_LENGTH);
      if (h != null) {
         int len = Integer.valueOf(h);
         if (f.getBody().getBytes(StandardCharsets.UTF_8).length < len) {
            return false;
         }
      }
      return true;
   }

   protected void close() throws IOException {
      socketChannel.close();
   }

   private class ReaderThread extends Thread {

      @Override
      public void run() {
         try {
            int n = socketChannel.read(readBuffer);

            while (n >= 0) {
               if (n > 0) {
                  receiveBytes(n);
               }
               n = socketChannel.read(readBuffer);
            }
            //peer closed
            close();

         } catch (IOException e) {
            try {
               close();
            } catch (IOException e1) {
               //ignore
            }
         }
      }
   }

   @Override
   public ClientStompFrame connect() throws Exception {
      return connect(null, null);
   }

   @Override
   public void destroy() {
      try {
         close();
      } catch (IOException e) {
      } finally {
         this.connected = false;
      }
   }

   @Override
   public ClientStompFrame connect(String username, String password) throws Exception {
      throw new RuntimeException("connect method not implemented!");
   }

   @Override
   public boolean isConnected() {
      return connected && socketChannel.isConnected();
   }

   @Override
   public String getVersion() {
      return version;
   }

   @Override
   public int getFrameQueueSize() {
      return this.frameQueue.size();
   }

   protected class Pinger extends Thread {

      long pingInterval;
      ClientStompFrame pingFrame;
      volatile boolean stop = false;

      Pinger(long interval) {
         this.pingInterval = interval;
         pingFrame = createFrame(Stomp.Commands.STOMP);
         pingFrame.setBody("\n");
         pingFrame.setForceOneway();
         pingFrame.setPing(true);
      }

      public void startPing() {
         start();
      }

      public synchronized void stopPing() {
         stop = true;
         this.notify();
      }

      @Override
      public void run() {
         synchronized (this) {
            while (!stop) {
               try {
                  sendFrame(pingFrame);

                  this.wait(pingInterval);
               } catch (Exception e) {
                  stop = true;
                  e.printStackTrace();
               }
            }
         }
      }
   }
}
