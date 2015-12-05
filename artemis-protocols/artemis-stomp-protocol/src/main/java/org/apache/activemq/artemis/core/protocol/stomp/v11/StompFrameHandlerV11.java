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
package org.apache.activemq.artemis.core.protocol.stomp.v11;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.core.protocol.stomp.ActiveMQStompException;
import org.apache.activemq.artemis.core.protocol.stomp.FrameEventListener;
import org.apache.activemq.artemis.core.protocol.stomp.SimpleBytes;
import org.apache.activemq.artemis.core.protocol.stomp.Stomp;
import org.apache.activemq.artemis.core.protocol.stomp.StompConnection;
import org.apache.activemq.artemis.core.protocol.stomp.StompDecoder;
import org.apache.activemq.artemis.core.protocol.stomp.StompFrame;
import org.apache.activemq.artemis.core.protocol.stomp.VersionedStompFrameHandler;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;

import static org.apache.activemq.artemis.core.protocol.stomp.ActiveMQStompProtocolMessageBundle.BUNDLE;

public class StompFrameHandlerV11 extends VersionedStompFrameHandler implements FrameEventListener {

   protected static final char ESC_CHAR = '\\';

   private HeartBeater heartBeater;

   public StompFrameHandlerV11(StompConnection connection) {
      super(connection);
      connection.addStompEventListener(this);
      decoder = new StompDecoderV11(this);
      decoder.init();
   }

   @Override
   public StompFrame onConnect(StompFrame frame) {
      StompFrame response = null;
      Map<String, String> headers = frame.getHeadersMap();
      String login = headers.get(Stomp.Headers.Connect.LOGIN);
      String passcode = headers.get(Stomp.Headers.Connect.PASSCODE);
      String clientID = headers.get(Stomp.Headers.Connect.CLIENT_ID);
      String requestID = headers.get(Stomp.Headers.Connect.REQUEST_ID);

      try {
         if (connection.validateUser(login, passcode)) {
            connection.setClientID(clientID);
            connection.setValid(true);

            response = this.createStompFrame(Stomp.Responses.CONNECTED);

            // version
            response.addHeader(Stomp.Headers.Connected.VERSION, connection.getVersion());

            // session
            response.addHeader(Stomp.Headers.Connected.SESSION, connection.getID().toString());

            // server
            response.addHeader(Stomp.Headers.Connected.SERVER, connection.getActiveMQServerName());

            if (requestID != null) {
               response.addHeader(Stomp.Headers.Connected.RESPONSE_ID, requestID);
            }

            // heart-beat. We need to start after connected frame has been sent.
            // otherwise the client may receive heart-beat before it receives
            // connected frame.
            String heartBeat = headers.get(Stomp.Headers.Connect.HEART_BEAT);

            if (heartBeat != null) {
               handleHeartBeat(heartBeat);
               if (heartBeater == null) {
                  response.addHeader(Stomp.Headers.Connected.HEART_BEAT, "0,0");
               }
               else {
                  response.addHeader(Stomp.Headers.Connected.HEART_BEAT, heartBeater.getServerHeartBeatValue());
               }
            }
         }
         else {
            // not valid
            response = createStompFrame(Stomp.Responses.ERROR);
            response.setNeedsDisconnect(true);
            response.addHeader(Stomp.Headers.CONTENT_TYPE, "text/plain");
            response.addHeader(Stomp.Headers.Error.MESSAGE, "Failed to connect");
            response.setBody("The login account is not valid.");
         }
      }
      catch (ActiveMQStompException e) {
         response = e.getFrame();
      }

      return response;
   }

   //ping parameters, hard-code for now
   //the server can support min 20 milliseconds and receive ping at 100 milliseconds (20,100)
   private void handleHeartBeat(String heartBeatHeader) throws ActiveMQStompException {
      String[] params = heartBeatHeader.split(",");
      if (params.length != 2) {
         throw new ActiveMQStompException(connection, "Incorrect heartbeat header " + heartBeatHeader);
      }

      //client ping
      long minPingInterval = Long.valueOf(params[0]);
      //client receive ping
      long minAcceptInterval = Long.valueOf(params[1]);

      if ((minPingInterval != 0) || (minAcceptInterval != 0)) {
         heartBeater = new HeartBeater(minPingInterval, minAcceptInterval);
      }
   }

   @Override
   public StompFrame onDisconnect(StompFrame frame) {
      if (this.heartBeater != null) {
         heartBeater.shutdown();
         try {
            heartBeater.join();
         }
         catch (InterruptedException e) {
            ActiveMQServerLogger.LOGGER.errorOnStompHeartBeat(e);
         }
      }
      return null;
   }

   @Override
   public StompFrame onUnsubscribe(StompFrame request) {
      StompFrame response = null;
      //unsubscribe in 1.1 only needs id header
      String id = request.getHeader(Stomp.Headers.Unsubscribe.ID);
      String durableSubscriberName = request.getHeader(Stomp.Headers.Unsubscribe.DURABLE_SUBSCRIBER_NAME);

      String subscriptionID = null;
      if (id != null) {
         subscriptionID = id;
      }
      else {
         response = BUNDLE.needSubscriptionID().setHandler(this).getFrame();
         return response;
      }

      try {
         connection.unsubscribe(subscriptionID, durableSubscriberName);
      }
      catch (ActiveMQStompException e) {
         response = e.getFrame();
      }
      return response;
   }

   @Override
   public StompFrame onAck(StompFrame request) {
      StompFrame response = null;

      String messageID = request.getHeader(Stomp.Headers.Ack.MESSAGE_ID);
      String txID = request.getHeader(Stomp.Headers.TRANSACTION);
      String subscriptionID = request.getHeader(Stomp.Headers.Ack.SUBSCRIPTION);

      if (txID != null) {
         ActiveMQServerLogger.LOGGER.stompTXAckNorSupported();
      }

      if (subscriptionID == null) {
         response = BUNDLE.needSubscriptionID().setHandler(this).getFrame();
         return response;
      }

      try {
         connection.acknowledge(messageID, subscriptionID);
      }
      catch (ActiveMQStompException e) {
         response = e.getFrame();
      }

      return response;
   }

   @Override
   public StompFrame onStomp(StompFrame request) {
      if (!connection.isValid()) {
         return onConnect(request);
      }
      return null;
   }

   @Override
   public StompFrame onNack(StompFrame request) {
      //this eventually means discard the message (it never be redelivered again).
      //we can consider supporting redeliver to a different sub.
      return onAck(request);
   }

   @Override
   public void replySent(StompFrame reply) {
      if (reply.getCommand().equals(Stomp.Responses.CONNECTED)) {
         //kick off the pinger
         startHeartBeat();
      }

      if (reply.needsDisconnect()) {
         connection.disconnect(false);
      }
      else {
         //update ping
         if (heartBeater != null) {
            heartBeater.pinged();
         }
      }
   }

   private void startHeartBeat() {
      if (heartBeater != null) {
         heartBeater.start();
      }
   }

   public StompFrame createPingFrame() {
      StompFrame frame = createStompFrame(Stomp.Commands.STOMP);
      frame.setPing(true);
      return frame;
   }

   //server heart beat
   //algorithm:
   //(a) server ping: if server hasn't sent any frame within serverPing
   //interval, send a ping.
   //(b) accept ping: if server hasn't received any frame within
   // 2*serverAcceptPing, disconnect!
   private class HeartBeater extends Thread {

      private static final int MIN_SERVER_PING = 500;
      private static final int MIN_CLIENT_PING = 500;

      long serverPing = 0;
      long serverAcceptPing = 0;
      volatile boolean shutdown = false;
      AtomicLong lastPingTime = new AtomicLong(0);
      AtomicLong lastAccepted = new AtomicLong(0);
      StompFrame pingFrame;

      public HeartBeater(long clientPing, long clientAcceptPing) {
         if (clientPing != 0) {
            serverAcceptPing = clientPing > MIN_CLIENT_PING ? clientPing : MIN_CLIENT_PING;
         }

         if (clientAcceptPing != 0) {
            serverPing = clientAcceptPing > MIN_SERVER_PING ? clientAcceptPing : MIN_SERVER_PING;
         }
      }

      public synchronized void shutdown() {
         shutdown = true;
         this.notify();
      }

      public String getServerHeartBeatValue() {
         return String.valueOf(serverPing) + "," + String.valueOf(serverAcceptPing);
      }

      public void pinged() {
         lastPingTime.set(System.currentTimeMillis());
      }

      @Override
      public void run() {
         lastAccepted.set(System.currentTimeMillis());
         pingFrame = createPingFrame();

         synchronized (this) {
            while (!shutdown) {
               long dur1 = 0;
               long dur2 = 0;

               if (serverPing != 0) {
                  dur1 = System.currentTimeMillis() - lastPingTime.get();
                  if (dur1 >= serverPing) {
                     lastPingTime.set(System.currentTimeMillis());
                     connection.ping(pingFrame);
                     dur1 = 0;
                  }
               }

               if (serverAcceptPing != 0) {
                  dur2 = System.currentTimeMillis() - lastAccepted.get();

                  if (dur2 > (2 * serverAcceptPing)) {
                     connection.disconnect(false);
                     shutdown = true;
                     break;
                  }
               }

               long waitTime1 = 0;
               long waitTime2 = 0;

               if (serverPing > 0) {
                  waitTime1 = serverPing - dur1;
               }

               if (serverAcceptPing > 0) {
                  waitTime2 = serverAcceptPing * 2 - dur2;
               }

               long waitTime = 10L;

               if ((waitTime1 > 0) && (waitTime2 > 0)) {
                  waitTime = Math.min(waitTime1, waitTime2);
               }
               else if (waitTime1 > 0) {
                  waitTime = waitTime1;
               }
               else if (waitTime2 > 0) {
                  waitTime = waitTime2;
               }

               try {
                  this.wait(waitTime);
               }
               catch (InterruptedException e) {
               }
            }
         }
      }

      public void pingAccepted() {
         this.lastAccepted.set(System.currentTimeMillis());
      }
   }

   @Override
   public void requestAccepted(StompFrame request) {
      if (heartBeater != null) {
         heartBeater.pingAccepted();
      }
   }

   @Override
   public StompFrame createStompFrame(String command) {
      return new StompFrameV11(command);
   }

   @Override
   public void initDecoder(VersionedStompFrameHandler existingHandler) {
      decoder.init(existingHandler.getDecoder());
   }

   protected class StompDecoderV11 extends StompDecoder {

      protected boolean isEscaping = false;
      protected SimpleBytes holder = new SimpleBytes(1024);

      public StompDecoderV11(StompFrameHandlerV11 handler) {
         super(handler);
      }

      @Override
      public void init(StompDecoder decoder) {
         this.data = decoder.data;
         this.workingBuffer = decoder.workingBuffer;
         this.pos = decoder.pos;
         this.command = decoder.command;
      }

      @Override
      public void init() {
         super.init();
         isEscaping = false;
         holder.reset();
      }

      @Override
      protected boolean parseCommand() throws ActiveMQStompException {
         int offset = 0;
         boolean nextChar = false;

         //check for ping
         // Some badly behaved STOMP clients add a \n *after* the terminating NUL char at the end of the
         // STOMP frame this can manifest as an extra \n at the beginning when the
         // next STOMP frame is read - we need to deal with this.
         // Besides, Stomp 1.2 allows for extra EOLs after NULL (i.e.
         // either "[\r]\n"s or "\n"s)
         while (true) {
            if (workingBuffer[offset] == NEW_LINE) {
               if (heartBeater != null) {
                  //client ping
                  heartBeater.pingAccepted();
               }
               nextChar = false;
            }
            else if (workingBuffer[offset] == CR) {
               if (nextChar)
                  throw BUNDLE.invalidTwoCRs().setHandler(handler);
               nextChar = true;
            }
            else {
               break;
            }
            offset++;
            if (offset == data)
               return false; //no more bytes
         }

         if (nextChar) {
            throw BUNDLE.badCRs().setHandler(handler);
         }

         //if some EOLs have been processed, drop those bytes before parsing command
         if (offset > 0) {
            System.arraycopy(workingBuffer, offset, workingBuffer, 0, data - offset);
            data = data - offset;
            offset = 0;
         }

         if (data < 4) {
            // Need at least four bytes to identify the command
            // - up to 3 bytes for the command name + potentially another byte for a leading \n
            return false;
         }

         byte b = workingBuffer[offset];

         switch (b) {
            case A: {
               if (workingBuffer[offset + 1] == StompDecoder.B) {
                  if (!tryIncrement(offset + COMMAND_ABORT_LENGTH + eolLen)) {
                     return false;
                  }

                  // ABORT
                  command = COMMAND_ABORT;
               }
               else {
                  if (!tryIncrement(offset + COMMAND_ACK_LENGTH + eolLen)) {
                     return false;
                  }

                  // ACK
                  command = COMMAND_ACK;
               }
               break;
            }
            case B: {
               if (!tryIncrement(offset + COMMAND_BEGIN_LENGTH + eolLen)) {
                  return false;
               }

               // BEGIN
               command = COMMAND_BEGIN;

               break;
            }
            case C: {
               if (workingBuffer[offset + 2] == M) {
                  if (!tryIncrement(offset + COMMAND_COMMIT_LENGTH + eolLen)) {
                     return false;
                  }

                  // COMMIT
                  command = COMMAND_COMMIT;
               }
               /**** added by meddy, 27 april 2011, handle header parser for reply to websocket protocol ****/
               else if (workingBuffer[offset + 7] == E) {
                  if (!tryIncrement(offset + COMMAND_CONNECTED_LENGTH + eolLen)) {
                     return false;
                  }

                  // CONNECTED
                  command = COMMAND_CONNECTED;
               }
               /**** end ****/
               else {
                  if (!tryIncrement(offset + COMMAND_CONNECT_LENGTH + eolLen)) {
                     return false;
                  }

                  // CONNECT
                  command = COMMAND_CONNECT;
               }
               break;
            }
            case D: {
               if (!tryIncrement(offset + COMMAND_DISCONNECT_LENGTH + eolLen)) {
                  return false;
               }

               // DISCONNECT
               command = COMMAND_DISCONNECT;

               break;
            }
            case R: {
               if (!tryIncrement(offset + COMMAND_RECEIPT_LENGTH + eolLen)) {
                  return false;
               }

               // RECEIPT
               command = COMMAND_RECEIPT;

               break;
            }
            /**** added by meddy, 27 april 2011, handle header parser for reply to websocket protocol ****/
            case E: {
               if (!tryIncrement(offset + COMMAND_ERROR_LENGTH + eolLen)) {
                  return false;
               }

               // ERROR
               command = COMMAND_ERROR;

               break;
            }
            case M: {
               if (!tryIncrement(offset + COMMAND_MESSAGE_LENGTH + eolLen)) {
                  return false;
               }

               // MESSAGE
               command = COMMAND_MESSAGE;

               break;
            }
            /**** end ****/
            case S: {
               if (workingBuffer[offset + 1] == E) {
                  if (!tryIncrement(offset + COMMAND_SEND_LENGTH + eolLen)) {
                     return false;
                  }

                  // SEND
                  command = COMMAND_SEND;
               }
               else if (workingBuffer[offset + 1] == U) {
                  if (!tryIncrement(offset + COMMAND_SUBSCRIBE_LENGTH + eolLen)) {
                     return false;
                  }

                  // SUBSCRIBE
                  command = COMMAND_SUBSCRIBE;
               }
               else {
                  if (!tryIncrement(offset + StompDecoder.COMMAND_STOMP_LENGTH + eolLen)) {
                     return false;
                  }

                  // SUBSCRIBE
                  command = COMMAND_STOMP;
               }
               break;
            }
            case U: {
               if (!tryIncrement(offset + COMMAND_UNSUBSCRIBE_LENGTH + eolLen)) {
                  return false;
               }

               // UNSUBSCRIBE
               command = COMMAND_UNSUBSCRIBE;

               break;
            }
            case N: {
               if (!tryIncrement(offset + COMMAND_NACK_LENGTH + eolLen)) {
                  return false;
               }
               //NACK
               command = COMMAND_NACK;
               break;
            }
            default: {
               throwInvalid();
            }
         }

         checkEol();

         return true;
      }

      protected void checkEol() throws ActiveMQStompException {
         if (workingBuffer[pos - 1] != NEW_LINE) {
            throwInvalid();
         }
      }

      @Override
      protected boolean parseHeaders() throws ActiveMQStompException {

      outer:
         while (true) {
            byte b = workingBuffer[pos++];

            switch (b) {
               //escaping
               case ESC_CHAR: {
                  if (isEscaping) {
                     //this is a backslash
                     holder.append(b);
                     isEscaping = false;
                  }
                  else {
                     //begin escaping
                     isEscaping = true;
                  }
                  break;
               }
               case HEADER_SEPARATOR: {
                  if (inHeaderName) {
                     headerName = holder.getString();

                     holder.reset();

                     inHeaderName = false;

                     headerValueWhitespace = true;
                  }

                  whiteSpaceOnly = false;

                  break;
               }
               case StompDecoder.LN: {
                  if (isEscaping) {
                     holder.append(StompDecoder.NEW_LINE);
                     isEscaping = false;
                  }
                  else {
                     holder.append(b);
                  }
                  break;
               }
               case StompDecoder.c: {
                  if (isEscaping) {
                     holder.append(StompDecoder.HEADER_SEPARATOR);
                     isEscaping = false;
                  }
                  else {
                     holder.append(b);
                  }
                  break;
               }
               case StompDecoder.NEW_LINE: {
                  if (whiteSpaceOnly) {
                     // Headers are terminated by a blank line
                     readingHeaders = false;

                     break outer;
                  }

                  String headerValue = holder.getString();
                  holder.reset();

                  headers.put(headerName, headerValue);

                  if (headerName.equals(Stomp.Headers.CONTENT_LENGTH)) {
                     contentLength = Integer.parseInt(headerValue);
                  }

                  if (headerName.equals(Stomp.Headers.CONTENT_TYPE)) {
                     contentType = headerValue;
                  }

                  whiteSpaceOnly = true;

                  inHeaderName = true;

                  headerValueWhitespace = false;

                  break;
               }
               default: {
                  whiteSpaceOnly = false;

                  headerValueWhitespace = false;

                  holder.append(b);
               }
            }
            if (pos == data) {
               // Run out of data
               return false;
            }
         }
         return true;
      }

      @Override
      protected StompFrame parseBody() throws ActiveMQStompException {
         byte[] content = null;

         if (contentLength != -1) {
            if (pos + contentLength + 1 > data) {
               // Need more bytes
            }
            else {
               content = new byte[contentLength];

               System.arraycopy(workingBuffer, pos, content, 0, contentLength);

               pos += contentLength;

               //drain all the rest
               if (bodyStart == -1) {
                  bodyStart = pos;
               }

               while (pos < data) {
                  if (workingBuffer[pos++] == 0) {
                     break;
                  }
               }
            }
         }
         else {
            // Need to scan for terminating NUL

            if (bodyStart == -1) {
               bodyStart = pos;
            }

            while (pos < data) {
               if (workingBuffer[pos++] == 0) {
                  content = new byte[pos - bodyStart - 1];

                  System.arraycopy(workingBuffer, bodyStart, content, 0, content.length);

                  break;
               }
            }
         }

         if (content != null) {
            if (data > pos) {
               if (workingBuffer[pos] == NEW_LINE)
                  pos++;

               if (data > pos)
                  // More data still in the buffer from the next packet
                  System.arraycopy(workingBuffer, pos, workingBuffer, 0, data - pos);
            }

            data = data - pos;

            // reset

            StompFrame ret = new StompFrameV11(command, headers, content);

            init();

            return ret;
         }
         else {
            return null;
         }
      }
   }
}
