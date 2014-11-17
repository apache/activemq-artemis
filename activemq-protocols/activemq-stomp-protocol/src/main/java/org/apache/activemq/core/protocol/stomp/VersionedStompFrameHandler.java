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
package org.apache.activemq.core.protocol.stomp;

import java.nio.charset.StandardCharsets;

import org.apache.activemq.api.core.HornetQBuffer;
import org.apache.activemq.api.core.Message;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.message.impl.MessageImpl;
import org.apache.activemq.core.protocol.stomp.Stomp.Headers;
import org.apache.activemq.core.protocol.stomp.v10.StompFrameHandlerV10;
import org.apache.activemq.core.protocol.stomp.v11.StompFrameHandlerV11;
import org.apache.activemq.core.protocol.stomp.v12.StompFrameHandlerV12;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.core.server.impl.ServerMessageImpl;
import org.apache.activemq.utils.DataConstants;

import static org.apache.activemq.core.protocol.stomp.HornetQStompProtocolMessageBundle.BUNDLE;

/**
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public abstract class VersionedStompFrameHandler
{
   protected StompConnection connection;
   protected StompDecoder decoder;

   public static VersionedStompFrameHandler getHandler(StompConnection connection, StompVersions version)
   {
      if (version == StompVersions.V1_0)
      {
         return new StompFrameHandlerV10(connection);
      }
      if (version == StompVersions.V1_1)
      {
         return new StompFrameHandlerV11(connection);
      }
      if (version == StompVersions.V1_2)
      {
         return new StompFrameHandlerV12(connection);
      }
      return null;
   }

   protected VersionedStompFrameHandler(StompConnection connection)
   {
      this.connection = connection;
   }

   public StompFrame decode(HornetQBuffer buffer) throws HornetQStompException
   {
      return decoder.decode(buffer);
   }

   public boolean hasBytes()
   {
      return decoder.hasBytes();
   }

   public StompDecoder getDecoder()
   {
      return decoder;
   }

   public StompFrame handleFrame(StompFrame request)
   {
      StompFrame response = null;

      if (Stomp.Commands.SEND.equals(request.getCommand()))
      {
         response = onSend(request);
      }
      else if (Stomp.Commands.ACK.equals(request.getCommand()))
      {
         response = onAck(request);
      }
      else if (Stomp.Commands.NACK.equals(request.getCommand()))
      {
         response = onNack(request);
      }
      else if (Stomp.Commands.BEGIN.equals(request.getCommand()))
      {
         response = onBegin(request);
      }
      else if (Stomp.Commands.COMMIT.equals(request.getCommand()))
      {
         response = onCommit(request);
      }
      else if (Stomp.Commands.ABORT.equals(request.getCommand()))
      {
         response = onAbort(request);
      }
      else if (Stomp.Commands.SUBSCRIBE.equals(request.getCommand()))
      {
         response = onSubscribe(request);
      }
      else if (Stomp.Commands.UNSUBSCRIBE.equals(request.getCommand()))
      {
         response = onUnsubscribe(request);
      }
      else if (Stomp.Commands.CONNECT.equals(request.getCommand()))
      {
         response = onConnect(request);
      }
      else if (Stomp.Commands.STOMP.equals(request.getCommand()))
      {
         response = onStomp(request);
      }
      else if (Stomp.Commands.DISCONNECT.equals(request.getCommand()))
      {
         response = onDisconnect(request);
      }
      else
      {
         response = onUnknown(request.getCommand());
      }

      if (response == null)
      {
         response = postprocess(request);
      }
      else
      {
         if (request.hasHeader(Stomp.Headers.RECEIPT_REQUESTED))
         {
            response.addHeader(Stomp.Headers.Response.RECEIPT_ID, request.getHeader(Stomp.Headers.RECEIPT_REQUESTED));
         }
      }

      return response;
   }

   public abstract StompFrame onConnect(StompFrame frame);

   public abstract StompFrame onDisconnect(StompFrame frame);

   public abstract StompFrame onAck(StompFrame request);

   public abstract StompFrame onUnsubscribe(StompFrame request);

   public abstract StompFrame onStomp(StompFrame request);

   public abstract StompFrame onNack(StompFrame request);

   public abstract StompFrame createStompFrame(String command);

   public StompFrame onUnknown(String command)
   {
      HornetQStompException error = BUNDLE.unknownCommand(command);
      StompFrame response = error.getFrame();
      return response;
   }

   public StompFrame handleReceipt(String receiptID)
   {
      StompFrame receipt = new StompFrame(Stomp.Responses.RECEIPT);
      receipt.addHeader(Stomp.Headers.Response.RECEIPT_ID, receiptID);

      return receipt;
   }

   public StompFrame onCommit(StompFrame request)
   {
      StompFrame response = null;

      String txID = request.getHeader(Stomp.Headers.TRANSACTION);
      if (txID == null)
      {
         HornetQStompException error = BUNDLE.needTxIDHeader();
         response = error.getFrame();
         return response;
      }

      try
      {
         connection.commitTransaction(txID);
      }
      catch (HornetQStompException e)
      {
         response = e.getFrame();
      }
      return response;
   }

   public StompFrame onSend(StompFrame frame)
   {
      StompFrame response = null;
      try
      {
         connection.validate();
         String destination = frame.getHeader(Stomp.Headers.Send.DESTINATION);
         checkDestination(destination);
         String txID = frame.getHeader(Stomp.Headers.TRANSACTION);

         long timestamp = System.currentTimeMillis();

         ServerMessageImpl message = connection.createServerMessage();
         message.setTimestamp(timestamp);
         message.setAddress(SimpleString.toSimpleString(destination));
         StompUtils.copyStandardHeadersFromFrameToMessage(frame, message);
         if (frame.hasHeader(Stomp.Headers.CONTENT_LENGTH))
         {
            message.setType(Message.BYTES_TYPE);
            message.getBodyBuffer().writeBytes(frame.getBodyAsBytes());
         }
         else
         {
            message.setType(Message.TEXT_TYPE);
            String text = frame.getBody();
            message.getBodyBuffer().writeNullableSimpleString(SimpleString.toSimpleString(text));
         }

         connection.sendServerMessage(message, txID);
      }
      catch (HornetQStompException e)
      {
         response = e.getFrame();
      }
      catch (Exception e)
      {
         HornetQStompException error = BUNDLE.errorHandleSend(e);
         response = error.getFrame();
      }

      return response;
   }

   private void checkDestination(String destination) throws HornetQStompException
   {
      connection.checkDestination(destination);
   }

   public StompFrame onBegin(StompFrame frame)
   {
      StompFrame response = null;
      String txID = frame.getHeader(Stomp.Headers.TRANSACTION);
      if (txID == null)
      {
         HornetQStompException error = BUNDLE.beginTxNoID();
         response = error.getFrame();
      }
      else
      {
         try
         {
            connection.beginTransaction(txID);
         }
         catch (HornetQStompException e)
         {
            response = e.getFrame();
         }
      }
      return response;
   }

   public StompFrame onAbort(StompFrame request)
   {
      StompFrame response = null;
      String txID = request.getHeader(Stomp.Headers.TRANSACTION);

      if (txID == null)
      {
         HornetQStompException error = BUNDLE.abortTxNoID();
         response = error.getFrame();
         return response;
      }

      try
      {
         connection.abortTransaction(txID);
      }
      catch (HornetQStompException e)
      {
         response = e.getFrame();
      }

      return response;
   }

   public StompFrame onSubscribe(StompFrame request)
   {
      StompFrame response = null;
      String destination = request.getHeader(Stomp.Headers.Subscribe.DESTINATION);

      String selector = request.getHeader(Stomp.Headers.Subscribe.SELECTOR);
      String ack = request.getHeader(Stomp.Headers.Subscribe.ACK_MODE);
      String id = request.getHeader(Stomp.Headers.Subscribe.ID);
      String durableSubscriptionName = request.getHeader(Stomp.Headers.Subscribe.DURABLE_SUBSCRIBER_NAME);
      boolean noLocal = false;

      if (request.hasHeader(Stomp.Headers.Subscribe.NO_LOCAL))
      {
         noLocal = Boolean.parseBoolean(request.getHeader(Stomp.Headers.Subscribe.NO_LOCAL));
      }

      try
      {
         connection.subscribe(destination, selector, ack, id, durableSubscriptionName, noLocal);
      }
      catch (HornetQStompException e)
      {
         response = e.getFrame();
      }

      return response;
   }

   public StompFrame postprocess(StompFrame request)
   {
      StompFrame response = null;
      if (request.hasHeader(Stomp.Headers.RECEIPT_REQUESTED))
      {
         response = handleReceipt(request.getHeader(Stomp.Headers.RECEIPT_REQUESTED));
         if (request.getCommand().equals(Stomp.Commands.DISCONNECT))
         {
            response.setNeedsDisconnect(true);
         }
      }
      else
      {
         //request null, disconnect if so.
         if (request.getCommand().equals(Stomp.Commands.DISCONNECT))
         {
            this.connection.disconnect(false);
         }
      }
      return response;
   }

   public StompFrame createMessageFrame(ServerMessage serverMessage,
                                        StompSubscription subscription, int deliveryCount) throws Exception
   {
      StompFrame frame = createStompFrame(Stomp.Responses.MESSAGE);

      if (subscription.getID() != null)
      {
         frame.addHeader(Stomp.Headers.Message.SUBSCRIPTION,
                         subscription.getID());
      }

      HornetQBuffer buffer = serverMessage.getBodyBufferCopy();

      int bodyPos = serverMessage.getEndOfBodyPosition() == -1 ? buffer
         .writerIndex() : serverMessage.getEndOfBodyPosition();

      buffer.readerIndex(MessageImpl.BUFFER_HEADER_SPACE
                            + DataConstants.SIZE_INT);

      int size = bodyPos - buffer.readerIndex();

      byte[] data = new byte[size];

      if (serverMessage.containsProperty(Stomp.Headers.CONTENT_LENGTH)
         || serverMessage.getType() == Message.BYTES_TYPE)
      {
         frame.addHeader(Headers.CONTENT_LENGTH, String.valueOf(data.length));
         buffer.readBytes(data);
      }
      else
      {
         SimpleString text = buffer.readNullableSimpleString();
         if (text != null)
         {
            data = text.toString().getBytes(StandardCharsets.UTF_8);
         }
         else
         {
            data = new byte[0];
         }
      }
      frame.setByteBody(data);

      StompUtils.copyStandardHeadersFromMessageToFrame(serverMessage, frame,
                                                       deliveryCount);

      return frame;
   }

   /**
    * this method is called when a newer version of handler is created. It should
    * take over the state of the decoder of the existingHandler so that
    * the decoding can be continued. For V10 handler it's never get called.
    *
    * @param existingHandler
    */
   public void initDecoder(VersionedStompFrameHandler existingHandler)
   {
      throw BUNDLE.invalidCall();
   }

   //sends an ERROR frame back to client if possible then close the connection
   public void onError(HornetQStompException e)
   {
      this.connection.sendFrame(e.getFrame());
      connection.destroy();
   }

}
