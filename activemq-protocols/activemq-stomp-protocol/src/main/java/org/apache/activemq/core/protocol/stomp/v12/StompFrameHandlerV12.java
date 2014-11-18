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
package org.apache.activemq.core.protocol.stomp.v12;

import org.apache.activemq.core.protocol.stomp.FrameEventListener;
import org.apache.activemq.core.protocol.stomp.HornetQStompException;
import org.apache.activemq.core.protocol.stomp.Stomp;
import org.apache.activemq.core.protocol.stomp.StompConnection;
import org.apache.activemq.core.protocol.stomp.StompDecoder;
import org.apache.activemq.core.protocol.stomp.StompFrame;
import org.apache.activemq.core.protocol.stomp.StompSubscription;
import org.apache.activemq.core.protocol.stomp.v11.StompFrameHandlerV11;
import org.apache.activemq.core.protocol.stomp.v11.StompFrameV11;
import org.apache.activemq.core.server.HornetQServerLogger;
import org.apache.activemq.core.server.ServerMessage;

import static org.apache.activemq.core.protocol.stomp.HornetQStompProtocolMessageBundle.BUNDLE;

/**
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class StompFrameHandlerV12 extends StompFrameHandlerV11 implements FrameEventListener
{
   public StompFrameHandlerV12(StompConnection connection)
   {
      super(connection);
      decoder = new StompDecoderV12();
      decoder.init();
   }

   @Override
   public StompFrame createStompFrame(String command)
   {
      return new StompFrameV12(command);
   }

   @Override
   public StompFrame createMessageFrame(ServerMessage serverMessage,
                                        StompSubscription subscription, int deliveryCount) throws Exception
   {
      StompFrame frame = super.createMessageFrame(serverMessage, subscription, deliveryCount);

      if (!subscription.getAck().equals(Stomp.Headers.Subscribe.AckModeValues.AUTO))
      {
         frame.addHeader(Stomp.Headers.Message.ACK, String.valueOf(serverMessage.getMessageID()));
      }

      return frame;
   }

   /**
    * Version 1.2's ACK frame only requires 'id' header
    * here we use id = messageID
    */
   @Override
   public StompFrame onAck(StompFrame request)
   {
      StompFrame response = null;

      String messageID = request.getHeader(Stomp.Headers.Ack.ID);
      String txID = request.getHeader(Stomp.Headers.TRANSACTION);

      if (txID != null)
      {
         HornetQServerLogger.LOGGER.stompTXAckNorSupported();
      }

      if (messageID == null)
      {
         HornetQStompException error = BUNDLE.noIDInAck();
         error.setHandler(connection.getFrameHandler());
         return error.getFrame();
      }

      try
      {
         connection.acknowledge(messageID, null);
      }
      catch (HornetQStompException e)
      {
         response = e.getFrame();
      }

      return response;
   }

   protected class StompDecoderV12 extends StompDecoderV11
   {
      protected boolean nextEOLChar = false;

      public StompDecoderV12()
      {
         //1.2 allow '\r\n'
         eolLen = 2;
      }

      @Override
      public void init()
      {
         super.init();
         nextEOLChar = false;
      }

      @Override
      protected void checkEol() throws HornetQStompException
      {
         //either \n or \r\n
         if (workingBuffer[pos - 2] == NEW_LINE)
         {
            pos--;
         }
         else if (workingBuffer[pos - 2] != CR)
         {
            throwInvalid();
         }
         else if (workingBuffer[pos - 1] != NEW_LINE)
         {
            throwInvalid();
         }
      }

      @Override
      public void init(StompDecoder decoder)
      {
         this.data = decoder.data;
         this.workingBuffer = decoder.workingBuffer;
         this.pos = decoder.pos;
         this.command = decoder.command;
      }

      @Override
      protected boolean parseHeaders() throws HornetQStompException
      {
      outer:
         while (true)
         {
            byte b = workingBuffer[pos++];

            switch (b)
            {
               //escaping
               case ESC_CHAR:
               {
                  if (isEscaping)
                  {
                     //this is a backslash
                     holder.append(b);
                     isEscaping = false;
                  }
                  else
                  {
                     //begin escaping
                     isEscaping = true;
                  }
                  break;
               }
               case HEADER_SEPARATOR:
               {
                  if (inHeaderName)
                  {
                     headerName = holder.getString();

                     holder.reset();

                     inHeaderName = false;

                     headerValueWhitespace = true;
                  }

                  whiteSpaceOnly = false;

                  break;
               }
               case LN:
               {
                  if (isEscaping)
                  {
                     holder.append(NEW_LINE);
                     isEscaping = false;
                  }
                  else
                  {
                     holder.append(b);
                  }
                  break;
               }
               case RT:
               {
                  if (isEscaping)
                  {
                     holder.append(CR);
                     isEscaping = false;
                  }
                  else
                  {
                     holder.append(b);
                  }
                  break;
               }
               case CR:
               {
                  if (nextEOLChar)
                  {
                     throw BUNDLE.invalidTwoCRs();
                  }
                  nextEOLChar = true;
                  break;
               }
               case StompDecoder.c:
               {
                  if (isEscaping)
                  {
                     holder.append(StompDecoder.HEADER_SEPARATOR);
                     isEscaping = false;
                  }
                  else
                  {
                     holder.append(b);
                  }
                  break;
               }
               case NEW_LINE:
               {
                  nextEOLChar = false;
                  if (whiteSpaceOnly)
                  {
                     // Headers are terminated by a blank line
                     readingHeaders = false;
                     break outer;
                  }

                  String headerValue = holder.getString();
                  holder.reset();

                  if (!headers.containsKey(headerName))
                  {
                     headers.put(headerName, headerValue);
                  }

                  if (headerName.equals(CONTENT_LENGTH_HEADER_NAME))
                  {
                     contentLength = Integer.parseInt(headerValue);
                  }

                  if (headerName.equals(CONTENT_TYPE_HEADER_NAME))
                  {
                     contentType = headerValue;
                  }

                  whiteSpaceOnly = true;

                  inHeaderName = true;

                  headerValueWhitespace = false;

                  break;
               }
               default:
               {
                  whiteSpaceOnly = false;

                  headerValueWhitespace = false;

                  holder.append(b);
               }
            }
            if (pos == data)
            {
               // Run out of data
               return false;
            }
         }
         return true;
      }

      protected StompFrame parseBody() throws HornetQStompException
      {
         byte[] content = null;

         if (contentLength != -1)
         {
            if (pos + contentLength + 1 > data)
            {
               // Need more bytes
            }
            else
            {
               content = new byte[contentLength];

               System.arraycopy(workingBuffer, pos, content, 0, contentLength);

               pos += contentLength;

               //drain all the rest
               if (bodyStart == -1)
               {
                  bodyStart = pos;
               }

               while (pos < data)
               {
                  if (workingBuffer[pos++] == 0)
                  {
                     break;
                  }
               }
            }
         }
         else
         {
            // Need to scan for terminating NUL

            if (bodyStart == -1)
            {
               bodyStart = pos;
            }

            while (pos < data)
            {
               if (workingBuffer[pos++] == 0)
               {
                  content = new byte[pos - bodyStart - 1];

                  System.arraycopy(workingBuffer, bodyStart, content, 0, content.length);

                  break;
               }
            }
         }

         if (content != null)
         {
            if (data > pos)
            {
               if (workingBuffer[pos] == NEW_LINE) pos++;

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
         else
         {
            return null;
         }
      }
   }
}
