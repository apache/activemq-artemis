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
package org.apache.activemq6.core.protocol.core.impl.wireformat;

import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq6.core.message.impl.MessageInternal;

/**
 * A SessionSendContinuationMessage<br>
 * Created Dec 4, 2008 12:25:14 PM
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class SessionSendContinuationMessage extends SessionContinuationMessage
{
   private boolean requiresResponse;

   // Used on confirmation handling
   private MessageInternal message;
   /**
    * In case, we are using a different handler than the one set on the {@link org.apache.activemq6.api.core.client.ClientSession}
    * <p/>
    * This field is only used at the client side.
    *
    * @see org.apache.activemq6.api.core.client.ClientSession#setSendAcknowledgementHandler(SendAcknowledgementHandler)
    * @see org.apache.activemq6.api.core.client.ClientProducer#send(org.hornetq.api.core.SimpleString, org.hornetq.api.core.Message, SendAcknowledgementHandler)
    */
   private final transient SendAcknowledgementHandler handler;

   /**
    * to be sent on the last package
    */
   private long messageBodySize = -1;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendContinuationMessage()
   {
      super(SESS_SEND_CONTINUATION);
      handler = null;
   }

   /**
    * @param body
    * @param continues
    * @param requiresResponse
    */
   public SessionSendContinuationMessage(final MessageInternal message, final byte[] body, final boolean continues,
                                         final boolean requiresResponse, final long messageBodySize,
                                         SendAcknowledgementHandler handler)
   {
      super(SESS_SEND_CONTINUATION, body, continues);
      this.requiresResponse = requiresResponse;
      this.message = message;
      this.handler = handler;
      this.messageBodySize = messageBodySize;
   }

   // Public --------------------------------------------------------

   /**
    * @return the requiresResponse
    */
   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }

   public long getMessageBodySize()
   {
      return messageBodySize;
   }


   /**
    * @return the message
    */
   public MessageInternal getMessage()
   {
      return message;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      super.encodeRest(buffer);
      if (!continues)
      {
         buffer.writeLong(messageBodySize);
      }
      buffer.writeBoolean(requiresResponse);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      super.decodeRest(buffer);
      if (!continues)
      {
         messageBodySize = buffer.readLong();
      }
      requiresResponse = buffer.readBoolean();
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((message == null) ? 0 : message.hashCode());
      result = prime * result + (int) (messageBodySize ^ (messageBodySize >>> 32));
      result = prime * result + (requiresResponse ? 1231 : 1237);
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionSendContinuationMessage))
         return false;
      SessionSendContinuationMessage other = (SessionSendContinuationMessage) obj;
      if (message == null)
      {
         if (other.message != null)
            return false;
      }
      else if (!message.equals(other.message))
         return false;
      if (messageBodySize != other.messageBodySize)
         return false;
      if (requiresResponse != other.requiresResponse)
         return false;
      return true;
   }

   public SendAcknowledgementHandler getHandler()
   {
      return handler;
   }
}
