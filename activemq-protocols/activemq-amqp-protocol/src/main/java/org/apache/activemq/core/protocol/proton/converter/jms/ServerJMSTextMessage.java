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
package org.apache.activemq6.core.protocol.proton.converter.jms;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.apache.activemq6.api.core.Message;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.core.message.impl.MessageInternal;

import static org.apache.activemq6.reader.TextMessageUtil.readBodyText;
import static org.apache.activemq6.reader.TextMessageUtil.writeBodyText;


/**
 * HornetQ implementation of a JMS TextMessage.
 * <br>
 * This class was ported from SpyTextMessage in JBossMQ.
 *
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version $Revision: 3412 $
 */
public class ServerJMSTextMessage extends ServerJMSMessage implements TextMessage
{
   // Constants -----------------------------------------------------

   public static final byte TYPE = Message.TEXT_TYPE;

   // Attributes ----------------------------------------------------

   // We cache it locally - it's more performant to cache as a SimpleString, the AbstractChannelBuffer write
   // methods are more efficient for a SimpleString
   private SimpleString text;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /*
    * This constructor is used to construct messages prior to sending
    */
   public ServerJMSTextMessage(MessageInternal message, int deliveryCount)
   {
      super(message, deliveryCount);

   }
   // TextMessage implementation ------------------------------------

   public void setText(final String text) throws JMSException
   {
      if (text != null)
      {
         this.text = new SimpleString(text);
      }
      else
      {
         this.text = null;
      }

      writeBodyText(message, this.text);
   }

   public String getText()
   {
      if (text != null)
      {
         return text.toString();
      }
      else
      {
         return null;
      }
   }

   @Override
   public void clearBody() throws JMSException
   {
      super.clearBody();

      text = null;
   }


   public void encode() throws Exception
   {
      super.encode();
      writeBodyText(message, text);
   }

   public void decode() throws Exception
   {
      super.decode();
      text = readBodyText(message);
   }

}