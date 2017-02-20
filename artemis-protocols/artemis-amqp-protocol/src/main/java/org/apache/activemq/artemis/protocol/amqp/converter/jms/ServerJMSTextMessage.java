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
package org.apache.activemq.artemis.protocol.amqp.converter.jms;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;

import static org.apache.activemq.artemis.reader.TextMessageUtil.readBodyText;
import static org.apache.activemq.artemis.reader.TextMessageUtil.writeBodyText;

/**
 * ActiveMQ Artemis implementation of a JMS TextMessage.
 * <br>
 * This class was ported from SpyTextMessage in JBossMQ.
 */
public class ServerJMSTextMessage extends ServerJMSMessage implements TextMessage {
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
   public ServerJMSTextMessage(ICoreMessage message) {
      super(message);

   }
   // TextMessage implementation ------------------------------------

   @Override
   public void setText(final String text) throws JMSException {
      if (text != null) {
         this.text = new SimpleString(text);
      } else {
         this.text = null;
      }

      writeBodyText(getWriteBodyBuffer(), this.text);
   }

   @Override
   public String getText() {
      if (text != null) {
         return text.toString();
      } else {
         return null;
      }
   }

   @Override
   public void clearBody() throws JMSException {
      super.clearBody();

      text = null;
   }

   @Override
   public void encode() throws Exception {
      super.encode();
      writeBodyText(getWriteBodyBuffer(), text);
   }

   @Override
   public void decode() throws Exception {
      super.decode();
      text = readBodyText(getReadBodyBuffer());
   }

}
