/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.converter.jms;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.qpid.proton.amqp.Binary;

public class ServerJMSObjectMessage extends ServerJMSMessage implements ObjectMessage {

   public static final byte TYPE = Message.OBJECT_TYPE;

   private Binary payload;

   public ServerJMSObjectMessage(ICoreMessage message) {
      super(message);
   }

   @Override
   public void setObject(Serializable object) throws JMSException {
      throw new UnsupportedOperationException("Cannot set Object on this internal message");
   }

   @Override
   public Serializable getObject() throws JMSException {
      throw new UnsupportedOperationException("Cannot set Object on this internal message");
   }

   public void setSerializedForm(Binary payload) {
      this.payload = payload;
   }

   public Binary getSerializedForm() {
      return payload;
   }

   @Override
   public void encode() throws Exception {
      super.encode();
      getInnerMessage().getBodyBuffer().writeInt(payload.getLength());
      getInnerMessage().getBodyBuffer().writeBytes(payload.getArray(), payload.getArrayOffset(), payload.getLength());
   }

   @Override
   public void decode() throws Exception {
      super.decode();
      ActiveMQBuffer buffer = getInnerMessage().getDataBuffer();
      int size = buffer.readInt();
      byte[] bytes = new byte[size];
      buffer.readBytes(bytes);
      payload = new Binary(bytes);
   }
}
