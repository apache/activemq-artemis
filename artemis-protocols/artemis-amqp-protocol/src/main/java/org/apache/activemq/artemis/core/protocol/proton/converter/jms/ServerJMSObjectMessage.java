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
package org.apache.activemq.artemis.core.protocol.proton.converter.jms;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.message.impl.MessageInternal;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;


public class ServerJMSObjectMessage  extends ServerJMSMessage implements ObjectMessage {
   public static final byte TYPE = Message.STREAM_TYPE;

   private Serializable object;

   public ServerJMSObjectMessage(MessageInternal message, int deliveryCount) {
      super(message, deliveryCount);
   }

   @Override
   public void setObject(Serializable object) throws JMSException {
      this.object = object;
   }

   @Override
   public Serializable getObject() throws JMSException {
      return object;
   }

   @Override
   public void encode() throws Exception {
      super.encode();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream ous = new ObjectOutputStream(out);
      ous.writeObject(object);
      getInnerMessage().getBodyBuffer().writeBytes(out.toByteArray());
   }

   @Override
   public void decode() throws Exception {
      super.decode();
      int size = getInnerMessage().getBodyBuffer().readableBytes();
      byte[] bytes = new byte[size];
      getInnerMessage().getBodyBuffer().readBytes(bytes);
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
      object = (Serializable) ois.readObject();
   }
}
