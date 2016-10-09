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
package org.apache.activemq.artemis.protocol.amqp.converter.message;

import java.io.UnsupportedEncodingException;

import javax.jms.JMSException;

import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.qpid.proton.codec.WritableBuffer;

public abstract class OutboundTransformer {

   protected IDGenerator idGenerator;

   public OutboundTransformer(IDGenerator idGenerator) {
      this.idGenerator = idGenerator;
   }

   /**
    * Given an JMS Message perform a conversion to an AMQP Message and encode into a form that
    * is ready for transmission.
    *
    * @param message
    *        the message to transform
    * @param buffer
    *        the buffer where encoding should write to
    *
    * @return the message format key of the encoded message.
    *
    * @throws JMSException
    *         if an error occurs during message transformation
    * @throws UnsupportedEncodingException
    *         if an error occurs during message encoding
    */
   public abstract long transform(ServerJMSMessage message, WritableBuffer buffer) throws JMSException, UnsupportedEncodingException;

}
