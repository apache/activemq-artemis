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
package org.apache.activemq.artemis.protocol.amqp.broker;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;

import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.TypeConstructor;

final class AMQPMessageSymbolSearch {

   // used to quick search for MessageAnnotations
   private static final IdentityHashMap<Class<?>, Boolean> MSG_BODY_TYPES;

   static {
      // we're including MessageAnnotations here because it will still cause termination
      final List<Class<?>> classList = Arrays.asList(MessageAnnotations.class, Properties.class,
                                                     ApplicationProperties.class, Data.class,
                                                     AmqpSequence.class, AmqpValue.class, Footer.class);
      MSG_BODY_TYPES = new IdentityHashMap<>(classList.size());
      classList.forEach(clazz -> MSG_BODY_TYPES.put(clazz, Boolean.TRUE));
   }

   public static KMPNeedle kmpNeedleOf(Symbol symbol) {
      return KMPNeedle.of(symbol.toString().getBytes(StandardCharsets.US_ASCII));
   }

   public static boolean anyMessageAnnotations(ReadableBuffer data, KMPNeedle[] needles) {
      DecoderImpl decoder = TLSEncode.getDecoder();
      final int position = data.position();
      decoder.setBuffer(data.rewind());
      try {
         while (data.hasRemaining()) {
            TypeConstructor<?> constructor = decoder.readConstructor();
            final Class<?> typeClass = constructor.getTypeClass();
            if (MSG_BODY_TYPES.containsKey(typeClass)) {
               if (MessageAnnotations.class.equals(typeClass)) {
                  final int start = data.position();
                  constructor.skipValue();
                  final int end = data.position();
                  for (int i = 0, count = needles.length; i < count; i++) {
                     final int foundIndex = needles[i].searchInto(data, start, end);
                     if (foundIndex != -1) {
                        return true;
                     }
                  }
               }
               return false;
            }
            constructor.skipValue();
         }
         return false;
      } finally {
         decoder.setBuffer(null);
         data.position(position);
      }
   }

}
