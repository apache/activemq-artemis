/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.util;

import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;

/** This can go away if Proton provides this feature. */
public class TLSEncode {

   // For now Proton requires that we create a decoder to create an encoder
   private static class EncoderDecoderPair {
      DecoderImpl decoder = new DecoderImpl();
      EncoderImpl encoder = new EncoderImpl(decoder);
      {
         AMQPDefinedTypes.registerAllTypes(decoder, encoder);
      }
   }

   private static final ThreadLocal<EncoderDecoderPair> tlsCodec = ThreadLocal.withInitial(() -> new EncoderDecoderPair());

   public static EncoderImpl getEncoder() {
      return tlsCodec.get().encoder;
   }

   public static DecoderImpl getDecoder() {
      return tlsCodec.get().decoder;
   }


}
