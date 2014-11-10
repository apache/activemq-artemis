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

package org.proton.plug.util;

import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;

/**
 * @author Clebert Suconic
 */

public class CodecCache
{

   private static class EncoderDecoderPair
   {
      DecoderImpl decoder = new DecoderImpl();
      EncoderImpl encoder = new EncoderImpl(decoder);

      {
         AMQPDefinedTypes.registerAllTypes(decoder, encoder);
      }
   }

   private static final ThreadLocal<EncoderDecoderPair> tlsCodec = new ThreadLocal<EncoderDecoderPair>()
   {
      @Override
      protected EncoderDecoderPair initialValue()
      {
         return new EncoderDecoderPair();
      }
   };

   public static DecoderImpl getDecoder()
   {
      return tlsCodec.get().decoder;
   }

   public static EncoderImpl getEncoder()
   {
      return tlsCodec.get().encoder;
   }

}
