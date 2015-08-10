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
package org.proton.plug.util;

import io.netty.buffer.ByteBuf;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;

public class DeliveryUtil {

   public static int readDelivery(Receiver receiver, ByteBuf buffer) {
      int initial = buffer.writerIndex();
      // optimization by norman
      int count;
      while ((count = receiver.recv(buffer.array(), buffer.arrayOffset() + buffer.writerIndex(), buffer.writableBytes())) > 0) {
         // Increment the writer index by the number of bytes written into it while calling recv.
         buffer.writerIndex(buffer.writerIndex() + count);
      }
      return buffer.writerIndex() - initial;
   }

   public static MessageImpl decodeMessageImpl(ByteBuf buffer) {
      MessageImpl message = (MessageImpl) Message.Factory.create();
      message.decode(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), buffer.readableBytes());
      return message;
   }

}
