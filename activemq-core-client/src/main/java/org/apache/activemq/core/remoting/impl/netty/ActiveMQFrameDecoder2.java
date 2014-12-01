/**
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
package org.apache.activemq.core.remoting.impl.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.activemq.utils.DataConstants;

/**
 * A Netty decoder specially optimised to to decode messages on the core protocol only
 *
 * @author <a href="tlee@redhat.com">Trustin Lee</a>
 * @author <a href="nmaurer@redhat.com">Norman Maurer</a>
 * @version $Revision: 7839 $, $Date: 2009-08-21 02:26:39 +0900 (2009-08-21, 금) $
 */
public class ActiveMQFrameDecoder2 extends LengthFieldBasedFrameDecoder
{
   public ActiveMQFrameDecoder2()
   {
      super(Integer.MAX_VALUE, 0, DataConstants.SIZE_INT);
   }

   @Override
   protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length)
   {
      return super.extractFrame(ctx, buffer, index, length).skipBytes(DataConstants.SIZE_INT);
   }
}
