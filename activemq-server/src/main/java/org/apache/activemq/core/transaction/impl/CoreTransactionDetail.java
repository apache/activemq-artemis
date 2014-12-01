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
package org.apache.activemq.core.transaction.impl;

import java.util.Map;

import javax.transaction.xa.Xid;

import org.apache.activemq.api.core.Message;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.core.transaction.Transaction;
import org.apache.activemq.core.transaction.TransactionDetail;

/**
 * A CoreTransactionDetail
 *
 * @author <a href="tm.igarashi@gmail.com">Tomohisa Igarashi</a>
 *
 *
 */
public class CoreTransactionDetail extends TransactionDetail
{
   public CoreTransactionDetail(Xid xid, Transaction tx, Long creation) throws Exception
   {
      super(xid,tx,creation);
   }

   @Override
   public String decodeMessageType(ServerMessage msg)
   {
      int type = msg.getType();
      switch (type)
      {
         case Message.DEFAULT_TYPE: // 0
            return "Default";
         case Message.OBJECT_TYPE: // 2
            return "ObjectMessage";
         case Message.TEXT_TYPE: // 3
            return "TextMessage";
         case Message.BYTES_TYPE: // 4
            return "ByteMessage";
         case Message.MAP_TYPE: // 5
            return "MapMessage";
         case Message.STREAM_TYPE: // 6
            return "StreamMessage";
         default:
            return "(Unknown Type)";
      }
   }

   @Override
   public Map<String, Object> decodeMessageProperties(ServerMessage msg)
   {
      return msg.toMap();
   }
}
