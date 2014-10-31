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
package org.hornetq.jms.transaction;

import java.util.Map;

import javax.transaction.xa.Xid;

import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionDetail;
import org.hornetq.jms.client.HornetQBytesMessage;
import org.hornetq.jms.client.HornetQMapMessage;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.jms.client.HornetQObjectMessage;
import org.hornetq.jms.client.HornetQStreamMessage;
import org.hornetq.jms.client.HornetQTextMessage;

/**
 * A JMSTransactionDetail
 *
 * @author <a href="tm.igarashi@gmail.com">Tomohisa Igarashi</a>
 *
 *
 */
public class JMSTransactionDetail extends TransactionDetail
{
   public JMSTransactionDetail(Xid xid, Transaction tx, Long creation) throws Exception
   {
      super(xid,tx,creation);
   }

   @Override
   public String decodeMessageType(ServerMessage msg)
   {
      int type = msg.getType();
      switch (type)
      {
         case HornetQMessage.TYPE: // 0
            return "Default";
         case HornetQObjectMessage.TYPE: // 2
            return "ObjectMessage";
         case HornetQTextMessage.TYPE: // 3
            return "TextMessage";
         case HornetQBytesMessage.TYPE: // 4
            return "ByteMessage";
         case HornetQMapMessage.TYPE: // 5
            return "MapMessage";
         case HornetQStreamMessage.TYPE: // 6
            return "StreamMessage";
         default:
            return "(Unknown Type)";
      }
   }

   @Override
   public Map<String, Object> decodeMessageProperties(ServerMessage msg)
   {
      try
      {
         return HornetQMessage.coreMaptoJMSMap(msg.toMap());
      }
      catch (Throwable t)
      {
         return null;
      }
   }
}
