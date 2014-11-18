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
package org.apache.activemq.jms.transaction;

import java.util.Map;

import javax.transaction.xa.Xid;

import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.core.transaction.Transaction;
import org.apache.activemq.core.transaction.TransactionDetail;
import org.apache.activemq.jms.client.ActiveMQBytesMessage;
import org.apache.activemq.jms.client.ActiveMQMapMessage;
import org.apache.activemq.jms.client.ActiveMQMessage;
import org.apache.activemq.jms.client.ActiveMQObjectMessage;
import org.apache.activemq.jms.client.ActiveMQStreamMessage;
import org.apache.activemq.jms.client.ActiveMQTextMessage;

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
         case ActiveMQMessage.TYPE: // 0
            return "Default";
         case ActiveMQObjectMessage.TYPE: // 2
            return "ObjectMessage";
         case ActiveMQTextMessage.TYPE: // 3
            return "TextMessage";
         case ActiveMQBytesMessage.TYPE: // 4
            return "ByteMessage";
         case ActiveMQMapMessage.TYPE: // 5
            return "MapMessage";
         case ActiveMQStreamMessage.TYPE: // 6
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
         return ActiveMQMessage.coreMaptoJMSMap(msg.toMap());
      }
      catch (Throwable t)
      {
         return null;
      }
   }
}
