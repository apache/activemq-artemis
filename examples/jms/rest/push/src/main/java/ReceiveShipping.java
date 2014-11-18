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
import org.apache.activemq.jms.client.ActiveMQDestination;
import org.apache.activemq.rest.Jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class ReceiveShipping
{
   public static void main(String[] args) throws Exception
   {
      ConnectionFactory factory = JmsHelper.createConnectionFactory("activemq-client.xml");
      Destination destination = (ActiveMQDestination) ActiveMQDestination.fromAddress("jms.queue.shipping");

      Connection conn = factory.createConnection();
      try
      {
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(destination);
         consumer.setMessageListener(new MessageListener()
         {
            @Override
            public void onMessage(Message message)
            {
               System.out.println("Received Message: ");
               Order order = Jms.getEntity(message, Order.class);
               System.out.println(order);
            }
         }
         );
         conn.start();
         Thread.sleep(1000000);
      }
      finally
      {
         conn.close();
      }
   }
}