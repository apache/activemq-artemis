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
package org.apache.activemq.javaee.example.server;

import org.apache.activemq.api.jms.HornetQJMSClient;
import org.jboss.ejb3.annotation.ResourceAdapter;

import javax.annotation.Resource;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.jms.*;

/**
 * MDB that is connected to the remote queue.
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author Justin Bertram
 */
@MessageDriven(name = "MDB_Queue",
               activationConfig =
                  {
                     @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
                     @ActivationConfigProperty(propertyName = "destination", propertyValue = "mdbQueue"),
                     @ActivationConfigProperty(propertyName = "useJNDI", propertyValue = "false")
                  })
@ResourceAdapter("hornetq-ra-remote.rar")
public class MDBQueue implements MessageListener
{
   @Resource(mappedName="java:/RemoteJmsXA")
   private ConnectionFactory connectionFactory;

   public void onMessage(Message message)
   {
      try
      {
         // Step 8. Receive the text message
         TextMessage tm = (TextMessage)message;

         String text = tm.getText();

         // Step 9. look up the reply queue
         Queue destQueue = HornetQJMSClient.createQueue("mdbReplyQueue");

         // Step 10. Create a connection
         Connection connection = connectionFactory.createConnection();

         // Step 11. Create a session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 12. Create a message producer to send the message
         MessageProducer producer = session.createProducer(destQueue);

         // Step 13. Create and send a reply text message
         producer.send(session.createTextMessage("A reply message"));

         // Step 14. Return the connection back to the pool
         connection.close();

      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}
