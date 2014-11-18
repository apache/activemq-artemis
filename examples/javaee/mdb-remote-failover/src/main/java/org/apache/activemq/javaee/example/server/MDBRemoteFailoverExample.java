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
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.jms.*;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 * @author Justin Bertram
 */
@MessageDriven(name = "MDBRemoteFailoverExample",
               activationConfig =
                     {
                        @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
                        @ActivationConfigProperty(propertyName = "destination", propertyValue = "inQueue"),
                        @ActivationConfigProperty(propertyName = "hA", propertyValue = "true"),
                        @ActivationConfigProperty(propertyName = "useJNDI", propertyValue = "false")
                     })
@ResourceAdapter("hornetq-remote-ra.rar")
public class MDBRemoteFailoverExample implements MessageListener
{

   @Resource(mappedName = "java:/RemoteJmsXA")
   ConnectionFactory connectionFactory;
   Queue replyQueue;

   public void onMessage(Message message)
   {
      Connection conn = null;
      try
      {
         replyQueue = HornetQJMSClient.createQueue("outQueue");
         //Step 9. We know the client is sending a text message so we cast
         TextMessage textMessage = (TextMessage)message;

         //Step 10. get the text from the message.
         String text = textMessage.getText();

         System.out.println("message " + text);

         //Step 11. we create a JMS connection
         conn = connectionFactory.createConnection();

         //Step 12. We create a JMS session
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 13. we create a producer for the reply queue
         MessageProducer producer = sess.createProducer(replyQueue);

         //Step 14. we create a message and send it
         producer.send(sess.createTextMessage("this is a reply"));

         System.out.println("reply sent");

      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if(conn != null)
         {
            try
            {
               conn.close();
            }
            catch (JMSException e)
            {
            }
         }
      }
   }
}
