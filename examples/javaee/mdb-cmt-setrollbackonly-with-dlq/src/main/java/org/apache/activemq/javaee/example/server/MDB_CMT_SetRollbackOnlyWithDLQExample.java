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

import javax.annotation.Resource;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.ejb.MessageDrivenContext;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.jboss.ejb3.annotation.ResourceAdapter;

/**
 * @author Justin Bertram
 */
@MessageDriven(name = "MDB_CMT_SetRollbackOnlyWithDLQExample",
               activationConfig =
                  {
                     @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Topic"),
                     @ActivationConfigProperty(propertyName = "destination", propertyValue = "topic/testTopic"),
                     @ActivationConfigProperty(propertyName = "subscriptionDurability", propertyValue = "Durable"),  
                     @ActivationConfigProperty(propertyName = "clientID", propertyValue = "myClientID"),
                     @ActivationConfigProperty(propertyName = "subscriptionName",propertyValue = "mySubscriptionName")
                  })
public class MDB_CMT_SetRollbackOnlyWithDLQExample implements MessageListener
{
   @Resource
   MessageDrivenContext ctx;

   public void onMessage(final Message message)
   {
      try
      {
         // Step 9. We know the client is sending a text message so we cast
         TextMessage textMessage = (TextMessage)message;

         // Step 10. get the text from the message.
         String text = textMessage.getText();
         System.out.println("message " + text + " received");

         // Step 11. rollback delivery of message to send to DLQ
         ctx.setRollbackOnly();
      }
      catch (JMSException e)
      {
         e.printStackTrace();
      }
   }
}
