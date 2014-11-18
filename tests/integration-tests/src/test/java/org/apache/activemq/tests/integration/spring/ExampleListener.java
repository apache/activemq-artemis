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
package org.apache.activemq.tests.integration.spring;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.activemq.utils.ReusableLatch;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class ExampleListener implements MessageListener
{
   public static String lastMessage = null;

   public static ReusableLatch latch = new ReusableLatch();

   public void onMessage(Message message)
   {
      try
      {
         lastMessage = ((TextMessage)message).getText();
      }
      catch (JMSException e)
      {
         throw new RuntimeException(e);
      }
      System.out.println("MESSAGE RECEIVED: " + lastMessage);
      latch.countDown();
   }
}
