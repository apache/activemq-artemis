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
package org.apache.activemq.artemis.example.wlp.sample;

import jakarta.annotation.Resource;
import jakarta.ejb.MessageDriven;
import jakarta.ejb.MessageDrivenContext;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;

@MessageDriven(name = "QueueMessageConsumer")
public class QueueMessageConsumer implements MessageListener {

   @Resource
   private MessageDrivenContext mdcContext;

   @Resource(name = "jms/sampleConnectionFactory")
   private ConnectionFactory connectionFactory;

   @Resource(name = "jms/sampleTopic")
   private Topic topic;

   @Override
   public void onMessage(Message message) {
      System.out.println("QueueMessageConsumer: " + message);
      Connection con = null;
      try {
         con = connectionFactory.createConnection();
         Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(topic);
         producer.send(message);
         con.close();
      } catch (Exception e) {
         e.printStackTrace();
         mdcContext.setRollbackOnly();
      } finally {
         if (con != null) {
            try {
               con.close();
            } catch (JMSException e) {
               e.printStackTrace();
            }
         }
      }
   }

}
