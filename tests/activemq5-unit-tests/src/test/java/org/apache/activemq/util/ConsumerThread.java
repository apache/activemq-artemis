/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.util;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerThread extends Thread {

   private static final Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);

   int messageCount = 1000;
   int received = 0;
   Destination dest;
   Session sess;
   boolean breakOnNull = true;

   public ConsumerThread(Session sess, Destination dest) {
      this.dest = dest;
      this.sess = sess;
   }

   @Override
   public void run() {
      MessageConsumer consumer = null;

      try {
         consumer = sess.createConsumer(dest);
         while (received < messageCount) {
            Message msg = consumer.receive(3000);
            if (msg != null) {
               LOG.info("Received " + received + ": " + ((TextMessage) msg).getText());
               received++;
            } else {
               if (breakOnNull) {
                  break;
               }
            }
         }
      } catch (JMSException e) {
         e.printStackTrace();
      } finally {
         if (consumer != null) {
            try {
               consumer.close();
            } catch (JMSException e) {
               e.printStackTrace();
            }
         }
      }
   }

   public int getReceived() {
      return received;
   }

   public void setMessageCount(int messageCount) {
      this.messageCount = messageCount;
   }

   public void setBreakOnNull(boolean breakOnNull) {
      this.breakOnNull = breakOnNull;
   }
}
