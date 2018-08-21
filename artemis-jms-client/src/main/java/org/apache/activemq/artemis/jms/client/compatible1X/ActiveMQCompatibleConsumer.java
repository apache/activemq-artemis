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

package org.apache.activemq.artemis.jms.client.compatible1X;

import javax.jms.JMSException;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQMessageConsumer;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;

public class ActiveMQCompatibleConsumer extends ActiveMQMessageConsumer {

   public ActiveMQCompatibleConsumer(ConnectionFactoryOptions options,
                                     ActiveMQConnection connection,
                                     ActiveMQSession session,
                                     ClientConsumer consumer,
                                     boolean noLocal,
                                     ActiveMQDestination destination,
                                     String selector,
                                     SimpleString autoDeleteQueueName) throws JMSException {
      super(options, connection, session, consumer, noLocal, destination, selector, autoDeleteQueueName);
   }

   protected ActiveMQMessage getActiveMQMessage(ClientMessage coreMessage,
                                                ClientSession coreSession,
                                                boolean needSession) {
      return ActiveMQCompatibleMessage.createMessage(coreMessage, needSession ? coreSession : null, options);
   }

}
