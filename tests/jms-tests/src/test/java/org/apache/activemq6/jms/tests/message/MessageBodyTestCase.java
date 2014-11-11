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
/**
 *
 */
package org.apache.activemq6.jms.tests.message;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq6.jms.tests.HornetQServerTestCase;
import org.junit.Before;

public abstract class MessageBodyTestCase extends HornetQServerTestCase
{
   protected Connection producerConnection, consumerConnection;
   protected Session queueProducerSession, queueConsumerSession;
   protected MessageProducer queueProducer;
   protected MessageConsumer queueConsumer;

   enum JmsMessageType
   {
      TEXT, MAP, OBJECT, BYTE, STREAM;
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      producerConnection = addConnection(getConnectionFactory().createConnection());
      consumerConnection = addConnection(getConnectionFactory().createConnection());

      queueProducerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queueConsumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      queueProducer = queueProducerSession.createProducer(queue1);
      queueConsumer = queueConsumerSession.createConsumer(queue1);

      consumerConnection.start();
   }
}
