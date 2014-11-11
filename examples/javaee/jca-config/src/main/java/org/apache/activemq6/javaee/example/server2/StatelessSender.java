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
package org.apache.activemq6.javaee.example.server2;

import org.apache.activemq6.api.jms.HornetQJMSClient;
import org.apache.activemq6.ra.HornetQRAConnectionFactoryImpl;

import javax.annotation.Resource;
import javax.ejb.Remote;
import javax.ejb.Stateless;
import javax.jms.*;

/**
 * A Stateless Bean that will connect to a remote JBM.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
@Remote(StatelessSenderService.class)
@Stateless
public class StatelessSender implements StatelessSenderService
{
   @Resource(mappedName="java:/RemoteJmsXA")
   private ConnectionFactory connectionFactory;

   public void sendHello(String message) throws Exception
   {
      // Step 4. Define the destinations that will receive the message (instead of using JNDI to the remote server)
      Queue destQueueA = HornetQJMSClient.createQueue("A");
      Queue destQueueB = HornetQJMSClient.createQueue("B");
      // Step 5. Create a connection to a remote server using a connection-factory (look at the deployed file jms-remote-ds.xml)
      Connection conn = connectionFactory.createConnection("guest", "password");

      // Step 6. Send a message to a QueueA on the remote server, which will be received by MDBQueueA
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prodA = sess.createProducer(destQueueA);
      prodA.send(sess.createTextMessage(message));

      // Step 7. Send a message to a QueueB on the remote server, which will be received by MDBQueueA
      MessageProducer prodB = sess.createProducer(destQueueB);
      prodB.send(sess.createTextMessage(message));

      // Step 8. Close the connection. (Since this is a JCA connection, this will just place the connection back to a connection pool)
      conn.close();
   }
}
