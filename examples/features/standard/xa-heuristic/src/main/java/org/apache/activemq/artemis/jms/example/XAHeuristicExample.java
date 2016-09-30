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
package org.apache.activemq.artemis.jms.example;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.InitialContext;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.utils.UUIDGenerator;

/**
 * A simple JMS example showing how to administer un-finished transactions.
 */
public class XAHeuristicExample {

   private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://localhost:3001/jmxrmi";

   public static void main(final String[] args) throws Exception {
      Boolean result = true;
      final ArrayList<String> receiveHolder = new ArrayList<>();
      XAConnection connection = null;
      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Lookup on the queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. Perform a lookup on the XA Connection Factory
         XAConnectionFactory cf = (XAConnectionFactory) initialContext.lookup("XAConnectionFactory");

         // Step 4.Create a JMS XAConnection
         connection = cf.createXAConnection();

         // Step 5. Start the connection
         connection.start();

         // Step 6. Create a JMS XASession
         XASession xaSession = connection.createXASession();

         // Step 7. Create a normal session
         Session normalSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 8. Create a normal Message Consumer
         MessageConsumer normalConsumer = normalSession.createConsumer(queue);
         normalConsumer.setMessageListener(new SimpleMessageListener(receiveHolder, result));

         // Step 9. Get the JMS Session
         Session session = xaSession.getSession();

         // Step 10. Create a message producer
         MessageProducer producer = session.createProducer(queue);

         // Step 11. Create two Text Messages
         TextMessage helloMessage = session.createTextMessage("hello");
         TextMessage worldMessage = session.createTextMessage("world");

         // Step 12. create a transaction
         Xid xid1 = new DummyXid("xa-example1".getBytes(StandardCharsets.ISO_8859_1), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

         // Step 13. Get the JMS XAResource
         XAResource xaRes = xaSession.getXAResource();

         // Step 14. Begin the Transaction work
         xaRes.start(xid1, XAResource.TMNOFLAGS);

         // Step 15. do work, sending hello message.
         producer.send(helloMessage);

         System.out.println("Sent message " + helloMessage.getText());

         // Step 16. Stop the work for xid1
         xaRes.end(xid1, XAResource.TMSUCCESS);

         // Step 17. Prepare xid1
         xaRes.prepare(xid1);

         // Step 18. Check none should be received
         checkNoMessageReceived(receiveHolder);

         // Step 19. Create another transaction.
         Xid xid2 = new DummyXid("xa-example2".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

         // Step 20. Begin the transaction work
         xaRes.start(xid2, XAResource.TMNOFLAGS);

         // Step 21. Send the second message
         producer.send(worldMessage);

         System.out.println("Sent message " + worldMessage.getText());

         // Step 22. Stop the work for xid2
         xaRes.end(xid2, XAResource.TMSUCCESS);

         // Step 23. prepare xid2
         xaRes.prepare(xid2);

         // Step 24. Again, no messages should be received!
         checkNoMessageReceived(receiveHolder);

         // Step 25. Create JMX Connector to connect to the server's MBeanServer
         JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(JMX_URL), new HashMap<String, String>());

         // Step 26. Retrieve the MBeanServerConnection
         MBeanServerConnection mbsc = connector.getMBeanServerConnection();

         // Step 27. List the prepared transactions
         ObjectName serverObject = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), "0.0.0.0", true).getActiveMQServerObjectName();
         String[] infos = (String[]) mbsc.invoke(serverObject, "listPreparedTransactions", null, null);

         System.out.println("Prepared transactions: ");
         for (String i : infos) {
            System.out.println(i);
         }

         // Step 28. Roll back the first transaction
         mbsc.invoke(serverObject, "rollbackPreparedTransaction", new String[]{DummyXid.toBase64String(xid1)}, new String[]{"java.lang.String"});

         // Step 29. Commit the second one
         mbsc.invoke(serverObject, "commitPreparedTransaction", new String[]{DummyXid.toBase64String(xid2)}, new String[]{"java.lang.String"});

         Thread.sleep(2000);

         // Step 30. Check the result, only the 'world' message received
         checkMessageReceived("world", receiveHolder);

         // Step 31. Check the prepared transaction again, should have none.
         infos = (String[]) mbsc.invoke(serverObject, "listPreparedTransactions", null, null);
         System.out.println("No. of prepared transactions now: " + infos.length);

         // Step 32. Close the JMX Connector
         connector.close();
      } finally {
         // Step 32. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }

   private static void checkMessageReceived(final String value, ArrayList<String> receiveHolder) {
      if (receiveHolder.size() != 1) {
         throw new IllegalStateException("Number of messages received not correct ! -- " + receiveHolder.size());
      }
      String msg = receiveHolder.get(0);
      if (!msg.equals(value)) {
         throw new IllegalStateException("Received message [" + msg + "], but we expect [" + value + "]");
      }
      receiveHolder.clear();
   }

   private static void checkNoMessageReceived(ArrayList<String> receiveHolder) {
      if (receiveHolder.size() > 0) {
         throw new IllegalStateException("Message received, wrong!");
      }
      receiveHolder.clear();
   }
}

class SimpleMessageListener implements MessageListener {

   ArrayList<String> receiveHolder;
   Boolean result;

   SimpleMessageListener(ArrayList<String> receiveHolder, Boolean result) {
      this.receiveHolder = receiveHolder;
      this.result = result;
   }

   @Override
   public void onMessage(final Message message) {
      try {
         System.out.println("Message received: " + ((TextMessage) message).getText());
         receiveHolder.add(((TextMessage) message).getText());
      } catch (JMSException e) {
         result = false;
         e.printStackTrace();
      }
   }
}
