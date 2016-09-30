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

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.InitialContext;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;

/**
 * An example that shows how to kick off a client connected to ActiveMQ Artemis by using JMX.
 */
public class ClientKickoffExample {

   private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://localhost:3000/jmxrmi";

   public static void main(final String[] args) throws Exception {
      QueueConnection connection = null;
      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perform a lookup on the Connection Factory
         QueueConnectionFactory cf = (QueueConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 3.Create a JMS Connection
         connection = cf.createQueueConnection();

         // Step 4. Set an exception listener on the connection to be notified after a problem occurred
         final AtomicReference<JMSException> exception = new AtomicReference<>();
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(final JMSException e) {
               exception.set(e);
            }
         });

         // Step 5. We start the connection
         connection.start();

         // Step 6. Create an ActiveMQServerControlMBean proxy to manage the server
         ObjectName on = ObjectNameBuilder.DEFAULT.getActiveMQServerObjectName();
         JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(JMX_URL), new HashMap<String, String>());
         MBeanServerConnection mbsc = connector.getMBeanServerConnection();
         ActiveMQServerControl serverControl = MBeanServerInvocationHandler.newProxyInstance(mbsc, on, ActiveMQServerControl.class, false);

         // Step 7. List the remote address connected to the server
         System.out.println("List of remote addresses connected to the server:");
         System.out.println("----------------------------------");
         String[] remoteAddresses = serverControl.listRemoteAddresses();
         for (String remoteAddress : remoteAddresses) {
            System.out.println(remoteAddress);
         }
         System.out.println("----------------------------------");

         // Step 8. Close the connections for the 1st remote address and kickoff the client
         serverControl.closeConnectionsForAddress(remoteAddresses[0]);

         // Sleep a little bit so that the stack trace from the server won't be
         // mingled with the JMSException received on the ExceptionListener
         Thread.sleep(1000);

         // Step 9. Display the exception received by the connection's ExceptionListener
         System.err.println("\nException received from the server:");
         System.err.println("----------------------------------");
         exception.get().printStackTrace();
         System.err.println("----------------------------------");
      } finally {
         // Step 10. Be sure to close the resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}
