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
package org.apache.activemq.javaee.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Date;
import java.util.Properties;

import org.apache.activemq.javaee.example.server.XARecoveryExampleService;

/**
 * An example which invokes an EJB. The EJB will be involved in a
 * transaction with a "buggy" XAResource to crash the server.
 * When the server is restarted, the recovery manager will recover the message
 * so that the consumer can receive it.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class XARecoveryExampleStepOne
{
   public static void main(final String[] args) throws Exception
   {
      InitialContext initialContext = null;
      try
      {
         // Step 1. Obtain an Initial Context
         Properties env = new Properties();
         env.put(Context.URL_PKG_PREFIXES, "org.jboss.ejb.client.naming");
         initialContext = new InitialContext(env);

         // Step 2. Lookup the EJB
         XARecoveryExampleService service = (XARecoveryExampleService) initialContext.lookup("ejb:/test//XARecoveryExampleBean!org.apache.activemq.javaee.example.server.XARecoveryExampleService");

         // Step 3. Invoke the send method. This will crash the server
         String message = "This is a text message sent at " + new Date();
         System.out.println("invoking the EJB service with text: " + message);
         try
         {
            service.send(message);
         }
         catch (Exception e)
         {
            System.out.println("#########################");
            System.out.println("The server crashed: " + e.getMessage());
            System.out.println("#########################");
         }

         // We will try to receive a message. Once the server is restarted, the message will be recovered and the consumer will receive it
      }
      finally
      {
         // Step 4. Be sure to close the resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
      }
   }
}
