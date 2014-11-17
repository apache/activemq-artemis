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

import org.apache.activemq.javaee.example.server2.StatelessSenderService;

import javax.jms.Connection;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

/**
 *
 * MDB Remote & JCA Configuration Example.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class MDBRemoteClientExample
{
   public static void main(String[] args) throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the EJB lookup.
         Properties env = new Properties();
         env.put(Context.URL_PKG_PREFIXES, "org.jboss.ejb.client.naming");
         initialContext = new InitialContext(env);

         // Step 2. Lookup the EJB
         StatelessSenderService sender = (StatelessSenderService)initialContext.lookup("ejb:/test//StatelessSender!org.apache.activemq.javaee.example.server2.StatelessSenderService");

         //Step 3. Calling a Stateless Session Bean. You will have more steps on the SessionBean
         sender.sendHello("Hello there MDB!");

         System.out.println("Step 3: Invoking the Stateless Bean");

         initialContext.close();
      }
      finally
      {
         //Step 11. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if(connection != null)
         {
            connection.close();
         }
      }
   }
}
