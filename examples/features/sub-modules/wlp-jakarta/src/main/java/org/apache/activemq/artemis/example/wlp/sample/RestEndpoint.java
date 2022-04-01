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

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;

import jakarta.annotation.Resource;
import jakarta.ejb.Stateless;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Stateless
@Path("/")
public class RestEndpoint {

   @Resource(name = "jms/sampleConnectionFactory")
   private ConnectionFactory connectionFactory;

   @Resource(name = "jms/sampleQueue")
   private Queue queue;

   private static void listContext(Context ctx, String indent) throws NamingException {
      NamingEnumeration list = ctx.listBindings("");
      while (list.hasMore()) {
         Binding item = (Binding) list.next();
         String className = item.getClassName();
         String name = item.getName();
         System.out.println(name + ": " + className);
         Object o = item.getObject();
         if (o instanceof javax.naming.Context) {
            listContext((Context) o, indent + " ");
         }
      }
   }

   @GET
   @Produces(MediaType.APPLICATION_JSON)
   public String sendMessage() {
      Connection con = null;
      try {
         con = connectionFactory.createConnection();
         Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);
         producer.send(session.createTextMessage("Sample Message"));
         con.close();
      } catch (Exception e) {
         e.printStackTrace();
      } finally {
         if (con != null) {
            try {
               con.close();
            } catch (JMSException e) {
               e.printStackTrace();
            }
         }
      }
      return "sent message";
   }
}