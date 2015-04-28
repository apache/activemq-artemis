/**
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
package org.apache.activemq.artemis.jms;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.objectweb.jtests.jms.admin.Admin;

/**
 * AbstractAdmin.
 */
public class AbstractAdmin implements Admin
{
   public String getName()
   {
      return getClass().getName();
   }

   public void start()
   {
   }

   public void stop() throws Exception
   {

   }

   public InitialContext createContext() throws NamingException
   {
      return new InitialContext();
   }

   public void createConnectionFactory(final String name)
   {
      throw new RuntimeException("FIXME NYI createConnectionFactory");
   }

   public void deleteConnectionFactory(final String name)
   {
      throw new RuntimeException("FIXME NYI deleteConnectionFactory");
   }

   public void createQueue(final String name)
   {
      throw new RuntimeException("FIXME NYI createQueue");
   }

   public void deleteQueue(final String name)
   {
      throw new RuntimeException("FIXME NYI deleteQueue");
   }

   public void createQueueConnectionFactory(final String name)
   {
      createConnectionFactory(name);
   }

   public void deleteQueueConnectionFactory(final String name)
   {
      deleteConnectionFactory(name);
   }

   public void createTopic(final String name)
   {
      throw new RuntimeException("FIXME NYI createTopic");
   }

   public void deleteTopic(final String name)
   {
      throw new RuntimeException("FIXME NYI deleteTopic");
   }

   public void createTopicConnectionFactory(final String name)
   {
      createConnectionFactory(name);
   }

   public void deleteTopicConnectionFactory(final String name)
   {
      deleteConnectionFactory(name);
   }

   public void startServer()
   {
   }

   public void stopServer()
   {
   }
}
