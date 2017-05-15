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
package org.apache.activemq.artemis.jndi;

import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

import org.junit.Test;

public class ActiveMQInitialContextFactoryTest {

   @Test
   public void providerURLTest() throws NamingException {
      String url = "(tcp://somehost:62616,tcp://somehost:62616)?ha=true";

      Properties props = new Properties();
      props.setProperty(javax.naming.Context.INITIAL_CONTEXT_FACTORY, ActiveMQInitialContextFactory.class.getName());
      props.setProperty(javax.naming.Context.PROVIDER_URL, url);

      InitialContext context =  new InitialContext(props);
      ConnectionFactory connectionFactory = (ConnectionFactory)context.lookup("ConnectionFactory");
   }

   @Test
   public void connectionFactoryProperty() throws NamingException {
      String url = "(tcp://somehost:62616,tcp://somehost:62616)?ha=true";

      Properties props = new Properties();
      props.setProperty(javax.naming.Context.INITIAL_CONTEXT_FACTORY, ActiveMQInitialContextFactory.class.getName());
      props.setProperty(javax.naming.Context.PROVIDER_URL, url);

      props.setProperty("connectionFactory.ConnectionFactory",url);

      InitialContext context =  new InitialContext(props);
      ConnectionFactory connectionFactory = (ConnectionFactory)context.lookup("ConnectionFactory");
   }
}
