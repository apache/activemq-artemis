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

package org.apache.activemq.uri;

import javax.jms.ConnectionFactory;
import javax.jms.XAQueueConnectionFactory;
import java.net.URI;

import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author clebertsuconic
 */

public class ConnectionFactoryURITest
{
   ConnectionFactoryParser parser = new ConnectionFactoryParser();


   @Test
   public void testUDP() throws Exception
   {
      ActiveMQConnectionFactory factory = parser.newObject(new URI("udp://localhost:3030?ha=true&type=QUEUE_XA_CF"));

      Assert.assertTrue(factory instanceof XAQueueConnectionFactory);
   }

   @Test
   public void testInvalidCFType() throws Exception
   {
      ActiveMQConnectionFactory factory = parser.newObject(new URI("udp://localhost:3030?ha=true&type=QUEUE_XA_CFInvalid"));

      Assert.assertTrue(factory instanceof ConnectionFactory);
   }

   @Test
   public void testJGroups() throws Exception
   {
      ActiveMQConnectionFactory factory = parser.newObject(new URI("jgroups://test.xml?test=33"));

//      Assert.assertTrue(factory instanceof ConnectionFactory);
   }
}
