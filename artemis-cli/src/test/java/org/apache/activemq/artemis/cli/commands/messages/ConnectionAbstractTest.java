/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.cli.commands.messages;

import org.apache.activemq.cli.test.TestActionContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class ConnectionAbstractTest {

   @Test
   public void testDefaultAcceptor() throws Exception {
      ConnectionAbstract connectionAbstract = new ConnectionAbstract();

      File brokerInstanceEtc = new File(this.getClass().getClassLoader()
         .getResource("broker.xml").getFile()).getParentFile();

      System.setProperty("artemis.instance.etc", brokerInstanceEtc.getAbsolutePath());
      try {
         connectionAbstract.setHomeValues(null, brokerInstanceEtc.getParentFile(), null);

         connectionAbstract.execute(new TestActionContext());

         Assert.assertEquals(ConnectionAbstract.DEFAULT_BROKER_URL, connectionAbstract.getBrokerURL());
      } finally {
         System.clearProperty("artemis.instance.etc");
      }
   }

   @Test
   public void testAMQPAcceptor() throws Exception {
      ConnectionAbstract connectionAbstract = new ConnectionAbstract();

      File brokerInstanceEtc = new File(this.getClass().getClassLoader()
         .getResource("broker.xml").getFile()).getParentFile();


      System.setProperty("artemis.instance.etc", brokerInstanceEtc.getAbsolutePath());
      try {
         connectionAbstract.setHomeValues(null, brokerInstanceEtc.getParentFile(), null);
         connectionAbstract.setAcceptor("amqp");

         connectionAbstract.execute(new TestActionContext());

         Assert.assertEquals("tcp://localhost:5672", connectionAbstract.getBrokerURL());
      } finally {
         System.clearProperty("artemis.instance.etc");
      }
   }
}
