/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import javax.jms.JMSException;
import java.io.File;

public class TransferTest {

   @Test
   public void testDefaultSourceAcceptor() {
      Transfer transfer = new Transfer();

      File brokerInstanceEtc = new File(this.getClass().getClassLoader()
         .getResource("broker.xml").getFile()).getParentFile();

      System.setProperty("artemis.instance.etc", brokerInstanceEtc.getAbsolutePath());
      try {
         transfer.setHomeValues(null, brokerInstanceEtc.getParentFile(), null);

         try {
            transfer.execute(new TestActionContext());
         } catch (Exception e) {
            Assert.assertEquals(JMSException.class, e.getClass());
         }

         Assert.assertEquals(ConnectionAbstract.DEFAULT_BROKER_URL, transfer.getSourceURL());
      } finally {
         System.clearProperty("artemis.instance.etc");
      }
   }

   @Test
   public void testAMQPSourceAcceptor() {
      Transfer transfer = new Transfer();

      File brokerInstanceEtc = new File(this.getClass().getClassLoader()
         .getResource("broker.xml").getFile()).getParentFile();

      System.setProperty("artemis.instance.etc", brokerInstanceEtc.getAbsolutePath());
      try {
         transfer.setHomeValues(null, brokerInstanceEtc.getParentFile(), null);
         transfer.setSourceAcceptor("amqp");

         try {
            transfer.execute(new TestActionContext());
         } catch (Exception e) {
            Assert.assertEquals(JMSException.class, e.getClass());
         }

         Assert.assertEquals("tcp://localhost:5672", transfer.getSourceURL());
      } finally {
         System.clearProperty("artemis.instance.etc");
      }
   }
}
