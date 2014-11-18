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
package org.apache.activemq.jms.tests.message;

import org.junit.Test;

import javax.jms.DeliveryMode;
import javax.jms.Message;

import org.apache.activemq.jms.tests.util.ProxyAssertSupport;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 */
public class JMSDeliveryModeHeaderTest extends MessageHeaderTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testDefaultDeliveryMode() throws Exception
   {
      ProxyAssertSupport.assertEquals(DeliveryMode.PERSISTENT, queueProducer.getDeliveryMode());
   }

   @Test
   public void testNonPersistentDeliveryMode() throws Exception
   {
      queueProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      ProxyAssertSupport.assertEquals(DeliveryMode.NON_PERSISTENT, queueProducer.getDeliveryMode());

      Message m = queueProducerSession.createMessage();
      queueProducer.send(m);

      ProxyAssertSupport.assertEquals(DeliveryMode.NON_PERSISTENT, queueConsumer.receive().getJMSDeliveryMode());
   }

   @Test
   public void testPersistentDeliveryMode() throws Exception
   {
      queueProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
      ProxyAssertSupport.assertEquals(DeliveryMode.PERSISTENT, queueProducer.getDeliveryMode());

      Message m = queueProducerSession.createMessage();
      queueProducer.send(m);

      ProxyAssertSupport.assertEquals(DeliveryMode.PERSISTENT, queueConsumer.receive().getJMSDeliveryMode());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
