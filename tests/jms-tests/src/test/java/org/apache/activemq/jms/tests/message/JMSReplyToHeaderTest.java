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
package org.apache.activemq6.jms.tests.message;

import org.junit.Test;

import javax.jms.Message;
import javax.jms.TemporaryQueue;

import org.apache.activemq6.jms.tests.util.ProxyAssertSupport;

/**
 *
 * A JMSReplyToHeaderTest

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class JMSReplyToHeaderTest extends MessageHeaderTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testJMSDestinationSimple() throws Exception
   {
      Message m = queueProducerSession.createMessage();
      TemporaryQueue tempQ = queueProducerSession.createTemporaryQueue();
      m.setJMSReplyTo(tempQ);

      queueProducer.send(m);
      queueConsumer.receive();
      ProxyAssertSupport.assertEquals(tempQ, m.getJMSReplyTo());
   }

   @Test
   public void testJMSDestinationNull() throws Exception
   {
      Message m = queueProducerSession.createMessage();
      m.setJMSReplyTo(null);

      queueProducer.send(m);
      queueConsumer.receive();
      ProxyAssertSupport.assertNull(m.getJMSReplyTo());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
