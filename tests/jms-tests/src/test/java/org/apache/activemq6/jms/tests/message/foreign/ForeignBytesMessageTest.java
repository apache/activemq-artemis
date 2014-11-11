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
package org.apache.activemq6.jms.tests.message.foreign;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq6.jms.tests.message.SimpleJMSBytesMessage;
import org.apache.activemq6.jms.tests.util.ProxyAssertSupport;

/**
 *
 * Tests the delivery/receipt of a foreign byte message
 *
 *
 * @author <a href="mailto:a.walker@base2group.com>Aaron Walker</a>
 *
 */
public class ForeignBytesMessageTest extends ForeignMessageTest
{
   @Override
   protected Message createForeignMessage() throws Exception
   {
      SimpleJMSBytesMessage m = new SimpleJMSBytesMessage();

      log.debug("creating JMS Message type " + m.getClass().getName());

      String bytes = "HornetQ";
      m.writeBytes(bytes.getBytes());
      return m;
   }

   @Override
   protected void assertEquivalent(final Message m, final int mode, final boolean redelivery) throws JMSException
   {
      super.assertEquivalent(m, mode, redelivery);

      BytesMessage byteMsg = (BytesMessage)m;

      StringBuffer sb = new StringBuffer();
      byte[] buffer = new byte[1024];
      int n = byteMsg.readBytes(buffer);
      while (n != -1)
      {
         sb.append(new String(buffer, 0, n));
         n = byteMsg.readBytes(buffer);
      }
      ProxyAssertSupport.assertEquals("HornetQ", sb.toString());
   }
}