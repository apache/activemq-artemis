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
package org.apache.activemq6.tests.unit.jms.client;

import static org.apache.activemq6.api.core.HornetQExceptionType.CONNECTION_TIMEDOUT;
import static org.apache.activemq6.api.core.HornetQExceptionType.GENERIC_EXCEPTION;
import static org.apache.activemq6.api.core.HornetQExceptionType.INVALID_FILTER_EXPRESSION;

import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.HornetQExceptionType;
import org.apache.activemq6.jms.client.JMSExceptionHelper;
import org.apache.activemq6.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class JMSExceptionHelperTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testCONNECTION_TIMEDOUT() throws Exception
   {
      doConvertException(CONNECTION_TIMEDOUT, JMSException.class);
   }

   @Test
   public void testILLEGAL_STATE() throws Exception
   {
      doConvertException(HornetQExceptionType.ILLEGAL_STATE, IllegalStateException.class);
   }

   @Test
   public void testINTERNAL_ERROR() throws Exception
   {
      doConvertException(HornetQExceptionType.INTERNAL_ERROR, JMSException.class);
   }

   @Test
   public void testINVALID_FILTER_EXPRESSION() throws Exception
   {
      doConvertException(INVALID_FILTER_EXPRESSION, InvalidSelectorException.class);
   }

   @Test
   public void testNOT_CONNECTED() throws Exception
   {
      doConvertException(HornetQExceptionType.NOT_CONNECTED, JMSException.class);
   }

   @Test
   public void testOBJECT_CLOSED() throws Exception
   {
      doConvertException(HornetQExceptionType.OBJECT_CLOSED, IllegalStateException.class);
   }

   @Test
   public void testQUEUE_DOES_NOT_EXIST() throws Exception
   {
      doConvertException(HornetQExceptionType.QUEUE_DOES_NOT_EXIST, InvalidDestinationException.class);
   }

   @Test
   public void testQUEUE_EXISTS() throws Exception
   {
      doConvertException(HornetQExceptionType.QUEUE_EXISTS, InvalidDestinationException.class);
   }

   @Test
   public void testSECURITY_EXCEPTION() throws Exception
   {
      doConvertException(HornetQExceptionType.SECURITY_EXCEPTION, JMSSecurityException.class);
   }

   @Test
   public void testUNSUPPORTED_PACKET() throws Exception
   {
      doConvertException(HornetQExceptionType.UNSUPPORTED_PACKET, IllegalStateException.class);
   }

   @Test
   public void testDefault() throws Exception
   {
      doConvertException(GENERIC_EXCEPTION, JMSException.class);
   }

   private void doConvertException(final HornetQExceptionType errorCode,
                                   final Class<? extends Throwable> expectedException)
   {
      HornetQException me = new HornetQException(errorCode);
      Exception e = JMSExceptionHelper.convertFromHornetQException(me);
      Assert.assertNotNull(e);
      Assert.assertTrue(e.getClass().isAssignableFrom(expectedException));
   }
}
