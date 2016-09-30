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
package org.apache.activemq.artemis.jms.tests.util;

import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;

import org.apache.activemq.artemis.jms.tests.ActiveMQServerTestCase;
import org.apache.activemq.artemis.utils.JNDIUtil;
import org.junit.Before;
import org.junit.Test;

public class JNDIUtilTest extends ActiveMQServerTestCase {
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private InitialContext ic;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testRebind_1() throws Exception {
      try {
         ic.lookup("/nosuchsubcontext");
         ProxyAssertSupport.fail("the name is not supposed to be there");
      } catch (NameNotFoundException e) {
         // OK
      }

      JNDIUtil.rebind(ic, "/nosuchsubcontext/sub1/sub2/sub3/name", new Integer(7));

      ProxyAssertSupport.assertEquals(7, ((Integer) ic.lookup("/nosuchsubcontext/sub1/sub2/sub3/name")).intValue());
   }

   @Test
   public void testRebind_2() throws Exception {
      try {
         ic.lookup("/doesnotexistyet");
         ProxyAssertSupport.fail("the name is not supposed to be there");
      } catch (NameNotFoundException e) {
         // OK
      }

      JNDIUtil.rebind(ic, "/doesnotexistyet", new Integer(8));

      ProxyAssertSupport.assertEquals(8, ((Integer) ic.lookup("/doesnotexistyet")).intValue());

      ic.unbind("doesnotexistyet");
   }

   @Test
   public void testRebind_3() throws Exception {
      try {
         ic.lookup("doesnotexistyet");
         ProxyAssertSupport.fail("the name is not supposed to be there");
      } catch (NameNotFoundException e) {
         // OK
      }

      JNDIUtil.rebind(ic, "doesnotexistyet", new Integer(9));

      ProxyAssertSupport.assertEquals(9, ((Integer) ic.lookup("/doesnotexistyet")).intValue());

      ic.unbind("doesnotexistyet");
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      // ServerManagement.start("none");

      ic = getInitialContext();

      log.debug("setup done");
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
