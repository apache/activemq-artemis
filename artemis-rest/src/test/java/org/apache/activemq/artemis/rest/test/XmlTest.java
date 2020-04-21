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
package org.apache.activemq.artemis.rest.test;

import javax.xml.bind.JAXBContext;
import java.io.StringReader;

import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;
import org.jboss.logging.Logger;
import org.junit.Test;

public class XmlTest {
   private static final Logger log = Logger.getLogger(XmlTest.class);

   @Test
   public void testPush() throws Exception {
      String xml = "<push-registration id=\"111\">\n" +
         "   <destination>bar</destination>\n" +
         "   <durable>true</durable>\n" +
         "   <session-count>10</session-count>\n" +
         "   <link rel=\"template\" href=\"http://somewhere.com/resources/{id}/messages\" method=\"PUT\"/>\n" +
         "   <authentication>\n" +
         "      <basic-auth><username>guest</username><password>geheim</password></basic-auth>" +
         "   </authentication>\n" +
         "   <header name=\"foo\">bar</header>" +
         "</push-registration>";

      JAXBContext ctx = JAXBContext.newInstance(PushRegistration.class);
      PushRegistration reg = (PushRegistration) ctx.createUnmarshaller().unmarshal(new StringReader(xml));

      log.debug(reg);
   }
}
