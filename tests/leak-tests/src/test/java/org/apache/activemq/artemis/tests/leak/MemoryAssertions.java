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

package org.apache.activemq.artemis.tests.leak;

import java.lang.invoke.MethodHandles;

import io.github.checkleak.core.CheckLeak;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireConnection;
import org.apache.activemq.artemis.core.server.impl.MessageReferenceImpl;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerReceiverContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerSenderContext;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryAssertions {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   /** most tests should have these as 0 after execution. */
   public static void basicMemoryAsserts() throws Exception {
      CheckLeak checkLeak = new CheckLeak();
      assertMemory(checkLeak, 0, OpenWireConnection.class.getName());
      assertMemory(checkLeak, 0, ProtonServerSenderContext.class.getName());
      assertMemory(checkLeak, 0, ProtonServerReceiverContext.class.getName());
      assertMemory(checkLeak, 0, AMQPSessionContext.class.getName());
      assertMemory(checkLeak, 0, ServerConsumerImpl.class.getName());
      assertMemory(checkLeak, 0, RoutingContextImpl.class.getName());
      assertMemory(checkLeak, 0, MessageReferenceImpl.class.getName());
   }

   public static void assertMemory(CheckLeak checkLeak, int maxExpected, String clazz) throws Exception {
      Wait.waitFor(() -> checkLeak.getAllObjects(clazz).length <= maxExpected, 5000, 100);

      Object[] objects = checkLeak.getAllObjects(clazz);
      if (objects.length > maxExpected) {
         for (Object obj : objects) {
            logger.warn("Object {} still in the heap", obj);
         }
         String report = checkLeak.exploreObjectReferences(5, 10, true, objects);
         logger.info(report);

         Assert.fail("Class " + clazz + " has leaked " + objects.length + " objects\n" + report);
      }
   }

}
