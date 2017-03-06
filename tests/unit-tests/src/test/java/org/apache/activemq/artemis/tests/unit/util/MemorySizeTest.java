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
package org.apache.activemq.artemis.tests.unit.util;

import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.server.impl.MessageReferenceImpl;
import org.apache.activemq.artemis.tests.unit.UnitTestLogger;
import org.apache.activemq.artemis.utils.MemorySize;
import org.junit.Assert;
import org.junit.Test;

public class MemorySizeTest extends Assert {

   @Test
   public void testObjectSizes() throws Exception {
      UnitTestLogger.LOGGER.info("Server message size is " + MemorySize.calculateSize(new MemorySize.ObjectFactory() {
         @Override
         public Object createObject() {
            return new CoreMessage(1, 1000);
         }
      }));

      UnitTestLogger.LOGGER.info("Message reference size is " + MemorySize.calculateSize(new MemorySize.ObjectFactory() {
         @Override
         public Object createObject() {
            return new MessageReferenceImpl();
         }
      }));
   }
}
