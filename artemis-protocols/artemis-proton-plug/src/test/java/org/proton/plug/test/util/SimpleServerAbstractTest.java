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
package org.proton.plug.test.util;

import org.proton.plug.test.AbstractJMSTest;
import org.proton.plug.test.Constants;
import org.proton.plug.test.invm.InVMTestConnector;
import org.proton.plug.test.minimalclient.Connector;
import org.proton.plug.test.minimalclient.SimpleAMQPConnector;
import org.proton.plug.test.minimalserver.DumbServer;
import org.proton.plug.test.minimalserver.MinimalServer;
import org.junit.After;
import org.junit.Before;

public class SimpleServerAbstractTest {

   protected final boolean useSASL;
   protected final boolean useInVM;
   protected MinimalServer server = new MinimalServer();

   public SimpleServerAbstractTest(boolean useSASL, boolean useInVM) {
      this.useSASL = useSASL;
      this.useInVM = useInVM;
   }

   @Before
   public void setUp() throws Exception {
      DumbServer.clear();
      AbstractJMSTest.forceGC();
      if (!useInVM) {
         server.start("127.0.0.1", Constants.PORT, useSASL);
      }

   }

   @After
   public void tearDown() throws Exception {
      if (!useInVM) {
         server.stop();
      }
      DumbServer.clear();
   }

   protected Connector newConnector() {
      if (useInVM) {
         return new InVMTestConnector();
      }
      else {
         return new SimpleAMQPConnector();
      }
   }

}
