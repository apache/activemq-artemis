/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.karaf;

import java.io.IOException;

import org.junit.After;
import org.ops4j.pax.exam.ExamSystem;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.TestAddress;
import org.ops4j.pax.exam.TestContainer;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.TestProbeProvider;
import org.ops4j.pax.exam.spi.PaxExamRuntime;

/**
 * This is useful for when you want to automate remote tests.
 */
public abstract class ContainerBaseTest extends KarafBaseTest {
   protected ExamSystem system;
   protected TestProbeBuilder builder;
   protected TestAddress testToBeCalled;
   protected TestProbeProvider probe;
   protected TestContainer container;

   protected void setupContainer(Class testToCall, String methodToCall, Option[] options) throws IOException {
      system = PaxExamRuntime.createTestSystem(options);
      builder = system.createProbe();
      testToBeCalled = builder.addTest(testToCall, methodToCall);
      probe = builder.build();
      container = PaxExamRuntime.createContainer(system);
      container.start();
      container.install(probe.getStream());
   }

   @After
   public void shutdownContainer() {
      if (container != null) {
         container.stop();
      }
   }


   protected void executeRemoteTest() {
      container.call(testToBeCalled);
   }


}
