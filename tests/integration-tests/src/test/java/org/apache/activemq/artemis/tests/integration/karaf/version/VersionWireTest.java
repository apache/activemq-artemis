/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.karaf.version;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.integration.karaf.KarafBaseTest;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

import static org.ops4j.pax.exam.CoreOptions.vmOptions;

// uncomment this to be able to debug it
// import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.debugConfiguration;

/**
 * Useful docs about this test: https://ops4j1.jira.com/wiki/display/paxexam/FAQ
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class VersionWireTest extends KarafBaseTest {

   File file = new File(System.getProperty("./target/generated.bin", System.getProperty("testFile", "./target/generated.bin")));


   private static Logger LOG = Logger.getLogger(VersionWireTest.class.getName());

   /**
    * plug to add more options on sub tests
    */
   @Override
   protected void testOptions(List<Option> options) throws Exception {
      options.add(vmOptions("-DtestFile=" + file.getCanonicalPath()));
   }

   @ProbeBuilder
   public TestProbeBuilder probeConfiguration(TestProbeBuilder probe) throws Exception {

      file.deleteOnExit();
      System.out.println("Path::" + file.getCanonicalPath());
      PrintStream out = new PrintStream(new FileOutputStream(file));
      out.println("hello");
      out.close();
      System.out.println("probing!!!");
      Message message = new ClientMessageImpl();
      System.out.println("probed!!!");
      return probe;
   }


   @Configuration
   public Option[] configure1_5() throws Exception {

      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
      factory.setBlockOnDurableSend(false);

      return configureArtemisFeatures(false, "1.5.0", "artemis-core");

   }

   @Configuration
   public Option[] configure13() throws Exception {
      return configureArtemisFeatures(false, null, "artemis-core");
   }


   @Test
   public void testSample() throws Throwable {
      System.out.println("Path::" + file.getCanonicalPath());

      Assert.assertTrue(file.getCanonicalPath() + " don't exist", file.exists());
      System.out.println("Hello!!!");
      ClientMessageImpl message = new ClientMessageImpl();
   }
}
