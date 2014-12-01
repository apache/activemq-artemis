/**
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
package org.apache.activemq.javaee.examples;

import org.apache.activemq.javaee.example.XARecoveryExampleStepOne;
import org.apache.activemq.javaee.example.XARecoveryExampleStepTwo;
import org.apache.activemq.javaee.example.server.XARecoveryExampleBean;
import org.apache.activemq.javaee.example.server.XARecoveryExampleService;
import org.jboss.arquillian.container.test.api.*;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author Justin Bertram
 */
@RunAsClient
@RunWith(Arquillian.class)
public class XARecoveryRunnerTest
{
   @ArquillianResource
   private ContainerController controller;
   @ArquillianResource
   private Deployer deployer;

   @Deployment(name = "deploy", managed = false)
   @TargetsContainer("jboss")
   public static Archive getDeployment()
   {
      final JavaArchive ejbJar = ShrinkWrap.create(JavaArchive.class, "ejb.jar");
      ejbJar.addClass(XARecoveryExampleBean.class);
      ejbJar.addClass(XARecoveryExampleService.class);
      System.out.println(ejbJar.toString(true));
      return ejbJar;
   }

   @Test
   @InSequence(0)
   public void runExample() throws Exception
   {
      XARecoveryExampleStepOne.main(null);
      try
      {
         controller.stop("jboss");
      }
      catch (Exception e)
      {
         //ignore
      }
   }

   @Test
   @InSequence(1)
   public void stepTwo() throws Exception
   {
      System.out.println("*****************************************************************************************************************************************************************");
      controller.start("jboss");
      XARecoveryExampleStepTwo.main(null);
      //give the example time to run
      Thread.sleep(10000);
   }

   @Test
   @InSequence(-1)
   public void startServer()
   {
      System.out.println("*****************************************************************************************************************************************************************");
      controller.start("jboss");
      System.out.println("*****************************************************************************************************************************************************************");
      deployer.deploy("deploy");
   }

   @Test
   @InSequence(2)
   public void stopServer()
   {
      deployer.undeploy("deploy");
      controller.stop("jboss");
   }
}
