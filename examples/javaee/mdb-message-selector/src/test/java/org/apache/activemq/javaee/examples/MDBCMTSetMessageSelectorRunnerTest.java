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
package org.apache.activemq6.javaee.examples;

import org.apache.activemq6.javaee.example.MDBMessageSelectorClientExample;
import org.apache.activemq6.javaee.example.server.MDBMessageSelectorExample;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         5/21/12
 */
@RunAsClient
@RunWith(Arquillian.class)
public class MDBCMTSetMessageSelectorRunnerTest
{
   @Deployment
   public static Archive getDeployment()
   {
      final JavaArchive ejbJar = ShrinkWrap.create(JavaArchive.class, "mdb.jar");
      ejbJar.addClass(MDBMessageSelectorExample.class);
      System.out.println(ejbJar.toString(true));
      return ejbJar;
   }

   @Test
   public void runExample() throws Exception
   {
      MDBMessageSelectorClientExample.main(null);
      //give the example time to run
      Thread.sleep(1000);
   }
}
