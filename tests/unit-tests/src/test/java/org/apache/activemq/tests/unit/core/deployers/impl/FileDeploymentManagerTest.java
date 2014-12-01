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
package org.apache.activemq.tests.unit.core.deployers.impl;

import java.io.File;
import java.net.URI;
import java.util.Map;

import org.apache.activemq.api.core.Pair;
import org.apache.activemq.core.deployers.Deployer;
import org.apache.activemq.core.deployers.impl.FileDeploymentManager;
import org.apache.activemq.core.deployers.impl.FileDeploymentManager.DeployInfo;
import org.apache.activemq.tests.unit.UnitTestLogger;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * A FileDeploymentManagerTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class FileDeploymentManagerTest extends UnitTestCase
{
   @Test
   public void testStartStop1() throws Exception
   {
      testStartStop1("fdm_test_file.xml");
   }

   @Test
   public void testStartStop2() throws Exception
   {
      testStartStop2("fdm_test_file.xml");
   }

   @Test
   public void testStartStop1WithWhitespace() throws Exception
   {
      testStartStop1("fdm test file.xml");
      if (!isWindows())
      {
         testStartStop1("fdm\ttest\tfile.xml");
      }
   }

   @Test
   public void testStartStop2WithWhitespace() throws Exception
   {
      testStartStop2("fdm test file.xml");
      if (!isWindows())
      {
         testStartStop2("fdm\ttest\tfile.xml");
      }
   }

   private void testStartStop1(final String filename) throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);

      UnitTestLogger.LOGGER.debug("Filename is " + filename);

      File file = new File("target/test-classes/");

      file.mkdirs();

      file = new File("target/test-classes/" + filename);

      UnitTestLogger.LOGGER.debug(file.getAbsoluteFile());

      file.createNewFile();

      FakeDeployer deployer = new FakeDeployer(filename);

      fdm.registerDeployer(deployer);

      fdm.unregisterDeployer(deployer);

      fdm.registerDeployer(deployer);

      fdm.start();
      try
      {
         URI expected = file.toURI();
         URI deployedUrl = deployer.deployedUri;
         Assert.assertTrue(expected.toString().equalsIgnoreCase(deployedUrl.toString()));
         deployer.deployedUri = null;
         fdm.start();
         Assert.assertNull(deployer.deployedUri);
         fdm.stop();

      }
      finally
      {
         file.delete();
         fdm.stop();
      }
   }

   private void testStartStop2(final String filename) throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);

      UnitTestLogger.LOGGER.debug("Filename is " + filename);

      File file = new File("target/test-classes/");

      file.mkdirs();

      file = new File("target/test-classes/" + filename);

      UnitTestLogger.LOGGER.debug(file.getAbsoluteFile());

      file.createNewFile();

      FakeDeployer deployer = new FakeDeployer(filename);

      fdm.start();

      try
      {
         fdm.registerDeployer(deployer);
         URI expected = file.toURI();
         URI deployedUrl = deployer.deployedUri;
         Assert.assertTrue(expected.toString().equalsIgnoreCase(deployedUrl.toString()));
         deployer.deployedUri = null;
         fdm.start();
         Assert.assertNull(deployer.deployedUri);
         fdm.stop();
      }
      finally
      {
         file.delete();
         fdm.stop();
      }
   }

   @Test
   public void testRegisterUnregister() throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);

      fdm.start();

      String filename1 = "fdm_test_file.xml1";
      String filename2 = "fdm_test_file.xml2";
      String filename3 = "fdm_test_file.xml3";

      File file1 = new File("target/test-classes/");
      File file2 = new File("target/test-classes/");
      File file3 = new File("target/test-classes/");

      file1.mkdirs();
      file2.mkdirs();
      file3.mkdirs();

      file1 = new File("target/test-classes/" + filename1);
      file2 = new File("target/test-classes/" + filename2);
      file3 = new File("target/test-classes/" + filename3);

      file1.createNewFile();
      file2.createNewFile();
      file3.createNewFile();

      FakeDeployer deployer1 = new FakeDeployer(filename1);
      FakeDeployer deployer2 = new FakeDeployer(filename2);
      FakeDeployer deployer3 = new FakeDeployer(filename3);
      FakeDeployer deployer4 = new FakeDeployer(filename3); // Can have multiple deployers on the same file
      try
      {
         URI url1 = file1.toURI();
         deployer1.deploy(url1);

         URI url2 = file2.toURI();
         deployer2.deploy(url2);

         URI url3 = file3.toURI();
         deployer3.deploy(url3);

         deployer4.deploy(url3);

         fdm.registerDeployer(deployer1);
         fdm.registerDeployer(deployer2);
         fdm.registerDeployer(deployer3);
         fdm.registerDeployer(deployer4);

         Assert.assertEquals(4, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer1));
         Assert.assertTrue(fdm.getDeployers().contains(deployer2));
         Assert.assertTrue(fdm.getDeployers().contains(deployer3));
         Assert.assertTrue(fdm.getDeployers().contains(deployer4));
         Assert.assertEquals(4, fdm.getDeployed().size());

         Assert.assertEquals(file1.toURI(), deployer1.deployedUri);
         Assert.assertEquals(file2.toURI(), deployer2.deployedUri);
         Assert.assertEquals(file3.toURI(), deployer3.deployedUri);
         Assert.assertEquals(file3.toURI(), deployer4.deployedUri);
         // Registering same again should do nothing

         fdm.registerDeployer(deployer1);

         Assert.assertEquals(4, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer1));
         Assert.assertTrue(fdm.getDeployers().contains(deployer2));
         Assert.assertTrue(fdm.getDeployers().contains(deployer3));
         Assert.assertTrue(fdm.getDeployers().contains(deployer4));
         Assert.assertEquals(4, fdm.getDeployed().size());

         fdm.unregisterDeployer(deployer1);

         Assert.assertEquals(3, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer2));
         Assert.assertTrue(fdm.getDeployers().contains(deployer3));
         Assert.assertTrue(fdm.getDeployers().contains(deployer4));
         Assert.assertEquals(3, fdm.getDeployed().size());

         fdm.unregisterDeployer(deployer2);
         fdm.unregisterDeployer(deployer3);

         Assert.assertEquals(1, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer4));
         Assert.assertEquals(1, fdm.getDeployed().size());

         fdm.unregisterDeployer(deployer4);

         Assert.assertEquals(0, fdm.getDeployers().size());
         Assert.assertEquals(0, fdm.getDeployed().size());

         // Now unregister again - should do nothing

         fdm.unregisterDeployer(deployer1);

         Assert.assertEquals(0, fdm.getDeployers().size());
         Assert.assertEquals(0, fdm.getDeployed().size());
      }
      finally
      {
         file1.delete();
         file2.delete();
         file3.delete();
         fdm.stop();
      }

   }

   @Test
   public void testRedeploy() throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);

      fdm.start();

      String filename = "fdm_test_file.xml1";

      File file = new File("target/test-classes/");

      file.mkdirs();

      file = new File("target/test-classes/" + filename);

      file.createNewFile();
      long oldLastModified = file.lastModified();

      FakeDeployer deployer = new FakeDeployer(filename);
      try
      {
         URI url = file.toURI();
         deployer.deploy(url);

         fdm.registerDeployer(deployer);
         Assert.assertEquals(file.toURI(), deployer.deployedUri);
         // Touch the file
         file.setLastModified(oldLastModified + 1000);

         deployer.redeploy(url);

         fdm.run();

         Assert.assertEquals(1, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer));
         Map<Pair<URI, Deployer>, DeployInfo> info = fdm.getDeployed();
         Assert.assertEquals(1, info.size());
         URI expected = file.toURI();
         URI deployedUrl = deployer.deployedUri;
         Assert.assertTrue(expected.toString().equalsIgnoreCase(deployedUrl.toString()));
         Pair<URI, Deployer> pair = new Pair<URI, Deployer>(url, deployer);
         Assert.assertEquals(oldLastModified + 1000, fdm.getDeployed().get(pair).lastModified);
         deployer.reDeployedUri = null;
         // Scanning again should not redeploy

         fdm.run();

         Assert.assertEquals(oldLastModified + 1000, fdm.getDeployed().get(pair).lastModified);
         Assert.assertNull(deployer.reDeployedUri);
      }
      finally
      {
         file.delete();
         fdm.stop();
      }
   }

   @Test
   public void testUndeployAndDeployAgain() throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);

      fdm.start();

      String filename = "fdm_test_file.xml1";

      File file = new File("target/test-classes/");

      file.mkdirs();

      file = new File("target/test-classes/" + filename);

      file.createNewFile();

      FakeDeployer deployer = new FakeDeployer(filename);
      try
      {
         URI uri = file.toURI();
         deployer.deploy(uri);

         fdm.registerDeployer(deployer);

         Assert.assertEquals(1, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer));
         Assert.assertEquals(1, fdm.getDeployed().size());
         Assert.assertEquals(file.toURI(), deployer.deployedUri);
         deployer.deployedUri = null;
         file.delete();

         // This should cause undeployment

         deployer.undeploy(uri);
         Assert.assertEquals(file.toURI(), deployer.unDeployedUri);

         fdm.run();

         Assert.assertEquals(1, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer));
         Assert.assertEquals(0, fdm.getDeployed().size());

         // Recreate file and it should be redeployed

         file.createNewFile();

         deployer.deploy(uri);

         fdm.run();

         Assert.assertEquals(1, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer));
         Assert.assertEquals(1, fdm.getDeployed().size());

         Assert.assertEquals(file.toURI(), deployer.deployedUri);
      }
      finally
      {
         file.delete();
         fdm.stop();
      }
   }

   class FakeDeployer implements Deployer
   {
      URI deployedUri;

      URI unDeployedUri;

      URI reDeployedUri;

      boolean started;

      private final String file;

      public FakeDeployer(final String file)
      {
         this.file = file;
      }

      public String[] getConfigFileNames()
      {
         return new String[]{file};
      }

      @Override
      public void deploy(final URI url) throws Exception
      {
         deployedUri = url;
      }

      @Override
      public void redeploy(final URI url) throws Exception
      {
         reDeployedUri = url;
      }

      @Override
      public void undeploy(final URI url) throws Exception
      {
         unDeployedUri = url;
      }

      @Override
      public void start() throws Exception
      {
         started = true;
      }

      @Override
      public void stop() throws Exception
      {
         started = false;
      }

      @Override
      public boolean isStarted()
      {
         return started;
      }
   }
}
