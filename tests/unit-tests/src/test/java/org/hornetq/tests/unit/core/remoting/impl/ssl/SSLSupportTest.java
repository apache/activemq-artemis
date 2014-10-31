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
package org.hornetq.tests.unit.core.remoting.impl.ssl;

import java.io.File;
import java.net.URL;

import org.hornetq.core.remoting.impl.ssl.SSLSupport;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class SSLSupportTest extends UnitTestCase
{

   private String keyStoreProvider;

   private String keyStorePath;

   private String keyStorePassword;

   private String trustStoreProvider;

   private String trustStorePath;

   private String trustStorePassword;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      keyStoreProvider = "JKS";
      keyStorePath = "server-side.keystore";
      keyStorePassword = "secureexample";
      trustStoreProvider = "JKS";
      trustStorePath = "server-side.truststore";
      trustStorePassword = keyStorePassword;
   }

   @Test
   public void testContextWithRightParameters() throws Exception
   {
      SSLSupport.createContext(keyStoreProvider, keyStorePath, keyStorePassword, trustStoreProvider, trustStorePath, trustStorePassword);
   }

   // This is valid as it will create key and trust managers with system defaults
   @Test
   public void testContextWithNullParameters() throws Exception
   {
      SSLSupport.createContext(null, null, null, null, null, null);
   }

   @Test
   public void testContextWithKeyStorePathAsURL() throws Exception
   {
      URL url = Thread.currentThread().getContextClassLoader().getResource(keyStorePath);
      SSLSupport.createContext(keyStoreProvider, url.toString(), keyStorePassword, trustStoreProvider, trustStorePath, trustStorePassword);
   }

   @Test
   public void testContextWithKeyStorePathAsFile() throws Exception
   {
      URL url = Thread.currentThread().getContextClassLoader().getResource(keyStorePath);
      File file = new File(url.toURI());
      SSLSupport.createContext(keyStoreProvider, file.getAbsolutePath(), keyStorePassword, trustStoreProvider, trustStorePath, trustStorePassword);
   }

   @Test
   public void testContextWithBadKeyStorePath() throws Exception
   {
      try
      {
         SSLSupport.createContext(keyStoreProvider, "not a keystore", keyStorePassword, trustStoreProvider, trustStorePath, trustStorePassword);
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   @Test
   public void testContextWithNullKeyStorePath() throws Exception
   {
      try
      {
         SSLSupport.createContext(keyStoreProvider, null, keyStorePassword, trustStoreProvider, trustStorePath, trustStorePassword);
      }
      catch (Exception e)
      {
         Assert.fail();
      }
   }

   @Test
   public void testContextWithKeyStorePathAsRelativePath() throws Exception
   {
      // this test is dependent on a path relative to the tests directory.
      // it will fail if launch from somewhere else (or from an IDE)
      File currentDir = new File(System.getProperty("user.dir"));
      if (!currentDir.getAbsolutePath().endsWith("tests"))
      {
         return;
      }

      SSLSupport.createContext(keyStoreProvider, "src/test/resources/server-side.keystore", keyStorePassword, trustStoreProvider, trustStorePath, trustStorePassword);
   }

   @Test
   public void testContextWithBadKeyStorePassword() throws Exception
   {
      try
      {
         SSLSupport.createContext(keyStoreProvider, keyStorePath, "bad password", trustStoreProvider, trustStorePath, trustStorePassword);
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   @Test
   public void testContextWithBadTrustStorePath() throws Exception
   {
      try
      {
         SSLSupport.createContext(keyStoreProvider, keyStorePath, keyStorePassword, trustStoreProvider, "not a trust store", trustStorePassword);
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   @Test
   public void testContextWithBadTrustStorePassword() throws Exception
   {
      try
      {
         SSLSupport.createContext(keyStoreProvider, keyStorePath, keyStorePassword, trustStoreProvider, trustStorePath, "bad passord");
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
