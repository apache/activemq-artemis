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
package org.apache.activemq.core.version.impl;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Assert;


/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class VersionImplTest extends Assert
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testVersionImpl() throws Exception
   {

      String versionName = "ACTIVEMQ";
      int majorVersion = 2;
      int minorVersion = 0;
      int microVersion = 1;
      int incrementingVersion = 10;
      String versionSuffix = "suffix";
      int[] compatibleVersionList = {7,8,9,10};
      VersionImpl version = new VersionImpl(versionName,
                                            majorVersion,
                                            minorVersion,
                                            microVersion,
                                            incrementingVersion,
                                            versionSuffix,
                                            compatibleVersionList);

      Assert.assertEquals(versionName, version.getVersionName());
      Assert.assertEquals(majorVersion, version.getMajorVersion());
      Assert.assertEquals(minorVersion, version.getMinorVersion());
      Assert.assertEquals(microVersion, version.getMicroVersion());
      Assert.assertEquals(incrementingVersion, version.getIncrementingVersion());
      Assert.assertEquals(versionSuffix, version.getVersionSuffix());
   }

   @Test
   public void testEquals() throws Exception
   {
      VersionImpl version = new VersionImpl("ACTIVEMQ", 2, 0, 1, 10, "suffix", new int[]{7,8,9,10});
      VersionImpl sameVersion = new VersionImpl("ACTIVEMQ", 2, 0, 1, 10, "suffix", new int[]{7,8,9,10});
      VersionImpl differentVersion = new VersionImpl("ACTIVEMQ", 2, 0, 1, 11, "suffix", new int[]{7,8,9,10,11});

      Assert.assertFalse(version.equals(new Object()));

      Assert.assertTrue(version.equals(version));
      Assert.assertTrue(version.equals(sameVersion));
      Assert.assertFalse(version.equals(differentVersion));
   }

   @Test
   public void testSerialize() throws Exception
   {
      VersionImpl version = new VersionImpl("uyiuy", 3, 7, 6, 12, "uhuhuh", new int[]{9,10,11,12});
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(version);
      oos.flush();

      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(bais);
      VersionImpl version2 = (VersionImpl)ois.readObject();

      Assert.assertTrue(version.equals(version2));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
