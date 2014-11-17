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

import java.io.Serializable;
import java.util.Arrays;

import org.apache.activemq.core.version.Version;

/**
 * A VersionImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class VersionImpl implements Version, Serializable
{
   private static final long serialVersionUID = -5271227256591080403L;

   private final String versionName;

   private final int majorVersion;

   private final int minorVersion;

   private final int microVersion;

   private final int incrementingVersion;

   private final String versionSuffix;

   private final int[] compatibleVersionList;

   // Constructors --------------------------------------------------

   public VersionImpl(final String versionName,
                      final int majorVersion,
                      final int minorVersion,
                      final int microVersion,
                      final int incrementingVersion,
                      final String versionSuffix,
                      final int[] compatibleVersionList)
   {
      this.versionName = versionName;

      this.majorVersion = majorVersion;

      this.minorVersion = minorVersion;

      this.microVersion = microVersion;

      this.incrementingVersion = incrementingVersion;

      this.versionSuffix = versionSuffix;

      this.compatibleVersionList = Arrays.copyOf(compatibleVersionList, compatibleVersionList.length);
   }

   // Version implementation ------------------------------------------

   public String getFullVersion()
   {
      return majorVersion + "." +
             minorVersion +
             "." +
             microVersion +
             "." +
             versionSuffix +
             " (" +
             versionName +
             ", " +
             incrementingVersion +
             ")";
   }

   public String getVersionName()
   {
      return versionName;
   }

   public int getMajorVersion()
   {
      return majorVersion;
   }

   public int getMinorVersion()
   {
      return minorVersion;
   }

   public int getMicroVersion()
   {
      return microVersion;
   }

   public String getVersionSuffix()
   {
      return versionSuffix;
   }

   public int getIncrementingVersion()
   {
      return incrementingVersion;
   }

   public boolean isCompatible(int version)
   {
      for (int element : compatibleVersionList)
      {
         if (element == version)
         {
            return true;
         }
      }
      return false;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(compatibleVersionList);
      result = prime * result + incrementingVersion;
      result = prime * result + majorVersion;
      result = prime * result + microVersion;
      result = prime * result + minorVersion;
      result = prime * result + ((versionName == null) ? 0 : versionName.hashCode());
      result = prime * result + ((versionSuffix == null) ? 0 : versionSuffix.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
      {
         return true;
      }
      if (obj == null)
      {
         return false;
      }
      if (!(obj instanceof VersionImpl))
      {
         return false;
      }
      VersionImpl other = (VersionImpl)obj;
      if (!Arrays.equals(compatibleVersionList, other.compatibleVersionList))
      {
         return false;
      }
      if (incrementingVersion != other.incrementingVersion)
      {
         return false;
      }
      if (majorVersion != other.majorVersion)
      {
         return false;
      }
      if (microVersion != other.microVersion)
      {
         return false;
      }
      if (minorVersion != other.minorVersion)
      {
         return false;
      }
      if (versionName == null)
      {
         if (other.versionName != null)
         {
            return false;
         }
      }
      else if (!versionName.equals(other.versionName))
      {
         return false;
      }
      if (versionSuffix == null)
      {
         if (other.versionSuffix != null)
         {
            return false;
         }
      }
      else if (!versionSuffix.equals(other.versionSuffix))
      {
         return false;
      }
      return true;
   }
}
