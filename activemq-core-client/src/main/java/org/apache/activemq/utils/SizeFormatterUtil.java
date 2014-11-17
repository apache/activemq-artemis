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
package org.apache.activemq6.utils;

/**
 * A SizeFormatterUtil
 *
 * @author <a href="mailto:jmesnil@gmail.com">Jeff Mesnil</a>
 *
 *
 */
public class SizeFormatterUtil
{

   // Constants -----------------------------------------------------

   private static long oneKiB = 1024;

   private static long oneMiB = SizeFormatterUtil.oneKiB * 1024;

   private static long oneGiB = SizeFormatterUtil.oneMiB * 1024;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static String sizeof(final long size)
   {
      double s = Long.valueOf(size).doubleValue();
      String suffix = "B";
      if (s > SizeFormatterUtil.oneGiB)
      {
         s /= SizeFormatterUtil.oneGiB;
         suffix = "GiB";
      }
      else if (s > SizeFormatterUtil.oneMiB)
      {
         s /= SizeFormatterUtil.oneMiB;
         suffix = "MiB";
      }
      else if (s > SizeFormatterUtil.oneKiB)
      {
         s /= SizeFormatterUtil.oneKiB;
         suffix = "kiB";
      }
      return String.format("%.2f %s", s, suffix);
   }
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
