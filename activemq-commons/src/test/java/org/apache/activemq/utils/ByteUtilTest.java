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

package org.apache.activemq.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class ByteUtilTest
{
   @Test
   public void testBytesToString()
   {
      byte[] byteArray = new byte[] {0, 1, 2, 3};

      testEquals("0001 0203", ByteUtil.bytesToHex(byteArray, 2));
      testEquals("00 01 02 03", ByteUtil.bytesToHex(byteArray, 1));
      testEquals("000102 03", ByteUtil.bytesToHex(byteArray, 3));
   }


   @Test
   public void testMaxString()
   {
      byte[] byteArray = new byte[20 * 1024];
      System.out.println(ByteUtil.maxString(ByteUtil.bytesToHex(byteArray, 2),150));
   }


   void testEquals(String string1, String string2)
   {
      if (!string1.equals(string2))
      {
         Assert.fail("String are not the same:=" + string1 + "!=" + string2);
      }
   }

}
