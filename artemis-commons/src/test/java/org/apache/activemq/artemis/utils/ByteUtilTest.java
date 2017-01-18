/*
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
package org.apache.activemq.artemis.utils;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ByteUtilTest {

   @Test
   public void testBytesToString() {
      byte[] byteArray = new byte[]{0, 1, 2, 3};

      testEquals("0001 0203", ByteUtil.bytesToHex(byteArray, 2));
      testEquals("00 01 02 03", ByteUtil.bytesToHex(byteArray, 1));
      testEquals("000102 03", ByteUtil.bytesToHex(byteArray, 3));
   }

   @Test
   public void testNonASCII() {
      assertEquals("aA", ByteUtil.toSimpleString(new byte[]{97, 0, 65, 0}));
      assertEquals(ByteUtil.NON_ASCII_STRING, ByteUtil.toSimpleString(new byte[]{0, 97, 0, 65}));

      System.out.println(ByteUtil.toSimpleString(new byte[]{0, 97, 0, 65}));
   }

   @Test
   public void testMaxString() {
      byte[] byteArray = new byte[20 * 1024];
      System.out.println(ByteUtil.maxString(ByteUtil.bytesToHex(byteArray, 2), 150));
   }

   void testEquals(String string1, String string2) {
      if (!string1.equals(string2)) {
         Assert.fail("String are not the same:=" + string1 + "!=" + string2);
      }
   }

   @Test
   public void testTextBytesToLongBytes() {
      long[] factor = new long[] {1, 5, 10};
      String[] type = new String[]{"", "b", "k", "m", "g"};
      long[] size = new long[]{1, 1, 1024, 1024 * 1024, 1024 * 1024 * 1024};

      for (int i = 0; i < 3; i++) {
         for (int j = 0; j < type.length; j++) {
            assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + type[j]));
            assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + " " + type[j]));
            assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + type[j].toUpperCase()));
            assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + " " + type[j].toUpperCase()));
            if (j >= 2) {
               assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + type[j] + "b"));
               assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + " " + type[j] + "b"));
               assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + type[j].toUpperCase() + "B"));
               assertEquals(factor[i] * size[j], ByteUtil.convertTextBytes(factor[i] + " " + type[j].toUpperCase() + "B"));
            }
         }
      }
   }

   @Test
   public void testTextBytesToLongBytesNegative() {
      try {
         ByteUtil.convertTextBytes("x");
         fail();
      } catch (Exception e) {
         assertTrue(e instanceof IllegalArgumentException);
      }
   }
}
