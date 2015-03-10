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
package org.apache.activemq.tests.unit.util;
import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.ActiveMQBuffers;
import org.junit.After;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import org.junit.Assert;

import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.UnitTestCase;
import org.apache.activemq.utils.DataConstants;
import org.apache.activemq.utils.Random;
import org.apache.activemq.utils.UTF8Util;

/**
 * A UTF8Test
 */
public class UTF8Test extends UnitTestCase
{

   @Test
   public void testValidateUTF() throws Exception
   {
      ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(60 * 1024);

      byte[] bytes = new byte[20000];

      Random random = new Random();
      random.getRandom().nextBytes(bytes);

      String str = new String(bytes);

      UTF8Util.saveUTF(buffer, str);

      String newStr = UTF8Util.readUTF(buffer);

      Assert.assertEquals(str, newStr);
   }

   @Test
   public void testValidateUTFOnDataInput() throws Exception
   {
      for (int i = 0; i < 100; i++)
      {
         Random random = new Random();

         // Random size between 15k and 20K
         byte[] bytes = new byte[15000 + RandomUtil.randomPositiveInt() % 5000];

         random.getRandom().nextBytes(bytes);

         String str = new String(bytes);

         // The maximum size the encoded UTF string would reach is str.length * 3 (look at the UTF8 implementation)
         testValidateUTFOnDataInputStream(str,
                                          ActiveMQBuffers.wrappedBuffer(ByteBuffer.allocate(str.length() * 3 +
                                                                                               DataConstants.SIZE_SHORT)));

         testValidateUTFOnDataInputStream(str, ActiveMQBuffers.dynamicBuffer(100));

         testValidateUTFOnDataInputStream(str, ActiveMQBuffers.fixedBuffer(100 * 1024));
      }
   }

   private void testValidateUTFOnDataInputStream(final String str, final ActiveMQBuffer wrap) throws Exception
   {
      UTF8Util.saveUTF(wrap, str);

      DataInputStream data = new DataInputStream(new ByteArrayInputStream(wrap.toByteBuffer().array()));

      String newStr = data.readUTF();

      Assert.assertEquals(str, newStr);

      ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
      DataOutputStream outData = new DataOutputStream(byteOut);

      outData.writeUTF(str);

      ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(byteOut.toByteArray());

      newStr = UTF8Util.readUTF(buffer);

      Assert.assertEquals(str, newStr);
   }

   @Test
   public void testBigSize() throws Exception
   {

      char[] chars = new char[0xffff + 1];

      for (int i = 0; i < chars.length; i++)
      {
         chars[i] = ' ';
      }

      String str = new String(chars);

      ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(0xffff + 4);

      try
      {
         UTF8Util.saveUTF(buffer, str);
         Assert.fail("String is too big, supposed to throw an exception");
      }
      catch (Exception ignored)
      {
      }

      Assert.assertEquals("A buffer was supposed to be untouched since the string was too big", 0, buffer.writerIndex());

      chars = new char[25000];

      for (int i = 0; i < chars.length; i++)
      {
         chars[i] = 0x810;
      }

      str = new String(chars);

      try
      {
         UTF8Util.saveUTF(buffer, str);
         Assert.fail("Encoded String is too big, supposed to throw an exception");
      }
      catch (Exception ignored)
      {
      }

      Assert.assertEquals("A buffer was supposed to be untouched since the string was too big", 0, buffer.writerIndex());

      // Testing a string right on the limit
      chars = new char[0xffff];

      for (int i = 0; i < chars.length; i++)
      {
         chars[i] = (char)(i % 100 + 1);
      }

      str = new String(chars);

      UTF8Util.saveUTF(buffer, str);

      Assert.assertEquals(0xffff + DataConstants.SIZE_SHORT, buffer.writerIndex());

      String newStr = UTF8Util.readUTF(buffer);

      Assert.assertEquals(str, newStr);

   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      UTF8Util.clearBuffer();
      super.tearDown();
   }
}
