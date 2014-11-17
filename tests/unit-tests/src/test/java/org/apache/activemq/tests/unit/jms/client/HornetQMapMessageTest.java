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
package org.apache.activemq6.tests.unit.jms.client;
import javax.jms.MessageFormatException;

import org.apache.activemq6.jms.client.HornetQMapMessage;
import org.apache.activemq6.tests.util.RandomUtil;
import org.apache.activemq6.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class HornetQMapMessageTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String itemName;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      itemName = RandomUtil.randomString();
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testClearBody() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setBoolean(itemName, true);

      Assert.assertTrue(message.itemExists(itemName));

      message.clearBody();

      Assert.assertFalse(message.itemExists(itemName));
   }

   @Test
   public void testGetType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      Assert.assertEquals(HornetQMapMessage.TYPE, message.getType());
   }

   @Test
   public void testCheckItemNameIsNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      try
      {
         message.setBoolean(null, true);
         Assert.fail("item name can not be null");
      }
      catch (IllegalArgumentException e)
      {
      }

   }

   @Test
   public void testCheckItemNameIsEmpty() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      try
      {
         message.setBoolean("", true);
         Assert.fail("item name can not be empty");
      }
      catch (IllegalArgumentException e)
      {
      }

   }

   @Test
   public void testGetBooleanFromBoolean() throws Exception
   {
      boolean value = true;

      HornetQMapMessage message = new HornetQMapMessage();
      message.setBoolean(itemName, value);

      Assert.assertEquals(value, message.getBoolean(itemName));
   }

   @Test
   public void testGetBooleanFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      Assert.assertEquals(false, message.getBoolean(itemName));
   }

   @Test
   public void testGetBooleanFromString() throws Exception
   {
      boolean value = true;

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Boolean.toString(value));

      Assert.assertEquals(value, message.getBoolean(itemName));
   }

   @Test
   public void testGetBooleanFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, RandomUtil.randomFloat());

      try
      {
         message.getBoolean(itemName);
         Assert.fail("MessageFormatException");
      }
      catch (MessageFormatException e)
      {
      }
   }

   @Test
   public void testGetByteFromByte() throws Exception
   {
      byte value = RandomUtil.randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setByte(itemName, value);

      Assert.assertEquals(value, message.getByte(itemName));
   }

   @Test
   public void testGetByteFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getByte(itemName);
         Assert.fail("NumberFormatException");
      }
      catch (NumberFormatException e)
      {
      }
   }

   @Test
   public void testGetByteFromString() throws Exception
   {
      byte value = RandomUtil.randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Byte.toString(value));

      Assert.assertEquals(value, message.getByte(itemName));
   }

   @Test
   public void testGetByteFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, RandomUtil.randomFloat());

      try
      {
         message.getByte(itemName);
         Assert.fail("MessageFormatException");
      }
      catch (MessageFormatException e)
      {
      }
   }

   @Test
   public void testGetShortFromByte() throws Exception
   {
      byte value = RandomUtil.randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setByte(itemName, value);

      Assert.assertEquals(value, message.getShort(itemName));
   }

   @Test
   public void testGetShortFromShort() throws Exception
   {
      short value = RandomUtil.randomShort();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setShort(itemName, value);

      Assert.assertEquals(value, message.getShort(itemName));
   }

   @Test
   public void testGetShortFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getShort(itemName);
         Assert.fail("NumberFormatException");
      }
      catch (NumberFormatException e)
      {
      }
   }

   @Test
   public void testGetShortFromString() throws Exception
   {
      short value = RandomUtil.randomShort();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Short.toString(value));

      Assert.assertEquals(value, message.getShort(itemName));
   }

   @Test
   public void testGetShortFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, RandomUtil.randomFloat());

      try
      {
         message.getShort(itemName);
         Assert.fail("MessageFormatException");
      }
      catch (MessageFormatException e)
      {
      }
   }

   @Test
   public void testGetIntFromByte() throws Exception
   {
      byte value = RandomUtil.randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setByte(itemName, value);

      Assert.assertEquals(value, message.getInt(itemName));
   }

   @Test
   public void testGetIntFromShort() throws Exception
   {
      short value = RandomUtil.randomShort();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setShort(itemName, value);

      Assert.assertEquals(value, message.getInt(itemName));
   }

   @Test
   public void testGetIntFromInt() throws Exception
   {
      int value = RandomUtil.randomInt();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setInt(itemName, value);

      Assert.assertEquals(value, message.getInt(itemName));
   }

   @Test
   public void testGetIntFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getInt(itemName);
         Assert.fail("NumberFormatException");
      }
      catch (NumberFormatException e)
      {
      }
   }

   @Test
   public void testGetIntFromString() throws Exception
   {
      int value = RandomUtil.randomInt();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Integer.toString(value));

      Assert.assertEquals(value, message.getInt(itemName));
   }

   @Test
   public void testGetIntFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, RandomUtil.randomFloat());

      try
      {
         message.getInt(itemName);
         Assert.fail("MessageFormatException");
      }
      catch (MessageFormatException e)
      {
      }
   }

   @Test
   public void testGetCharFromChar() throws Exception
   {
      char value = RandomUtil.randomChar();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setChar(itemName, value);

      Assert.assertEquals(value, message.getChar(itemName));
   }

   @Test
   public void testGetCharFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getChar(itemName);
         Assert.fail("NullPointerException");
      }
      catch (NullPointerException e)
      {
      }
   }

   @Test
   public void testGetCharFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, RandomUtil.randomFloat());

      try
      {
         message.getChar(itemName);
         Assert.fail("MessageFormatException");
      }
      catch (MessageFormatException e)
      {
      }
   }

   @Test
   public void testGetLongFromByte() throws Exception
   {
      byte value = RandomUtil.randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setByte(itemName, value);

      Assert.assertEquals(value, message.getLong(itemName));
   }

   @Test
   public void testGetLongFromShort() throws Exception
   {
      short value = RandomUtil.randomShort();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setShort(itemName, value);

      Assert.assertEquals(value, message.getLong(itemName));
   }

   @Test
   public void testGetLongFromInt() throws Exception
   {
      int value = RandomUtil.randomInt();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setInt(itemName, value);

      Assert.assertEquals(value, message.getLong(itemName));
   }

   @Test
   public void testGetLongFromLong() throws Exception
   {
      long value = RandomUtil.randomLong();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setLong(itemName, value);

      Assert.assertEquals(value, message.getLong(itemName));
   }

   @Test
   public void testGetLongFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getLong(itemName);
         Assert.fail("NumberFormatException");
      }
      catch (NumberFormatException e)
      {
      }
   }

   @Test
   public void testGetLongFromString() throws Exception
   {
      long value = RandomUtil.randomLong();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Long.toString(value));

      Assert.assertEquals(value, message.getLong(itemName));
   }

   @Test
   public void testGetLongFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, RandomUtil.randomFloat());

      try
      {
         message.getLong(itemName);
         Assert.fail("MessageFormatException");
      }
      catch (MessageFormatException e)
      {
      }
   }

   @Test
   public void testGetFloatFromFloat() throws Exception
   {
      float value = RandomUtil.randomFloat();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, value);

      Assert.assertEquals(value, message.getFloat(itemName), 0.000001);
   }

   @Test
   public void testGetFloatFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getFloat(itemName);
         Assert.fail("NullPointerException");
      }
      catch (NullPointerException e)
      {
      }
   }

   @Test
   public void testGetFloatFromString() throws Exception
   {
      float value = RandomUtil.randomFloat();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Float.toString(value));

      Assert.assertEquals(value, message.getFloat(itemName), 0.000001);
   }

   @Test
   public void testGetFloatFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setChar(itemName, RandomUtil.randomChar());

      try
      {
         message.getFloat(itemName);
         Assert.fail("MessageFormatException");
      }
      catch (MessageFormatException e)
      {
      }
   }

   @Test
   public void testGetDoubleFromFloat() throws Exception
   {
      float value = RandomUtil.randomFloat();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, value);

      Assert.assertEquals(Float.valueOf(value).doubleValue(), message.getDouble(itemName), 0.000001);
   }

   @Test
   public void testGetDoubleFromDouble() throws Exception
   {
      double value = RandomUtil.randomDouble();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setDouble(itemName, value);

      Assert.assertEquals(value, message.getDouble(itemName), 0.000001);
   }

   @Test
   public void testGetDoubleFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      try
      {
         message.getDouble(itemName);
         Assert.fail("NullPointerException");
      }
      catch (NullPointerException e)
      {
      }
   }

   @Test
   public void testGetDoubleFromString() throws Exception
   {
      double value = RandomUtil.randomDouble();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, Double.toString(value));

      Assert.assertEquals(value, message.getDouble(itemName), 0.000001);
   }

   @Test
   public void testGetDoubleFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setChar(itemName, RandomUtil.randomChar());

      try
      {
         message.getDouble(itemName);
         Assert.fail("MessageFormatException");
      }
      catch (MessageFormatException e)
      {
      }
   }

   @Test
   public void testGetStringFromBoolean() throws Exception
   {
      boolean value = RandomUtil.randomBoolean();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setBoolean(itemName, value);

      Assert.assertEquals(Boolean.toString(value), message.getString(itemName));
   }

   @Test
   public void testGetStringFromByte() throws Exception
   {
      byte value = RandomUtil.randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setByte(itemName, value);

      Assert.assertEquals(Byte.toString(value), message.getString(itemName));
   }

   @Test
   public void testGetStringFromChar() throws Exception
   {
      char value = RandomUtil.randomChar();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setChar(itemName, value);

      Assert.assertEquals(Character.toString(value), message.getString(itemName));
   }

   @Test
   public void testGetStringFromShort() throws Exception
   {
      short value = RandomUtil.randomShort();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setShort(itemName, value);

      Assert.assertEquals(Short.toString(value), message.getString(itemName));
   }

   @Test
   public void testGetStringFromInt() throws Exception
   {
      int value = RandomUtil.randomInt();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setInt(itemName, value);

      Assert.assertEquals(Integer.toString(value), message.getString(itemName));
   }

   @Test
   public void testGetStringFromLong() throws Exception
   {
      long value = RandomUtil.randomLong();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setLong(itemName, value);

      Assert.assertEquals(Long.toString(value), message.getString(itemName));
   }

   @Test
   public void testGetStringFromFloat() throws Exception
   {
      float value = RandomUtil.randomFloat();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setFloat(itemName, value);

      Assert.assertEquals(Float.toString(value), message.getString(itemName));
   }

   @Test
   public void testGetStringFromDouble() throws Exception
   {
      double value = RandomUtil.randomByte();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setDouble(itemName, value);

      Assert.assertEquals(Double.toString(value), message.getString(itemName));
   }

   @Test
   public void testGetStringFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      Assert.assertNull(message.getString(itemName));
   }

   @Test
   public void testGetStringFromString() throws Exception
   {
      String value = RandomUtil.randomString();

      HornetQMapMessage message = new HornetQMapMessage();
      message.setString(itemName, value);

      Assert.assertEquals(value, message.getString(itemName));
   }

   @Test
   public void testGetBytesFromBytes() throws Exception
   {
      byte[] value = RandomUtil.randomBytes();
      HornetQMapMessage message = new HornetQMapMessage();
      message.setBytes(itemName, value);

      UnitTestCase.assertEqualsByteArrays(value, message.getBytes(itemName));
   }

   @Test
   public void testGetBytesFromNull() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();

      Assert.assertNull(message.getBytes(itemName));
   }

   @Test
   public void testGetBytesFromInvalidType() throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setChar(itemName, RandomUtil.randomChar());

      try
      {
         message.getBytes(itemName);
         Assert.fail("MessageFormatException");
      }
      catch (MessageFormatException e)
      {
      }
   }

   @Test
   public void testSetObjectFromBoolean() throws Exception
   {
      boolean value = RandomUtil.randomBoolean();
      HornetQMapMessage message = new HornetQMapMessage();
      message.setObject(itemName, value);

      Assert.assertEquals(value, message.getObject(itemName));
   }

   @Test
   public void testSetObjectFromByte() throws Exception
   {
      doTestSetObject(RandomUtil.randomByte());
   }

   @Test
   public void testSetObjectFromShort() throws Exception
   {
      doTestSetObject(RandomUtil.randomShort());
   }

   @Test
   public void testSetObjectFromChar() throws Exception
   {
      doTestSetObject(RandomUtil.randomChar());
   }

   @Test
   public void testSetObjectFromInt() throws Exception
   {
      doTestSetObject(RandomUtil.randomInt());
   }

   @Test
   public void testSetObjectFromLong() throws Exception
   {
      doTestSetObject(RandomUtil.randomLong());
   }

   @Test
   public void testSetObjectFromFloat() throws Exception
   {
      doTestSetObject(RandomUtil.randomFloat());
   }

   @Test
   public void testSetObjectFromDouble() throws Exception
   {
      doTestSetObject(RandomUtil.randomDouble());
   }

   @Test
   public void testSetObjectFromString() throws Exception
   {
      doTestSetObject(RandomUtil.randomString());
   }

   @Test
   public void testSetObjectFromBytes() throws Exception
   {
      doTestSetObject(RandomUtil.randomBytes());
   }

   private void doTestSetObject(final Object value) throws Exception
   {
      HornetQMapMessage message = new HornetQMapMessage();
      message.setObject(itemName, value);

      Assert.assertEquals(value, message.getObject(itemName));
   }
}
