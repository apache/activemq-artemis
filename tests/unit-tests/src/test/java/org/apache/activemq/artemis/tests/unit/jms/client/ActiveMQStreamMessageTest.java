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
package org.apache.activemq.artemis.tests.unit.jms.client;

import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import java.util.ArrayList;

import org.apache.activemq.artemis.jms.client.ActiveMQStreamMessage;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class ActiveMQStreamMessageTest extends ActiveMQTestBase {
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testGetType() throws Exception {
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();
      Assert.assertEquals(ActiveMQStreamMessage.TYPE, message.getType());
   }

   @Test
   public void testReadBooleanFromBoolean() throws Exception {
      boolean value = RandomUtil.randomBoolean();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeBoolean(value);
      message.reset();

      Assert.assertEquals(value, message.readBoolean());
   }

   @Test
   public void testReadBooleanFromString() throws Exception {
      boolean value = RandomUtil.randomBoolean();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeString(Boolean.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readBoolean());
   }

   @Test
   public void testReadBooleanFromInvalidType() throws Exception {
      doReadTypeFromInvalidType(RandomUtil.randomFloat(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readBoolean();
         }
      });
   }

   @Test
   public void testReadBooleanFromEmptyMessage() throws Exception {
      doReadTypeFromEmptyMessage(new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readBoolean();
         }
      });
   }

   @Test
   public void testReadCharFromInvalidType() throws Exception {
      doReadTypeFromInvalidType(RandomUtil.randomFloat(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readChar();
         }
      });
   }

   @Test
   public void testReadCharFromEmptyMessage() throws Exception {
      doReadTypeFromEmptyMessage(new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readChar();
         }
      });
   }

   @Test
   public void testReadByteFromByte() throws Exception {
      byte value = RandomUtil.randomByte();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeByte(value);
      message.reset();

      Assert.assertEquals(value, message.readByte());
   }

   @Test
   public void testReadByteFromString() throws Exception {
      byte value = RandomUtil.randomByte();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeString(Byte.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readByte());
   }

   @Test
   public void testReadByteFromInvalidType() throws Exception {
      doReadTypeFromInvalidType(RandomUtil.randomFloat(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readByte();
         }
      });
   }

   @Test
   public void testReadByteFromEmptyMessage() throws Exception {
      doReadTypeFromEmptyMessage(new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readByte();
         }
      });
   }

   @Test
   public void testReadBytesFromBytes() throws Exception {
      byte[] value = RandomUtil.randomBytes();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeBytes(value);
      message.reset();

      byte[] v = new byte[value.length];
      message.readBytes(v);

      ActiveMQTestBase.assertEqualsByteArrays(value, v);
   }

   @Test
   public void testReadBytesFromBytes_2() throws Exception {
      byte[] value = RandomUtil.randomBytes(512);
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeBytes(value, 0, 256);
      message.reset();

      byte[] v = new byte[256];
      message.readBytes(v);

      ActiveMQTestBase.assertEqualsByteArrays(256, value, v);
   }

   @Test
   public void testReadBytesFromInvalidType() throws Exception {
      doReadTypeFromInvalidType(RandomUtil.randomBoolean(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readByte();
         }
      });
   }

   @Test
   public void testReadBytesFromEmptyMessage() throws Exception {
      doReadTypeFromEmptyMessage(new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            byte[] bytes = new byte[1];
            return message.readBytes(bytes);
         }
      });
   }

   @Test
   public void testReadShortFromByte() throws Exception {
      byte value = RandomUtil.randomByte();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeByte(value);
      message.reset();

      Assert.assertEquals(value, message.readShort());
   }

   @Test
   public void testReadShortFromShort() throws Exception {
      short value = RandomUtil.randomShort();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeShort(value);
      message.reset();

      Assert.assertEquals(value, message.readShort());
   }

   @Test
   public void testReadShortFromString() throws Exception {
      short value = RandomUtil.randomShort();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeString(Short.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readShort());
   }

   @Test
   public void testReadShortFromInvalidType() throws Exception {
      doReadTypeFromInvalidType(RandomUtil.randomFloat(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readShort();
         }
      });
   }

   @Test
   public void testReadShortFromEmptyMessage() throws Exception {
      doReadTypeFromEmptyMessage(new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readShort();
         }
      });
   }

   @Test
   public void testReadIntFromByte() throws Exception {
      byte value = RandomUtil.randomByte();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeByte(value);
      message.reset();

      Assert.assertEquals(value, message.readInt());
   }

   @Test
   public void testReadIntFromShort() throws Exception {
      short value = RandomUtil.randomShort();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeShort(value);
      message.reset();

      Assert.assertEquals(value, message.readInt());
   }

   @Test
   public void testReadIntFromInt() throws Exception {
      int value = RandomUtil.randomInt();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeInt(value);
      message.reset();

      Assert.assertEquals(value, message.readInt());
   }

   @Test
   public void testReadIntFromString() throws Exception {
      int value = RandomUtil.randomInt();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeString(Integer.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readInt());
   }

   @Test
   public void testReadIntFromInvalidType() throws Exception {
      doReadTypeFromInvalidType(RandomUtil.randomFloat(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readInt();
         }
      });
   }

   @Test
   public void testReadIntFromEmptyMessage() throws Exception {
      doReadTypeFromEmptyMessage(new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readInt();
         }
      });
   }

   @Test
   public void testReadCharFromChar() throws Exception {
      char value = RandomUtil.randomChar();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeChar(value);
      message.reset();

      Assert.assertEquals(value, message.readChar());
   }

   @Test
   public void testReadCharFromNull() throws Exception {
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeString(null);
      message.reset();

      try {
         message.readChar();
         fail();
      } catch (NullPointerException e) {
      }
   }

   @Test
   public void testReadLongFromByte() throws Exception {
      byte value = RandomUtil.randomByte();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeByte(value);
      message.reset();

      Assert.assertEquals(value, message.readLong());
   }

   @Test
   public void testReadLongFromShort() throws Exception {
      short value = RandomUtil.randomShort();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeShort(value);
      message.reset();

      Assert.assertEquals(value, message.readLong());
   }

   @Test
   public void testReadLongFromInt() throws Exception {
      int value = RandomUtil.randomInt();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeInt(value);
      message.reset();

      Assert.assertEquals(value, message.readLong());
   }

   @Test
   public void testReadLongFromLong() throws Exception {
      long value = RandomUtil.randomLong();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeLong(value);
      message.reset();

      Assert.assertEquals(value, message.readLong());
   }

   @Test
   public void testReadLongFromString() throws Exception {
      long value = RandomUtil.randomLong();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeString(Long.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readLong());
   }

   @Test
   public void testReadLongFromInvalidType() throws Exception {
      doReadTypeFromInvalidType(RandomUtil.randomFloat(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readLong();
         }
      });
   }

   @Test
   public void testReadLongFromEmptyMessage() throws Exception {
      doReadTypeFromEmptyMessage(new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readLong();
         }
      });
   }

   @Test
   public void testReadFloatFromFloat() throws Exception {
      float value = RandomUtil.randomFloat();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeFloat(value);
      message.reset();

      Assert.assertEquals(value, message.readFloat(), 0.000001);
   }

   @Test
   public void testReadFloatFromString() throws Exception {
      float value = RandomUtil.randomFloat();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeString(Float.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readFloat(), 0.000001);
   }

   @Test
   public void testReadFloatFromInvalidType() throws Exception {
      doReadTypeFromInvalidType(RandomUtil.randomBoolean(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readFloat();
         }
      });
   }

   @Test
   public void testReadFloatFromEmptyMessage() throws Exception {
      doReadTypeFromEmptyMessage(new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readFloat();
         }
      });
   }

   @Test
   public void testReadDoubleFromFloat() throws Exception {
      float value = RandomUtil.randomFloat();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeFloat(value);
      message.reset();

      Assert.assertEquals(Float.valueOf(value).doubleValue(), message.readDouble(), 0.000001);
   }

   @Test
   public void testReadDoubleFromDouble() throws Exception {
      double value = RandomUtil.randomDouble();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeDouble(value);
      message.reset();

      Assert.assertEquals(value, message.readDouble(), 0.000001);
   }

   @Test
   public void testReadDoubleFromString() throws Exception {
      double value = RandomUtil.randomDouble();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeString(Double.toString(value));
      message.reset();

      Assert.assertEquals(value, message.readDouble(), 0.000001);
   }

   @Test
   public void testReadDoubleFromInvalidType() throws Exception {
      doReadTypeFromInvalidType(RandomUtil.randomBoolean(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readDouble();
         }
      });
   }

   @Test
   public void testReadDoubleFromEmptyMessage() throws Exception {
      doReadTypeFromEmptyMessage(new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readDouble();
         }
      });
   }

   @Test
   public void testReadStringFromBoolean() throws Exception {
      boolean value = RandomUtil.randomBoolean();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeBoolean(value);
      message.reset();

      Assert.assertEquals(Boolean.toString(value), message.readString());
   }

   @Test
   public void testReadStringFromChar() throws Exception {
      char value = RandomUtil.randomChar();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeChar(value);
      message.reset();

      Assert.assertEquals(Character.toString(value), message.readString());
   }

   @Test
   public void testReadStringFromByte() throws Exception {
      byte value = RandomUtil.randomByte();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeByte(value);
      message.reset();

      Assert.assertEquals(Byte.toString(value), message.readString());
   }

   @Test
   public void testString() throws Exception {
      String value = RandomUtil.randomString();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeString(value);
      message.reset();

      try {
         message.readByte();
         fail("must throw a NumberFormatException");
      } catch (NumberFormatException e) {
      }

      // we can read the String without resetting the message
      Assert.assertEquals(value, message.readString());
   }

   @Test
   public void testReadStringFromShort() throws Exception {
      short value = RandomUtil.randomShort();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeShort(value);
      message.reset();

      Assert.assertEquals(Short.toString(value), message.readString());
   }

   @Test
   public void testReadStringFromInt() throws Exception {
      int value = RandomUtil.randomInt();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeInt(value);
      message.reset();

      Assert.assertEquals(Integer.toString(value), message.readString());
   }

   @Test
   public void testReadStringFromLong() throws Exception {
      long value = RandomUtil.randomLong();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeLong(value);
      message.reset();

      Assert.assertEquals(Long.toString(value), message.readString());
   }

   @Test
   public void testReadStringFromFloat() throws Exception {
      float value = RandomUtil.randomFloat();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeFloat(value);
      message.reset();

      Assert.assertEquals(Float.toString(value), message.readString());
   }

   @Test
   public void testReadStringFromDouble() throws Exception {
      double value = RandomUtil.randomDouble();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeDouble(value);
      message.reset();

      Assert.assertEquals(Double.toString(value), message.readString());
   }

   @Test
   public void testReadStringFromString() throws Exception {
      String value = RandomUtil.randomString();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeString(value);
      message.reset();

      Assert.assertEquals(value, message.readString());
   }

   @Test
   public void testReadStringFromNullString() throws Exception {
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeString(null);
      message.reset();

      Assert.assertNull(message.readString());
   }

   @Test
   public void testReadStringFromEmptyMessage() throws Exception {
      doReadTypeFromEmptyMessage(new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readString();
         }
      });
   }

   @Test
   public void testWriteObjectWithBoolean() throws Exception {
      doWriteObjectWithType(RandomUtil.randomBoolean(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readBoolean();
         }
      });
   }

   @Test
   public void testWriteObjectWithChar() throws Exception {
      doWriteObjectWithType(RandomUtil.randomChar(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readChar();
         }
      });
   }

   @Test
   public void testWriteObjectWithByte() throws Exception {
      doWriteObjectWithType(RandomUtil.randomByte(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readByte();
         }
      });
   }

   @Test
   public void testWriteObjectWithBytes() throws Exception {
      final byte[] value = RandomUtil.randomBytes();
      doWriteObjectWithType(value, new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            byte[] bytes = new byte[value.length];
            message.readBytes(bytes);
            return bytes;
         }
      });
   }

   @Test
   public void testWriteObjectWithShort() throws Exception {
      doWriteObjectWithType(RandomUtil.randomShort(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readShort();
         }
      });
   }

   @Test
   public void testWriteObjectWithInt() throws Exception {
      doWriteObjectWithType(RandomUtil.randomInt(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readInt();
         }
      });
   }

   @Test
   public void testWriteObjectWithLong() throws Exception {
      doWriteObjectWithType(RandomUtil.randomLong(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readLong();
         }
      });
   }

   @Test
   public void testWriteObjectWithFloat() throws Exception {
      doWriteObjectWithType(RandomUtil.randomFloat(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readFloat();
         }
      });
   }

   @Test
   public void testWriteObjectWithDouble() throws Exception {
      doWriteObjectWithType(RandomUtil.randomDouble(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readDouble();
         }
      });
   }

   @Test
   public void testWriteObjectWithString() throws Exception {
      doWriteObjectWithType(RandomUtil.randomString(), new TypeReader() {
         @Override
         public Object readType(final ActiveMQStreamMessage message) throws Exception {
            return message.readString();
         }
      });
   }

   @Test
   public void testWriteObjectWithNull() throws Exception {
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeObject(null);
   }

   @Test
   public void testWriteObjectWithInvalidType() throws Exception {
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      try {
         message.writeObject(new ArrayList<String>());
         Assert.fail("MessageFormatException");
      } catch (MessageFormatException e) {
      }
   }

   @Test
   public void testReadObjectFromBoolean() throws Exception {
      boolean value = RandomUtil.randomBoolean();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();
      message.writeBoolean(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   @Test
   public void testReadObjectFromChar() throws Exception {
      char value = RandomUtil.randomChar();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();
      message.writeChar(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   @Test
   public void testReadObjectFromByte() throws Exception {
      byte value = RandomUtil.randomByte();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();
      message.writeByte(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   @Test
   public void testReadObjectFromBytes() throws Exception {
      byte[] value = RandomUtil.randomBytes();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();
      message.writeBytes(value);

      message.reset();

      byte[] v = (byte[]) message.readObject();
      ActiveMQTestBase.assertEqualsByteArrays(value, v);
   }

   @Test
   public void testReadObjectFromShort() throws Exception {
      short value = RandomUtil.randomShort();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();
      message.writeShort(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   @Test
   public void testReadObjectFromInt() throws Exception {
      int value = RandomUtil.randomInt();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();
      message.writeInt(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   @Test
   public void testReadObjectFromLong() throws Exception {
      long value = RandomUtil.randomLong();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();
      message.writeLong(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   @Test
   public void testReadObjectFromFloat() throws Exception {
      float value = RandomUtil.randomFloat();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();
      message.writeFloat(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   @Test
   public void testReadObjectFromDouble() throws Exception {
      double value = RandomUtil.randomDouble();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();
      message.writeDouble(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   @Test
   public void testReadObjectFromString() throws Exception {
      String value = RandomUtil.randomString();
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();
      message.writeString(value);

      message.reset();

      Assert.assertEquals(value, message.readObject());
   }

   // Private -------------------------------------------------------

   private void doReadTypeFromEmptyMessage(final TypeReader reader) throws Exception {
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();
      message.reset();

      try {
         reader.readType(message);
         Assert.fail("MessageEOFException");
      } catch (MessageEOFException e) {
      }
   }

   private void doReadTypeFromInvalidType(final Object invalidValue, final TypeReader reader) throws Exception {
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeObject(invalidValue);
      message.reset();

      try {
         reader.readType(message);
         Assert.fail("MessageFormatException");
      } catch (MessageFormatException e) {
      }
   }

   private void doWriteObjectWithType(final Object value, final TypeReader reader) throws Exception {
      ActiveMQStreamMessage message = new ActiveMQStreamMessage();

      message.writeObject(value);
      message.reset();

      Object v = reader.readType(message);
      if (value instanceof byte[]) {
         ActiveMQTestBase.assertEqualsByteArrays((byte[]) value, (byte[]) v);
      } else {
         Assert.assertEquals(value, v);
      }
   }

   // Inner classes -------------------------------------------------

   private interface TypeReader {

      Object readType(ActiveMQStreamMessage message) throws Exception;
   }
}
