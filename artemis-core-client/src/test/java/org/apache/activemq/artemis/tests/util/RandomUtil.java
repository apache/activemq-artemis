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
package org.apache.activemq.artemis.tests.util;

import javax.transaction.xa.Xid;
import java.util.Random;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;

public final class RandomUtil
{
   // Constants -----------------------------------------------------

   private static final Random random = new Random(System.currentTimeMillis());

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static String randomString()
   {
      return UUID.randomUUID().toString();
   }

   public static SimpleString randomSimpleString()
   {
      return new SimpleString(RandomUtil.randomString());
   }

   public static char randomChar()
   {
      return RandomUtil.randomString().charAt(0);
   }

   public static long randomLong()
   {
      return RandomUtil.random.nextLong();
   }

   public static long randomPositiveLong()
   {
      return Math.abs(RandomUtil.randomLong());
   }

   public static int randomInt()
   {
      return RandomUtil.random.nextInt();
   }

   public static int randomPositiveInt()
   {
      return Math.abs(RandomUtil.randomInt());
   }


   public static ActiveMQBuffer randomBuffer(final int size, final long... data)
   {
      ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(size + 8 * data.length);

      for (long d : data)
      {
         buffer.writeLong(d);
      }

      for (int i = 0; i < size; i++)
      {
         buffer.writeByte(randomByte());
      }

      return buffer;
   }


   public static int randomInterval(final int min, final int max)
   {
      return min + randomMax(max - min);
   }

   public static int randomMax(final int max)
   {
      int value = randomPositiveInt() % max;

      if (value == 0)
      {
         value = max;
      }

      return value;
   }

   public static int randomPort()
   {
      return RandomUtil.random.nextInt(65536);
   }

   public static short randomShort()
   {
      return (short) RandomUtil.random.nextInt(Short.MAX_VALUE);
   }

   public static byte randomByte()
   {
      return Integer.valueOf(RandomUtil.random.nextInt()).byteValue();
   }

   public static boolean randomBoolean()
   {
      return RandomUtil.random.nextBoolean();
   }

   public static byte[] randomBytes()
   {
      return RandomUtil.randomString().getBytes();
   }

   public static byte[] randomBytes(final int length)
   {
      byte[] bytes = new byte[length];
      for (int i = 0; i < bytes.length; i++)
      {
         bytes[i] = RandomUtil.randomByte();
      }
      return bytes;
   }

   public static double randomDouble()
   {
      return RandomUtil.random.nextDouble();
   }

   public static float randomFloat()
   {
      return RandomUtil.random.nextFloat();
   }

   public static Xid randomXid()
   {
      return new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes());
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
