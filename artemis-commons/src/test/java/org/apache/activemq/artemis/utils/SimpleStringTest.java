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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleStringTest {

   @Test
   public void testOutOfBoundsThrownOnMalformedString() {
      ByteBuf byteBuffer = ByteBufAllocator.DEFAULT.buffer(5);
      byteBuffer.writeInt(Integer.MAX_VALUE);

      Exception e = null;
      try {
         SimpleString.readSimpleString(byteBuffer);
      } catch (IndexOutOfBoundsException iob) {
         e = iob;
      }
      assertInstanceOf(IndexOutOfBoundsException.class, e);
   }

   @Test
   public void testBlank() {
      for (int i = 0; i <= 10; i++) {
         assertTrue(SimpleString.of(" ".repeat(i)).isBlank());
      }
      for (int i = 0; i <= 10; i++) {
         assertTrue(SimpleString.of("\t".repeat(i)).isBlank());
      }
      for (int i = 0; i <= 10; i++) {
         assertTrue(SimpleString.of("\n".repeat(i)).isBlank());
      }
      for (int i = 0; i <= 10; i++) {
         assertTrue(SimpleString.of("\r".repeat(i)).isBlank());
      }
      for (int i = 1; i <= 10; i++) {
         assertFalse(SimpleString.of("x".repeat(i)).isBlank());
      }
   }
}
