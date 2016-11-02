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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DefaultSensitiveStringCodecTest {

   @Test
   public void testDefaultAlgorithm() throws Exception {
      SensitiveDataCodec<String> codec = PasswordMaskingUtil.getDefaultCodec();
      assertTrue(codec instanceof DefaultSensitiveStringCodec);
   }

   @Test
   public void testOnewayAlgorithm() throws Exception {
      DefaultSensitiveStringCodec codec = new DefaultSensitiveStringCodec();
      Map<String, String> params = new HashMap<>();
      params.put(DefaultSensitiveStringCodec.ALGORITHM, DefaultSensitiveStringCodec.ONE_WAY);
      codec.init(params);

      String plainText = "some_password";
      String maskedText = codec.encode(plainText);
      System.out.println("encoded value: " + maskedText);

      //one way can't decode
      try {
         codec.decode(maskedText);
         fail("one way algorithm can't decode");
      } catch (IllegalArgumentException expected) {
      }

      assertTrue(codec.verify(plainText.toCharArray(), maskedText));

      String otherPassword = "some_other_password";
      assertFalse(codec.verify(otherPassword.toCharArray(), maskedText));
   }

   @Test
   public void testTwowayAlgorithm() throws Exception {
      DefaultSensitiveStringCodec codec = new DefaultSensitiveStringCodec();
      Map<String, String> params = new HashMap<>();
      params.put(DefaultSensitiveStringCodec.ALGORITHM, DefaultSensitiveStringCodec.TWO_WAY);
      codec.init(params);

      String plainText = "some_password";
      String maskedText = codec.encode(plainText);
      System.out.println("encoded value: " + maskedText);

      String decoded = codec.decode(maskedText);
      System.out.println("encoded value: " + maskedText);

      assertEquals("decoded result not match: " + decoded, decoded, plainText);
   }
}
