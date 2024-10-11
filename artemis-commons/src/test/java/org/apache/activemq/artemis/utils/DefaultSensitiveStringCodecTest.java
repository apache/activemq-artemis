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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DefaultSensitiveStringCodecTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   public void testDefaultCodec() {
      SensitiveDataCodec<String> codec = PasswordMaskingUtil.getDefaultCodec();
      assertTrue(codec instanceof DefaultSensitiveStringCodec);
   }

   @Test
   public void testDefaultAlgorithm() throws Exception {
      testAlgorithm(Collections.emptyMap());

      testAlgorithm(Map.of(DefaultSensitiveStringCodec.KEY_PARAM, "my-key"));

      System.setProperty(DefaultSensitiveStringCodec.KEY_SYS_PROP, "my-key");
      try {
         testAlgorithm(Collections.emptyMap());
      } finally {
         System.clearProperty(DefaultSensitiveStringCodec.KEY_SYS_PROP);
      }
   }

   @Test
   public void testOnewayAlgorithm() throws Exception {
      testAlgorithm(Map.of(DefaultSensitiveStringCodec.ALGORITHM_PARAM, DefaultSensitiveStringCodec.ALGORITHM_ONE_WAY));
   }

   @Test
   public void testTwowayAlgorithm() throws Exception {
      testAlgorithm(Map.of(DefaultSensitiveStringCodec.ALGORITHM_PARAM,
         DefaultSensitiveStringCodec.ALGORITHM_TWO_WAY));
      testAlgorithm(Map.of(DefaultSensitiveStringCodec.ALGORITHM_PARAM, DefaultSensitiveStringCodec.ALGORITHM_TWO_WAY,
         DefaultSensitiveStringCodec.KEY_PARAM, "my-key"));
   }

   @Test
   public void testTwoway1Algorithm() throws Exception {
      try {
         testAlgorithm(Map.of(DefaultSensitiveStringCodec.ALGORITHM_PARAM,
            DefaultSensitiveStringCodec.ALGORITHM_TWO_WAY_1));
         fail();
      } catch (Exception e) {
         assertEquals(IllegalArgumentException.class, e.getClass());
      }

      testAlgorithm(Map.of(DefaultSensitiveStringCodec.ALGORITHM_PARAM, DefaultSensitiveStringCodec.ALGORITHM_TWO_WAY_1,
         DefaultSensitiveStringCodec.KEY_PARAM, "my-key"));

      System.setProperty(DefaultSensitiveStringCodec.KEY_SYS_PROP, "my-key");
      try {
         testAlgorithm(Map.of(DefaultSensitiveStringCodec.ALGORITHM_PARAM, DefaultSensitiveStringCodec.ALGORITHM_TWO_WAY_1,
            DefaultSensitiveStringCodec.KEY_PARAM, "my-key"));
      } finally {
         System.clearProperty(DefaultSensitiveStringCodec.KEY_SYS_PROP);
      }
   }

   private void testAlgorithm(Map<String, String> params) throws Exception {
      DefaultSensitiveStringCodec codec = getDefaultSensitiveStringCodec(params);

      String plainText = "some_password";
      String maskedText = codec.encode(plainText);
      logger.debug("encoded value: {}", maskedText);

      if (DefaultSensitiveStringCodec.ALGORITHM_ONE_WAY.equals(params.get(DefaultSensitiveStringCodec.ALGORITHM_PARAM))) {
         //one way can't decode
         try {
            codec.decode(maskedText);
            fail("one way algorithm can't decode");
         } catch (IllegalArgumentException expected) {
         }
      } else {
         String decoded = codec.decode(maskedText);
         logger.debug("encoded value: {}", maskedText);

         assertEquals(decoded, plainText, "decoded result not match: " + decoded);
      }

      assertTrue(codec.verify(plainText.toCharArray(), maskedText));

      String otherPassword = "some_other_password";
      assertFalse(codec.verify(otherPassword.toCharArray(), maskedText));
   }

   @Test
   public void testInitFromEnvVar() throws Exception {
      final String someString = "bla";
      DefaultSensitiveStringCodec codecFromEnvVarConfig = new DefaultSensitiveStringCodec() {
         @Override
         public String getFromEnv(String v) {
            if (DefaultSensitiveStringCodec.KEY_ENV_VAR.equals(v)) {
               return someString;
            }
            return null;
         }
      };
      Map<String, String> params = new HashMap<>();
      codecFromEnvVarConfig.init(params);
      String blaVersion = codecFromEnvVarConfig.encode(someString);
      Map<String, String> twowayParams = Map.of(DefaultSensitiveStringCodec.ALGORITHM_PARAM, DefaultSensitiveStringCodec.ALGORITHM_ONE_WAY);
      assertNotEquals(blaVersion,  getDefaultSensitiveStringCodec(twowayParams).encode(someString));
   }

   @Test
   public void testCompareWithOnewayAlgorithm() throws Exception {
      testCompareWithAlgorithm(Map.of(DefaultSensitiveStringCodec.ALGORITHM_PARAM, DefaultSensitiveStringCodec.ALGORITHM_ONE_WAY));
   }

   @Test
   public void testCompareWithTwowayAlgorithm() throws Exception {
      testCompareWithAlgorithm(Map.of(DefaultSensitiveStringCodec.ALGORITHM_PARAM, DefaultSensitiveStringCodec.ALGORITHM_TWO_WAY));
   }

   @Test
   public void testCompareWithTwoway1Algorithm() throws Exception {
      testCompareWithAlgorithm(Map.of(DefaultSensitiveStringCodec.ALGORITHM_PARAM, DefaultSensitiveStringCodec.ALGORITHM_TWO_WAY_1,
         DefaultSensitiveStringCodec.KEY_PARAM, "my-key"));
   }

   private void testCompareWithAlgorithm(Map<String, String> params) throws Exception {
      DefaultSensitiveStringCodec codec = getDefaultSensitiveStringCodec(params);

      String plainText = "some_password";
      String maskedText = codec.encode(plainText);
      logger.debug("encoded value: {}", maskedText);

      assertTrue(codec.verify(plainText.toCharArray(), maskedText));

      String otherPassword = "some_other_password";
      assertFalse(codec.verify(otherPassword.toCharArray(), maskedText));
   }

   private DefaultSensitiveStringCodec getDefaultSensitiveStringCodec(Map<String, String> params) throws Exception {
      DefaultSensitiveStringCodec codec = new DefaultSensitiveStringCodec();
      codec.init(params);
      return codec;
   }
}
