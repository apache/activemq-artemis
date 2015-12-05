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

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

/**
 * A DefaultSensitiveDataCodec
 *
 * The default implementation of SensitiveDataCodec.
 * This class is used when the user indicates in the config
 * file to use a masked password but doesn't give a
 * codec implementation.
 *
 * The decode() and encode() method is copied originally from
 * JBoss AS code base.
 */
public class DefaultSensitiveStringCodec implements SensitiveDataCodec<String> {

   private byte[] internalKey = "clusterpassword".getBytes();

   @Override
   public String decode(Object secret) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
      SecretKeySpec key = new SecretKeySpec(internalKey, "Blowfish");

      BigInteger n = new BigInteger((String) secret, 16);
      byte[] encoding = n.toByteArray();

      // JBAS-3457: fix leading zeros
      if (encoding.length % 8 != 0) {
         int length = encoding.length;
         int newLength = ((length / 8) + 1) * 8;
         int pad = newLength - length; // number of leading zeros
         byte[] old = encoding;
         encoding = new byte[newLength];
         for (int i = old.length - 1; i >= 0; i--) {
            encoding[i + pad] = old[i];
         }
      }

      Cipher cipher = Cipher.getInstance("Blowfish");
      cipher.init(Cipher.DECRYPT_MODE, key);
      byte[] decode = cipher.doFinal(encoding);

      return new String(decode);
   }

   public Object encode(String secret) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
      SecretKeySpec key = new SecretKeySpec(internalKey, "Blowfish");

      Cipher cipher = Cipher.getInstance("Blowfish");
      cipher.init(Cipher.ENCRYPT_MODE, key);
      byte[] encoding = cipher.doFinal(secret.getBytes());
      BigInteger n = new BigInteger(encoding);
      return n.toString(16);
   }

   @Override
   public void init(Map<String, String> params) {
      String key = params.get("key");
      if (key != null) {
         updateKey(key);
      }
   }

   /**
    * This main class is as documented on configuration-index.md, where the user can mask the password here. *
    *
    * @param args
    * @throws Exception
    */
   public static void main(String[] args) throws Exception {
      if (args.length != 1) {
         System.err.println("Use: java -cp <classPath> org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec password-to-encode");
         System.err.println("Error: no password on the args");
         System.exit(-1);
      }
      DefaultSensitiveStringCodec codec = new DefaultSensitiveStringCodec();
      Object encode = codec.encode(args[0]);
      System.out.println("Encoded password (without quotes): \"" + encode + "\"");
   }

   private void updateKey(String key) {
      this.internalKey = key.getBytes();
   }

}
