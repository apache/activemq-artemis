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

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.activemq.artemis.logs.ActiveMQUtilBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * A DefaultSensitiveDataCodec
 *
 * The default implementation of SensitiveDataCodec.
 * This class is used when the user indicates in the config
 * file to use a masked password but doesn't give a
 * codec implementation.
 *
 * It supports one-way hash (digest) and two-way (encrypt-decrpt) algorithms
 * The two-way uses "Blowfish" algorithm
 * The one-way uses "PBKDF2" hash algorithm
 */
public class DefaultSensitiveStringCodec implements SensitiveDataCodec<String> {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String ALGORITHM_PARAM = "algorithm";
   public static final String ALGORITHM_SYS_PROP = "artemis.default.sensitive.string.codec.algorithm";
   public static final String ALGORITHM_ENV_VAR = "ARTEMIS_DEFAULT_SENSITIVE_STRING_CODEC_ALGORITHM";
   public static final String ALGORITHM_ONE_WAY = "one-way";
   public static final String ALGORITHM_TWO_WAY = "two-way";
   public static final String ALGORITHM_TWO_WAY_1 = "two-way-1";
   public static final String KEY_PARAM = "key";
   public static final String KEY_SYS_PROP = "artemis.default.sensitive.string.codec.key";
   public static final String KEY_ENV_VAR  = "ARTEMIS_DEFAULT_SENSITIVE_STRING_CODEC_KEY";

   private CodecAlgorithm algorithm;

   @Override
   public String decode(Object secret) throws Exception {
      if (algorithm == null) {
         throw new IllegalStateException("Algorithm not initialized");
      }

      return algorithm.decode((String) secret);
   }

   @Override
   public String encode(Object secret) throws Exception {
      if (algorithm == null) {
         throw new IllegalStateException("Algorithm not initialized");
      }

      return algorithm.encode((String) secret);
   }

   @Override
   public void init(Map<String, String> params) throws Exception {

      logger.trace("Loading algorithm from params {}", ALGORITHM_PARAM);
      String algorithmName = params.get(ALGORITHM_PARAM);
      if (algorithmName == null || algorithmName.trim().length() == 0) {
         logger.trace("Loading algorithm from system property {}", ALGORITHM_SYS_PROP);
         algorithmName = System.getProperty(ALGORITHM_SYS_PROP);
         if (algorithmName == null || algorithmName.trim().length() == 0) {
            logger.trace("Loading algorithm from env var {}", ALGORITHM_ENV_VAR);
            algorithmName = getFromEnv(ALGORITHM_ENV_VAR);
            if (algorithmName == null || algorithmName.trim().length() == 0) {
               algorithmName = ALGORITHM_TWO_WAY;
            }
         }
      }

      if (algorithmName.equals(ALGORITHM_ONE_WAY)) {
         algorithm = new PBKDF2Algorithm(params);
      } else if (algorithmName.equals(ALGORITHM_TWO_WAY)) {
         algorithm = new BlowfishAlgorithm(params);
      } else if (algorithmName.equals(ALGORITHM_TWO_WAY_1)) {
         algorithm = new AESAlgorithm(params);
      } else {
         throw ActiveMQUtilBundle.BUNDLE.invalidAlgorithm(algorithmName);
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
      Map<String, String> params = new HashMap<>();
      Properties properties = System.getProperties();
      synchronized (properties) {
         for (final String name : properties.stringPropertyNames()) {
            params.put(name, properties.getProperty(name));
         }
      }
      codec.init(params);
      Object encode = codec.encode(args[0]);

      System.out.println("Encoded password (without quotes): \"" + encode + "\"");
   }

   @Override
   public boolean verify(char[] inputValue, String storedValue) {
      return algorithm.verify(inputValue, storedValue);
   }

   private abstract static class CodecAlgorithm {
      public static final String SEPARATOR = ":";

      protected Map<String, String> params;

      CodecAlgorithm(Map<String, String> params) {
         this.params = params;
      }

      public abstract String decode(String secret) throws Exception;
      public abstract String encode(String secret) throws Exception;

      public boolean verify(char[] inputValue, String storedValue) {
         return false;
      }
   }

   @Deprecated
   private class BlowfishAlgorithm extends CodecAlgorithm {

      private byte[] internalKey = "clusterpassword".getBytes();


      BlowfishAlgorithm(Map<String, String> params) {
         super(params);
         String key = params.get(KEY_PARAM);
         if (key != null) {
            updateKey(key);
         } else {
            key = System.getProperty(KEY_SYS_PROP);
            if (key != null && key.trim().length() > 0) {
               logger.trace("Set key from system property {}", KEY_SYS_PROP);
               updateKey(key);
            }
            if (key == null) {
               key = getFromEnv(KEY_ENV_VAR);
               if (key != null) {
                  logger.trace("Set key from env var {}", KEY_ENV_VAR);
                  updateKey(key);
               }
            }
         }
      }

      private void updateKey(String key) {
         this.internalKey = key.getBytes();
      }

      @Override
      public String decode(String secret) throws Exception {
         SecretKeySpec key = new SecretKeySpec(internalKey, "Blowfish");

         byte[] encoding;
         try {
            encoding = new BigInteger(secret, 16).toByteArray();
         } catch (Exception ex) {
            if (logger.isDebugEnabled()) {
               logger.debug(ex.getMessage(), ex);
            }
            throw new IllegalArgumentException("Password must be encrypted.");
         }

         if (encoding.length % 8 != 0) {
            int length = encoding.length;
            int newLength = ((length / 8) + 1) * 8;
            int pad = newLength - length; // number of leading zeros
            byte[] old = encoding;
            encoding = new byte[newLength];
            System.arraycopy(old, 0, encoding, pad, old.length);
         }

         Cipher cipher = Cipher.getInstance("Blowfish");
         cipher.init(Cipher.DECRYPT_MODE, key);
         byte[] decode = cipher.doFinal(encoding);

         return new String(decode);
      }

      @Override
      public String encode(String secret) throws Exception {
         SecretKeySpec key = new SecretKeySpec(internalKey, "Blowfish");

         Cipher cipher = Cipher.getInstance("Blowfish");
         cipher.init(Cipher.ENCRYPT_MODE, key);
         byte[] encoding = cipher.doFinal(secret.getBytes());
         BigInteger n = new BigInteger(encoding);
         return n.toString(16);
      }

      @Override
      public boolean verify(char[] inputValue, String storedValue) {
         try {
            return Objects.equals(storedValue, encode(String.valueOf(inputValue)));
         } catch (Exception e) {
            logger.debug("Exception on verifying:", e);
            return false;
         }
      }
   }

   private class AESAlgorithm extends CodecAlgorithm {
      private static final String CIPHER_TRANSFORMATION = "AES/CBC/PKCS5Padding";
      private static final String SECURITY_KEY_ALGORITHM = "AES";
      private static final String SECURITY_KEY_FACTORY_ALGORITHM = "PBKDF2WithHmacSHA256";
      private static final int SECURITY_KEY_ITERATIONS = 65536;
      private static final int SECURITY_KEY_LENGTH = 256;

      private char[] key;
      private SecureRandom secureRandom;

      AESAlgorithm(Map<String, String> params) {
         super(params);

         logger.trace("Loading key from params {}", KEY_PARAM);
         String keyString = params.get(KEY_PARAM);
         if (keyString == null) {
            logger.trace("Loading key from system property {}", KEY_SYS_PROP);
            keyString = System.getProperty(KEY_SYS_PROP);
            if (keyString == null || keyString.trim().length() == 0) {
               logger.trace("Loading key from env var {}", KEY_ENV_VAR);
               keyString = getFromEnv(KEY_ENV_VAR);
               if (keyString == null || keyString.trim().length() == 0) {
                  throw new IllegalArgumentException("Invalid key value");
               }
            }
         }

         secureRandom = new SecureRandom();
         key = keyString.toCharArray();
      }

      @Override
      public String decode(String secret) throws Exception {
         String[] secretTokens = secret.split(SEPARATOR);
         if (secretTokens.length != 3) {
            throw new IllegalArgumentException("Invalid number of secret tokens");
         }
         byte[] salt = ByteUtil.hexToBytes(secretTokens[0]);
         KeySpec keySpec = new PBEKeySpec(key, salt, SECURITY_KEY_ITERATIONS, SECURITY_KEY_LENGTH);
         SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(SECURITY_KEY_FACTORY_ALGORITHM);
         SecretKey secretKey = new SecretKeySpec(secretKeyFactory.generateSecret(keySpec)
            .getEncoded(), SECURITY_KEY_ALGORITHM);
         byte[] iv = ByteUtil.hexToBytes(secretTokens[1]);
         Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
         cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(iv));
         byte[] decryptedBytes = cipher.doFinal(ByteUtil.hexToBytes(secretTokens[2]));
         return new String(decryptedBytes);
      }

      @Override
      public String encode(String secret) throws Exception {
         byte[] salt = new byte[32];
         secureRandom.nextBytes(salt);
         KeySpec keySpec = new PBEKeySpec(key, salt, SECURITY_KEY_ITERATIONS, SECURITY_KEY_LENGTH);
         SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(SECURITY_KEY_FACTORY_ALGORITHM);
         SecretKey secretKey = new SecretKeySpec(secretKeyFactory.generateSecret(keySpec)
            .getEncoded(), SECURITY_KEY_ALGORITHM);
         byte[] iv = new byte[16];
         secureRandom.nextBytes(iv);
         Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
         cipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(iv));
         byte[] encryptedBytes = cipher.doFinal(secret.getBytes());
         StringBuilder builder = new StringBuilder();
         builder.append(ByteUtil.bytesToHex(salt))
            .append(SEPARATOR).append(ByteUtil.bytesToHex(iv))
            .append(SEPARATOR).append(ByteUtil.bytesToHex(encryptedBytes));
         return builder.toString();
      }

      @Override
      public boolean verify(char[] inputValue, String storedValue) {
         try {
            return Objects.equals(String.valueOf(inputValue), decode(storedValue));
         } catch (Exception e) {
            logger.debug("Exception on verifying:", e);
            return false;
         }
      }
   }

   protected String getFromEnv(final String envVarName) {
      return System.getenv(envVarName);
   }

   private static class PBKDF2Algorithm extends CodecAlgorithm {
      private String sceretKeyAlgorithm = "PBKDF2WithHmacSHA1";
      private String randomScheme = "SHA1PRNG";
      private int keyLength = 64 * 8;
      private int saltLength = 32;
      private int iterations = 1024;
      private SecretKeyFactory skf;
      private static SecureRandom sr;

      PBKDF2Algorithm(Map<String, String> params) throws NoSuchAlgorithmException {
         super(params);
         skf = SecretKeyFactory.getInstance(sceretKeyAlgorithm);
         if (sr == null) {
            sr = SecureRandom.getInstance(randomScheme);
         }
      }

      @Override
      public String decode(String secret) throws Exception {
         throw new IllegalArgumentException("Algorithm doesn't support decoding");
      }

      public byte[] getSalt() {
         byte[] salt = new byte[this.saltLength];
         sr.nextBytes(salt);
         return salt;
      }

      @Override
      public String encode(String secret) throws Exception {
         char[] chars = secret.toCharArray();
         byte[] salt = getSalt();

         StringBuilder builder = new StringBuilder();
         builder.append(iterations).append(SEPARATOR).append(ByteUtil.bytesToHex(salt)).append(SEPARATOR);

         PBEKeySpec spec = new PBEKeySpec(chars, salt, iterations, keyLength);

         byte[] hash = skf.generateSecret(spec).getEncoded();
         String hexValue = ByteUtil.bytesToHex(hash);
         builder.append(hexValue);

         return builder.toString();
      }

      @Override
      public boolean verify(char[] plainChars, String storedValue) {
         String[] parts = storedValue.split(SEPARATOR);
         int originalIterations = Integer.parseInt(parts[0]);
         byte[] salt = ByteUtil.hexToBytes(parts[1]);
         byte[] originalHash = ByteUtil.hexToBytes(parts[2]);

         PBEKeySpec spec = new PBEKeySpec(plainChars, salt, originalIterations, originalHash.length * 8);
         byte[] newHash;

         try {
            newHash = skf.generateSecret(spec).getEncoded();
         } catch (InvalidKeySpecException e) {
            return false;
         }

         return Arrays.equals(newHash, originalHash);
      }
   }
}
