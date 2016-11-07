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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.logs.ActiveMQUtilBundle;

public final class PasswordMaskingUtil {

   private PasswordMaskingUtil() {

   }

   private static final class LazyPlainTextProcessorHolder {

      private LazyPlainTextProcessorHolder() {

      }

      private static final HashProcessor INSTANCE = new NoHashProcessor();

   }

   private static final class LazySecureProcessorHolder {

      private LazySecureProcessorHolder() {

      }

      private static final HashProcessor INSTANCE;
      private static final Exception EXCEPTION;

      static {
         HashProcessor processor = null;
         Exception exception = null;
         final String codecDesc = "org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec;algorithm=one-way";
         try {
            final DefaultSensitiveStringCodec codec = (DefaultSensitiveStringCodec) PasswordMaskingUtil.getCodec(codecDesc);
            processor = new SecureHashProcessor(codec);
         } catch (Exception e) {
            //THE STACK TRACE IS THE ORIGINAL ONE!
            exception = e;
         } finally {
            EXCEPTION = exception;
            INSTANCE = processor;
         }
      }
   }

   //stored password takes 2 forms, ENC() or plain text
   public static HashProcessor getHashProcessor(String storedPassword) throws Exception {

      if (!isEncoded(storedPassword)) {
         return LazyPlainTextProcessorHolder.INSTANCE;
      }
      final Exception secureProcessorException = LazySecureProcessorHolder.EXCEPTION;
      if (secureProcessorException != null) {
         //reuse old descriptions/messages of the exception but refill the stack trace
         throw new RuntimeException(secureProcessorException);
      }
      return LazySecureProcessorHolder.INSTANCE;
   }

   private static boolean isEncoded(String storedPassword) {
      return storedPassword == null || (storedPassword.startsWith("ENC(") && storedPassword.endsWith(")"));
   }

   public static HashProcessor getHashProcessor() {
      HashProcessor processor = LazySecureProcessorHolder.INSTANCE;
      //it can be null due to a previous failed attempts to instantiate it!
      if (processor == null) {
         processor = LazyPlainTextProcessorHolder.INSTANCE;
      }
      return processor;
   }

   /*
    * Loading the codec class.
    *
    * @param codecDesc This parameter must have the following format:
    *
    * <full qualified class name>;key=value;key1=value1;...
    *
    * Where only <full qualified class name> is required. key/value pairs are optional
    */
   public static SensitiveDataCodec<String> getCodec(String codecDesc) throws ActiveMQException {
      SensitiveDataCodec<String> codecInstance;

      // semi colons
      String[] parts = codecDesc.split(";");

      if (parts.length < 1)
         throw new ActiveMQException(ActiveMQExceptionType.ILLEGAL_STATE, "Invalid PasswordCodec value: " + codecDesc);

      final String codecClassName = parts[0];

      // load class
      codecInstance = AccessController.doPrivileged(new PrivilegedAction<SensitiveDataCodec<String>>() {
         @Override
         public SensitiveDataCodec<String> run() {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try {
               Class<?> clazz = loader.loadClass(codecClassName);
               return (SensitiveDataCodec<String>) clazz.newInstance();
            } catch (Exception e) {
               throw ActiveMQUtilBundle.BUNDLE.errorCreatingCodec(e, codecClassName);
            }
         }
      });

      if (parts.length > 1) {
         Map<String, String> props = new HashMap<>();

         for (int i = 1; i < parts.length; i++) {
            String[] keyVal = parts[i].split("=");
            if (keyVal.length != 2)
               throw ActiveMQUtilBundle.BUNDLE.invalidProperty(parts[i]);
            props.put(keyVal[0], keyVal[1]);
         }
         try {
            codecInstance.init(props);
         } catch (Exception e) {
            throw new ActiveMQException("Fail to init codec", e, ActiveMQExceptionType.SECURITY_EXCEPTION);
         }
      }

      return codecInstance;
   }

   public static DefaultSensitiveStringCodec getDefaultCodec() {
      return new DefaultSensitiveStringCodec();
   }

}
