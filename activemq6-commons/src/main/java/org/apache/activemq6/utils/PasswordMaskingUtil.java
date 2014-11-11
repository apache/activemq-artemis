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
package org.apache.activemq6.utils;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.HornetQExceptionType;

/**
 * A PasswordMarkingUtil
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 *
 */
public class PasswordMaskingUtil
{
   /*
    * Loading the codec class.
    *
    * @param codecDesc This parameter must have the following format:
    *
    * <full qualified class name>;key=value;key1=value1;...
    *
    * Where only <full qualified class name> is required. key/value pairs are optional
    */
   public static SensitiveDataCodec<String> getCodec(String codecDesc) throws HornetQException
   {
      SensitiveDataCodec<String> codecInstance = null;

      // semi colons
      String[] parts = codecDesc.split(";");

      if (parts.length < 1)
         throw new HornetQException(HornetQExceptionType.ILLEGAL_STATE, "Invalid PasswordCodec value: " + codecDesc);

      final String codecClassName = parts[0];

      // load class
      codecInstance = AccessController.doPrivileged(new PrivilegedAction<SensitiveDataCodec<String>>()
      {
         public SensitiveDataCodec<String> run()
         {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try
            {
               Class<?> clazz = loader.loadClass(codecClassName);
               return (SensitiveDataCodec<String>)clazz.newInstance();
            }
            catch (Exception e)
            {
               throw HornetQUtilBundle.BUNDLE.errorCreatingCodec(e, codecClassName);
            }
         }
      });

      if (parts.length > 1)
      {
         Map<String, String> props = new HashMap<String, String>();

         for (int i = 1; i < parts.length; i++)
         {
            String[] keyVal = parts[i].split("=");
            if (keyVal.length != 2)
               throw HornetQUtilBundle.BUNDLE.invalidProperty(parts[i]);
            props.put(keyVal[0], keyVal[1]);
         }
         codecInstance.init(props);
      }

      return codecInstance;
   }

   public static SensitiveDataCodec<String> getDefaultCodec()
   {
      return new DefaultSensitiveStringCodec();
   }
}
