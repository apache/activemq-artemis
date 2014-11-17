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
package org.apache.activemq.utils;

import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.core.client.HornetQClientLogger;
import org.apache.activemq.core.client.HornetQClientMessageBundle;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * A ConfigurationHelper
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ConfigurationHelper
{
   public static String getStringProperty(final String propName, final String def, final Map<String, Object> props)
   {
      if (props == null)
      {
         return def;
      }

      Object prop = props.get(propName);

      if (prop == null)
      {
         return def;
      }
      else
      {
         if (prop instanceof String == false)
         {
            return prop.toString();
         }
         else
         {
            return (String)prop;
         }
      }
   }

   public static int getIntProperty(final String propName, final int def, final Map<String, Object> props)
   {
      if (props == null)
      {
         return def;
      }
      Object prop = props.get(propName);

      if (prop == null)
      {
         return def;
      }
      else
      {
         // The resource adapter will aways send Strings, hence the conversion here
         if (prop instanceof String)
         {
            return Integer.valueOf((String)prop);
         }
         else if (prop instanceof Number == false)
         {
            HornetQClientLogger.LOGGER.propertyNotInteger(propName, prop.getClass().getName());

            return def;
         }
         else
         {
            return ((Number)prop).intValue();
         }
      }
   }

   public static long getLongProperty(final String propName, final long def, final Map<String, Object> props)
   {
      if (props == null)
      {
         return def;
      }

      Object prop = props.get(propName);

      if (prop == null)
      {
         return def;
      }
      else
      {
         // The resource adapter will aways send Strings, hence the conversion here
         if (prop instanceof String)
         {
            return Long.valueOf((String)prop);
         }
         else if (prop instanceof Number == false)
         {
            HornetQClientLogger.LOGGER.propertyNotLong(propName, prop.getClass().getName());

            return def;
         }
         else
         {
            return ((Number)prop).longValue();
         }
      }
   }

   public static boolean getBooleanProperty(final String propName, final boolean def, final Map<String, Object> props)
   {
      if (props == null)
      {
         return def;
      }

      Object prop = props.get(propName);

      if (prop == null)
      {
         return def;
      }
      else
      {
         // The resource adapter will aways send Strings, hence the conversion here
         if (prop instanceof String)
         {
            return Boolean.valueOf((String)prop);
         }
         else if (prop instanceof Boolean == false)
         {
            HornetQClientLogger.LOGGER.propertyNotBoolean(propName, prop.getClass().getName());

            return def;
         }
         else
         {
            return (Boolean)prop;
         }
      }
   }

   public static Set<String> checkKeys(final Set<String> allowableKeys, final Set<String> keys)
   {
      Set<String> invalid = new HashSet<String>();

      for (String key : keys)
      {
         if (!allowableKeys.contains(key))
         {
            invalid.add(key);
         }
      }
      return invalid;
   }

   public static Set<String> checkKeysExist(final Set<String> requiredKeys, final Set<String> keys)
   {
      Set<String> invalid = new HashSet<String>(requiredKeys);

      for (String key : keys)
      {
         if (requiredKeys.contains(key))
         {
            invalid.remove(key);
         }
      }
      return invalid;
   }

   public static String stringSetToCommaListString(final Set<String> invalid)
   {
      StringBuilder sb = new StringBuilder();
      int count = 0;
      for (String key : invalid)
      {
         sb.append(key);
         if (count != invalid.size() - 1)
         {
            sb.append(", ");
         }
         count++;
      }
      return sb.toString();
   }

   public static String getPasswordProperty(final String propName,
         final String def, final Map<String, Object> props,
         String defaultMaskPassword, String defaultPasswordCodec)
   {
      if (props == null)
      {
         return def;
      }

      Object prop = props.get(propName);

      if (prop == null)
      {
         return def;
      }

      String value = prop.toString();
      Boolean useMask = (Boolean) props.get(defaultMaskPassword);
      if (useMask == null || (!useMask))
      {
         return value;
      }

      final String classImpl = (String) props.get(defaultPasswordCodec);

      if (classImpl == null)
      {
         throw HornetQClientMessageBundle.BUNDLE.noCodec();
      }

      SensitiveDataCodec<String> codec = null;
      try
      {
         codec = PasswordMaskingUtil.getCodec(classImpl);
      }
      catch (HornetQException e1)
      {
         throw HornetQClientMessageBundle.BUNDLE.failedToGetDecoder(e1);
      }

      try
      {
         return codec.decode(value);
      }
      catch (Exception e)
      {
         throw HornetQClientMessageBundle.BUNDLE.errordecodingPassword(e);
      }
   }

}
