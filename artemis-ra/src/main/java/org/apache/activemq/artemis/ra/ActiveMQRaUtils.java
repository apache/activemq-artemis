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
package org.apache.activemq.artemis.ra;

import javax.naming.Context;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.jgroups.JChannel;

/**
 * Various utility functions
 */
public final class ActiveMQRaUtils {

   /**
    * Private constructor
    */
   private ActiveMQRaUtils() {
   }

   /**
    * Compare two strings.
    *
    * @param me  First value
    * @param you Second value
    * @return True if object equals else false.
    */
   @SuppressWarnings("StringEquality")
   public static boolean compare(final String me, final String you) {
      // If both null or intern equals
      if (me == you) {
         return true;
      }

      // if me null and you are not
      if (me == null) {
         return false;
      }

      // me will not be null, test for equality
      return me.equals(you);
   }

   /**
    * Compare two integers.
    *
    * @param me  First value
    * @param you Second value
    * @return True if object equals else false.
    */
   public static boolean compare(final Integer me, final Integer you) {
      // If both null or intern equals
      if (me == you) {
         return true;
      }

      // if me null and you are not
      if (me == null) {
         return false;
      }

      // me will not be null, test for equality
      return me.equals(you);
   }

   /**
    * Compare two longs.
    *
    * @param me  First value
    * @param you Second value
    * @return True if object equals else false.
    */
   public static boolean compare(final Long me, final Long you) {
      // If both null or intern equals
      if (me == you) {
         return true;
      }

      // if me null and you are not
      if (me == null) {
         return false;
      }

      // me will not be null, test for equality
      return me.equals(you);
   }

   /**
    * Compare two doubles.
    *
    * @param me  First value
    * @param you Second value
    * @return True if object equals else false.
    */
   public static boolean compare(final Double me, final Double you) {
      // If both null or intern equals
      if (me == you) {
         return true;
      }

      // if me null and you are not
      if (me == null) {
         return false;
      }

      // me will not be null, test for equality
      return me.equals(you);
   }

   /**
    * Compare two booleans.
    *
    * @param me  First value
    * @param you Second value
    * @return True if object equals else false.
    */
   public static boolean compare(final Boolean me, final Boolean you) {
      // If both null or intern equals
      if (me == you) {
         return true;
      }

      // if me null and you are not
      if (me == null) {
         return false;
      }

      // me will not be null, test for equality
      return me.equals(you);
   }

   /**
    * Lookup an object in the default initial context
    *
    * @param context The context to use
    * @param name    the name to lookup
    * @param clazz   the expected type
    * @return the object
    * @throws Exception for any error
    */
   public static Object lookup(final Context context, final String name, final Class<?> clazz) throws Exception {
      return context.lookup(name);
   }

   /**
    * Used on parsing JNDI Configuration
    *
    * @param config
    * @return hash-table with configuration option pairs
    */
   public static Hashtable<String, String> parseHashtableConfig(final String config) {
      Hashtable<String, String> hashtable = new Hashtable<>();

      String[] topElements = config.split(";");

      for (String element : topElements) {
         String[] expression = element.split("=");

         if (expression.length != 2) {
            throw new IllegalArgumentException("Invalid expression " + element + " at " + config);
         }

         hashtable.put(expression[0].trim(), expression[1].trim());
      }

      return hashtable;
   }

   public static List<Map<String, Object>> parseConfig(final String config) {
      List<Map<String, Object>> result = new ArrayList<>();

      String[] topElements = config.split(",");

      for (String topElement : topElements) {
         HashMap<String, Object> map = new HashMap<>();
         result.add(map);

         String[] elements = topElement.split(";");

         for (String element : elements) {
            String[] expression = element.split("=");

            if (expression.length != 2) {
               throw new IllegalArgumentException("Invalid expression " + element + " at " + config);
            }

            map.put(expression[0].trim(), expression[1].trim());
         }
      }

      return result;
   }

   public static List<String> parseConnectorConnectorConfig(String config) {
      List<String> res = new ArrayList<>();

      String[] elements = config.split(",");

      for (String element : elements) {
         res.add(element.trim());
      }

      return res;
   }

   /**
    * Within AS7 the RA is loaded by JCA. properties can only be passed in String form. However if
    * RA is configured using jgroups stack, we need to pass a Channel object. As is impossible with
    * JCA, we use this method to allow a JChannel object to be located.
    */
   public static JChannel locateJGroupsChannel(final String locatorClass, final String name) {
      return AccessController.doPrivileged(new PrivilegedAction<JChannel>() {
         @Override
         public JChannel run() {
            try {
               ClassLoader loader = Thread.currentThread().getContextClassLoader();
               Class<?> aClass = loader.loadClass(locatorClass);
               Object o = aClass.newInstance();
               Method m = aClass.getMethod("locateChannel", new Class[]{String.class});
               return (JChannel) m.invoke(o, name);
            } catch (Throwable e) {
               ActiveMQRALogger.LOGGER.debug(e.getMessage(), e);
               return null;
            }
         }
      });
   }

   /**
    * This seems duplicate code all over the place, but for security reasons we can't let something like this to be open in a
    * utility class, as it would be a door to load anything you like in a safe VM.
    * For that reason any class trying to do a privileged block should do with the AccessController directly.
    */
   private static Object safeInitNewInstance(final String className) {
      return AccessController.doPrivileged(new PrivilegedAction<Object>() {
         @Override
         public Object run() {
            ClassLoader loader = getClass().getClassLoader();
            try {
               Class<?> clazz = loader.loadClass(className);
               return clazz.newInstance();
            } catch (Throwable t) {
               try {
                  loader = Thread.currentThread().getContextClassLoader();
                  if (loader != null)
                     return loader.loadClass(className).newInstance();
               } catch (RuntimeException e) {
                  throw e;
               } catch (Exception e) {
               }

               throw new IllegalArgumentException("Could not find class " + className);
            }
         }
      });
   }

}
