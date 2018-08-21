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

import java.net.URL;
import java.util.Properties;

import org.jboss.logging.Logger;

public final class ArtemisSystemProperties {

   private static Logger logger = Logger.getLogger(ArtemisSystemProperties.class);

   static {
      init();
   }

   private static ArtemisSystemProperties theInstance;

   private Properties properties;
   private boolean enable1XPrefixes = false;


   public boolean is1XPrefix() {
      return enable1XPrefixes;
   }

   public Properties getProperties() {
      return properties;
   }

   public static void init() {
      theInstance = new ArtemisSystemProperties();

      String value = theInstance.getProperties().getProperty("org.apache.activemq.artemis.enable1xPrefixes");
      if (value != null) {
         try {
            theInstance.enable1XPrefixes = Boolean.parseBoolean(value);
         } catch (Exception e) {
            logger.warn("enable1xPrefixes::" + e.getMessage(), e);
         }
      }
   }

   public static ArtemisSystemProperties getInstance() {
      return theInstance;
   }

   private ArtemisSystemProperties() {
      properties = new Properties();

      try {
         URL url = ClassloadingUtil.findResource("org.apache.activemq.Artemis.properties");
         properties.load(url.openStream());
      } catch (Throwable ignored) {
      }

      // System properties will bypass the ones from the file
      properties.putAll(System.getProperties());
   }
}
