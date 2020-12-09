/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.utils;

import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.spi.JsonProvider;
import java.io.Reader;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * This is to make sure we use the proper classLoader to load JSon libraries.
 * This is equivalent to using {@link javax.json.Json}
 */
public class JsonLoader {

   private static final JsonProvider provider;

   static {
      provider = loadProvider();
   }

   private static JsonProvider loadProvider() {
      return AccessController.doPrivileged(new PrivilegedAction<JsonProvider>() {
         @Override
         public JsonProvider run() {
            ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
            try {
               Thread.currentThread().setContextClassLoader(JsonLoader.class.getClassLoader());
               return JsonProvider.provider();
            } finally {
               Thread.currentThread().setContextClassLoader(originalLoader);
            }
         }
      });

   }

   public static JsonObject readObject(Reader reader) {
      try (JsonReader jsonReader = provider.createReader(reader)) {
         return jsonReader.readObject();
      }
   }

   public static JsonArray readArray(Reader reader) {
      try (JsonReader jsonReader = provider.createReader(reader)) {
         return jsonReader.readArray();
      }
   }

   public static JsonArrayBuilder createArrayBuilder() {
      return provider.createArrayBuilder();
   }

   public static JsonObjectBuilder createObjectBuilder() {
      return provider.createObjectBuilder();
   }

}
