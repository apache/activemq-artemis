/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.utils;

import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonArrayBuilder;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.json.JsonObjectBuilder;
import org.apache.activemq.artemis.json.impl.JsonArrayBuilderImpl;
import org.apache.activemq.artemis.json.impl.JsonArrayImpl;
import org.apache.activemq.artemis.json.impl.JsonObjectBuilderImpl;
import org.apache.activemq.artemis.json.impl.JsonObjectImpl;

import javax.json.JsonReader;
import javax.json.spi.JsonProvider;
import java.io.Reader;

/**
 * This is to make sure we use the proper classLoader to load JSon libraries. This is equivalent to using
 * {@link javax.json.Json}
 */
public class JsonLoader {

   private static final JsonProvider provider = new org.apache.johnzon.core.JsonProviderImpl();

   public static JsonObject readObject(Reader reader) {
      try (JsonReader jsonReader = provider.createReader(reader)) {
         return new JsonObjectImpl(jsonReader.readObject());
      }
   }

   public static JsonArray readArray(Reader reader) {
      try (JsonReader jsonReader = provider.createReader(reader)) {
         return new JsonArrayImpl(jsonReader.readArray());
      }
   }

   public static JsonArrayBuilder createArrayBuilder() {
      return new JsonArrayBuilderImpl(provider.createArrayBuilder());
   }

   public static JsonObjectBuilder createObjectBuilder() {
      return new JsonObjectBuilderImpl(provider.createObjectBuilder());
   }
}
