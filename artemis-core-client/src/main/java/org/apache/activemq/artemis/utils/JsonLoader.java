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

import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.spi.JsonProvider;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGeneratorFactory;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParserFactory;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

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

   public static JsonParser createParser(Reader reader) {
      return provider.createParser(reader);
   }

   public static JsonParser createParser(InputStream in) {
      return provider.createParser(in);
   }

   public static JsonGenerator createGenerator(Writer writer) {
      return provider.createGenerator(writer);
   }

   public static JsonGenerator createGenerator(OutputStream out) {
      return provider.createGenerator(out);
   }

   public static JsonParserFactory createParserFactory(Map<String, ?> config) {
      return provider.createParserFactory(config);
   }

   public static JsonGeneratorFactory createGeneratorFactory(Map<String, ?> config) {
      return provider.createGeneratorFactory(config);
   }

   public static JsonWriter createWriter(Writer writer) {
      return provider.createWriter(writer);
   }

   public static JsonWriter createWriter(OutputStream out) {
      return provider.createWriter(out);
   }

   public static JsonReader createReader(Reader reader) {
      return provider.createReader(reader);
   }

   public static JsonReader createReader(InputStream in) {
      return provider.createReader(in);
   }

   public static JsonReaderFactory createReaderFactory(Map<String, ?> config) {
      return provider.createReaderFactory(config);
   }

   public static JsonWriterFactory createWriterFactory(Map<String, ?> config) {
      return provider.createWriterFactory(config);
   }

   public static JsonArrayBuilder createArrayBuilder() {
      return provider.createArrayBuilder();
   }

   public static JsonObjectBuilder createObjectBuilder() {
      return provider.createObjectBuilder();
   }

   public static JsonBuilderFactory createBuilderFactory(Map<String, ?> config) {
      return provider.createBuilderFactory(config);
   }

}
