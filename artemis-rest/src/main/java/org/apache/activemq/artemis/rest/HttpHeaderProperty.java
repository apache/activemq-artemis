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
package org.apache.activemq.artemis.rest;

public class HttpHeaderProperty {

   public static final String CONTENT_TYPE = "http_content$type";

   /**
    * Converts an HTTP header name to a selector compatible property name.  '-' character is converted to
    * '$'. The return property name will also be all lower case with an "http_" prepended.  For example
    * "Content-Type" would be converted to "http_content$type";
    *
    * @param httpHeader
    * @return
    */
   public static String toPropertyName(String httpHeader) {
      httpHeader = httpHeader.replace('-', '$');
      return "http_" + httpHeader.toLowerCase();
   }

   /**
    * Converts a JMS property name to an HTTP header name.
    *
    * @param name
    * @return null if property name isn't an HTTP header name.
    */
   public static String fromPropertyName(String name) {
      if (!name.startsWith("http_"))
         return null;
      return name.substring("http_".length()).replace('$', '-');
   }
}