/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.activemq.artemis.utils;

import java.time.Instant;
import java.time.ZoneId;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

public class TimestampUtil {

   /**
    * Formats the given epoch time in milliseconds into an RFC 1123 formatted date-time string. This is useful, for
    * example, for providing date-time string for display on the web console.
    *
    * @param epochMillis the epoch time in milliseconds to be formatted
    * @return a string representation of the given epoch time formatted in RFC 1123 format
    * @see java.time.format.DateTimeFormatter#RFC_1123_DATE_TIME
    * @see <a href="https://tools.ietf.org/html/rfc1123">RFC 1123</a>
    */
   public static String formatEpochMillis(long epochMillis) {
      return RFC_1123_DATE_TIME.format(Instant.ofEpochMilli(epochMillis).atZone(ZoneId.of("UTC")));
   }
}