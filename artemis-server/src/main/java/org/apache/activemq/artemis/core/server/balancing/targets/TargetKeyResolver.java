/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.balancing.targets;

import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.jboss.logging.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TargetKeyResolver {
   public static final String DEFAULT_KEY_VALUE = "DEFAULT";


   private static final Logger logger = Logger.getLogger(TargetKeyResolver.class);

   private static final char SOCKET_ADDRESS_DELIMITER = ':';
   private static final String SOCKET_ADDRESS_PREFIX = "/";


   private final TargetKey key;

   private final Pattern keyFilter;


   public TargetKey getKey() {
      return key;
   }

   public String getKeyFilter() {
      return keyFilter != null ? keyFilter.pattern() : null;
   }

   public TargetKeyResolver(TargetKey key, String keyFilter) {
      this.key = key;

      this.keyFilter = keyFilter != null ? Pattern.compile(keyFilter) : null;
   }

   public String resolve(Connection connection, String clientID, String username) {
      String keyValue = null;

      switch (key) {
         case CLIENT_ID:
            keyValue = clientID;
            break;
         case SNI_HOST:
            if (connection != null) {
               keyValue = connection.getSNIHostName();
            }
            break;
         case SOURCE_IP:
            if (connection != null &&  connection.getRemoteAddress() != null) {
               keyValue = connection.getRemoteAddress();

               boolean hasPrefix = keyValue.startsWith(SOCKET_ADDRESS_PREFIX);
               int delimiterIndex = keyValue.lastIndexOf(SOCKET_ADDRESS_DELIMITER);

               if (hasPrefix || delimiterIndex > 0) {
                  keyValue = keyValue.substring(hasPrefix ? SOCKET_ADDRESS_PREFIX.length() : 0,
                     delimiterIndex > 0 ? delimiterIndex : keyValue.length());
               }
            }
            break;
         case USER_NAME:
            keyValue = username;
            break;
         default:
            throw new IllegalStateException("Unexpected value: " + key);
      }

      if (logger.isDebugEnabled()) {
         logger.debugf("keyValue for %s: %s", key, keyValue);
      }

      if (keyValue == null) {
         keyValue = DEFAULT_KEY_VALUE;
      } else if (keyFilter != null) {
         Matcher keyMatcher = keyFilter.matcher(keyValue);

         if (keyMatcher.find()) {
            keyValue = keyMatcher.group();

            if (logger.isDebugEnabled()) {
               logger.debugf("keyValue with filter %s: %s", keyFilter, keyValue);
            }
         }
      }


      return keyValue;
   }
}
