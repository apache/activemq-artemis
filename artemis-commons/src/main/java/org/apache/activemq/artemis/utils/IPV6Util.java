/**
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

import java.util.regex.Pattern;

public class IPV6Util {
   // regex from http://stackoverflow.com/questions/53497/regular-expression-that-matches-valid-ipv6-addresses
   private static final Pattern IPV6 = Pattern.compile("(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|" +           // 1:2:3:4:5:6:7:8
                                                          "([0-9a-fA-F]{1,4}:){1,7}:|" +                           // 1::                              1:2:3:4:5:6:7::
                                                          "([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|" +           // 1::8             1:2:3:4:5:6::8  1:2:3:4:5:6::8
                                                          "([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|" +    // 1::7:8           1:2:3:4:5::7:8  1:2:3:4:5::8
                                                          "([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|" +    // 1::6:7:8         1:2:3:4::6:7:8  1:2:3:4::8
                                                          "([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|" +    // 1::5:6:7:8       1:2:3::5:6:7:8  1:2:3::8
                                                          "([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|" +    // 1::4:5:6:7:8     1:2::4:5:6:7:8  1:2::8
                                                          "[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|" +         // 1::3:4:5:6:7:8   1::3:4:5:6:7:8  1::8
                                                          ":((:[0-9a-fA-F]{1,4}){1,7}|:)|" +                       // ::2:3:4:5:6:7:8  ::2:3:4:5:6:7:8 ::8       ::
                                                          "[fF][eE]80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|" + // fe80::7:8%eth0   fe80::7:8%1     (link-local IPv6 addresses with zone index)
                                                          "::([fF]{4}(:0{1,4}){0,1}:){0,1}" +
                                                          "((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}" +
                                                          "(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|" +            // ::255.255.255.255   ::ffff:255.255.255.255  ::ffff:0:255.255.255.255  (IPv4-mapped IPv6 addresses and IPv4-translated addresses)
                                                          "([0-9a-fA-F]{1,4}:){1,4}:" +
                                                          "((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}" +
                                                          "(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))");            // 2001:db8:3:4::192.0.2.33  64:ff9b::192.0.2.33 (IPv4-Embedded IPv6 Address)


   /**
    * It will enclose an IPV6 host with [], that we need before building URIs
    * */
   public static String encloseHost(final String host) {
      if (host != null && IPV6.matcher(host).matches()) {
         return "[" + host + "]";
      }
      else {
         return host;
      }
   }
}
