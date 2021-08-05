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

public enum TargetKey {
   CLIENT_ID, SNI_HOST, SOURCE_IP, USER_NAME;

   public static final String validValues;

   static {
      StringBuffer stringBuffer = new StringBuffer();
      for (TargetKey type : TargetKey.values()) {

         if (stringBuffer.length() != 0) {
            stringBuffer.append(",");
         }

         stringBuffer.append(type.name());
      }

      validValues = stringBuffer.toString();
   }

   public static TargetKey getType(String type) {
      switch (type) {
         case "CLIENT_ID":
            return CLIENT_ID;
         case "SNI_HOST":
            return SNI_HOST;
         case "SOURCE_IP":
            return SOURCE_IP;
         case "USER_NAME":
            return USER_NAME;
         default:
            throw new IllegalStateException("Invalid RedirectKey:" + type + " valid Types: " + validValues);
      }
   }
}
