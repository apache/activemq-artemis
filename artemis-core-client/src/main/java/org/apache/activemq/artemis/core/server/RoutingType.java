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
package org.apache.activemq.artemis.core.server;

public enum RoutingType {

   MULTICAST, ANYCAST, STRIP, PASS;

   public byte getType() {
      switch (this) {
         case MULTICAST:
            return 0;
         case ANYCAST:
            return 1;
         case STRIP:
            return 2;
         case PASS:
            return 3;
         default:
            return -1;
      }
   }

   public static RoutingType getType(byte type) {
      switch (type) {
         case 0:
            return MULTICAST;
         case 1:
            return ANYCAST;
         case 2:
            return STRIP;
         case 3:
            return PASS;
         default:
            return null;
      }
   }
}
