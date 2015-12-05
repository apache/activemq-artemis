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
package org.apache.activemq.artemis.tests.integration.stomp.util;

/**
 * pls use factory to create frames.
 */
public class ClientStompFrameV11 extends AbstractClientStompFrame {

   static {
      validCommands.add("NACK");
      validCommands.add("STOMP");
   }

   boolean forceOneway = false;
   boolean isPing = false;

   public ClientStompFrameV11(String command) {
      super(command);
   }

   public ClientStompFrameV11(String command, boolean validate) {
      super(command, validate);
   }

   @Override
   public void setForceOneway() {
      forceOneway = true;
   }

   @Override
   public boolean needsReply() {
      if (forceOneway)
         return false;

      if ("CONNECT".equals(command) || "STOMP".equals(command) || headerKeys.contains(HEADER_RECEIPT)) {
         return true;
      }
      return false;
   }

   @Override
   public void setPing(boolean b) {
      isPing = b;
   }

   @Override
   public boolean isPing() {
      return isPing;
   }

   @Override
   public String toString() {
      return "[1.1]" + super.toString();
   }
}
