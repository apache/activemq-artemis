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

package org.apache.activemq.artemis.core.protocol.mqtt.exceptions;

import org.apache.activemq.artemis.core.protocol.mqtt.MQTTReasonCodes;

public class DisconnectException extends Exception {

   private final byte code;

   public DisconnectException() {
      code = MQTTReasonCodes.UNSPECIFIED_ERROR;
   }

   public DisconnectException(final byte code) {
      this.code = code;
   }

   public byte getCode() {
      return code;
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName() + "[code=" + code + "]";
   }
}