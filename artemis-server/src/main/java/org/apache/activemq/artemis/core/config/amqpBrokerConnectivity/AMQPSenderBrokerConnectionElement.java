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

package org.apache.activemq.artemis.core.config.amqpBrokerConnectivity;

/**
 * Broker connection AMQP sender connection element configuration.
 */
public class AMQPSenderBrokerConnectionElement extends AMQPBrokerConnectionElement {

   private static final long serialVersionUID = 1059627894111477952L;

   private String targetAddress;
   private String targetCapabilities;

   public AMQPSenderBrokerConnectionElement() {
      this.setType(AMQPBrokerConnectionAddressType.SENDER);
   }

   public AMQPSenderBrokerConnectionElement(String name) {
      this.setType(AMQPBrokerConnectionAddressType.SENDER);
      this.setName(name);
   }

   public String getTargetAddress() {
      return targetAddress;
   }

   public void setTargetAddress(String targetAddress) {
      this.targetAddress = targetAddress;
   }

   public String getTargetCapabilities() {
      return targetCapabilities;
   }

   public void setTargetCapabilities(String targetCapabilities) {
      this.targetCapabilities = targetCapabilities;
   }
}
