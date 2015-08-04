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
package org.apache.activemq.artemis.rest.queue;

public class DestinationResource {

   protected String destination;
   protected PostMessage sender;
   protected DestinationServiceManager serviceManager;

   public DestinationServiceManager getServiceManager() {
      return serviceManager;
   }

   public void setServiceManager(DestinationServiceManager serviceManager) {
      this.serviceManager = serviceManager;
   }

   public PostMessage getSender() {
      return sender;
   }

   public void setSender(PostMessage sender) {
      this.sender = sender;
   }

   public String getDestination() {
      return destination;
   }

   public void setDestination(String destination) {
      this.destination = destination;
   }
}
