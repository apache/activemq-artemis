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
package org.apache.activemq.artemis.api.core.management;

/**
 * A BrokerConnectionControl is used to manage a BrokerConnections.
 */
public interface BrokerConnectionControl extends ActiveMQComponentControl {

   /**
    * Returns {@code true} if this connection is open, {@code false} else.
    */
   @Attribute(desc = "whether this connection is open")
   boolean isOpen();

   /**
    * Returns the name of this broker connection
    */
   @Attribute(desc = "name of this broker connection")
   String getName();

   /**
    * Returns the connection uri for this broker connection.
    */
   @Attribute(desc = "connection uri for this broker connection")
   String getUri();

   /**
    * Returns the user this broker connection is using.
    */
   @Attribute(desc = "the user this broker connection is using")
   String getUser();

   /**
    * Returns the protocol this broker connection is using.
    */
   @Attribute(desc = "protocol this broker connection is using")
   String getProtocol();

   /**
    * Returns the retry interval used by this broker connection.
    */
   @Attribute(desc = "retry interval used by this broker connection")
   long getRetryInterval();

   /**
    * Returns the number of reconnection attempts used by this broker connection.
    */
   @Attribute(desc = "number of reconnection attempts used by this broker connection")
   int getReconnectAttempts();

}
