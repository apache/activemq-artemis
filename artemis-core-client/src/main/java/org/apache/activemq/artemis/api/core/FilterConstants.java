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
package org.apache.activemq.artemis.api.core;

/**
 * Constants representing pre-defined message attributes that can be referenced in ActiveMQ Artemis core
 * filter expressions.
 */
public final class FilterConstants {

   /**
    * Name of the ActiveMQ Artemis UserID header.
    */
   public static final SimpleString ACTIVEMQ_USERID = SimpleString.toSimpleString("AMQUserID");

   /**
    * Name of the ActiveMQ Artemis Message expiration header.
    */
   public static final SimpleString ACTIVEMQ_EXPIRATION = SimpleString.toSimpleString("AMQExpiration");

   /**
    * Name of the ActiveMQ Artemis Message durable header.
    */
   public static final SimpleString ACTIVEMQ_DURABLE = SimpleString.toSimpleString("AMQDurable");

   /**
    * Value for the Durable header when the message is non-durable.
    */
   public static final SimpleString NON_DURABLE = SimpleString.toSimpleString("NON_DURABLE");

   /**
    * Value for the Durable header when the message is durable.
    */
   public static final SimpleString DURABLE = SimpleString.toSimpleString("DURABLE");

   /**
    * Name of the ActiveMQ Artemis Message timestamp header.
    */
   public static final SimpleString ACTIVEMQ_TIMESTAMP = SimpleString.toSimpleString("AMQTimestamp");

   /**
    * Name of the ActiveMQ Artemis Message priority header.
    */
   public static final SimpleString ACTIVEMQ_PRIORITY = SimpleString.toSimpleString("AMQPriority");

   /**
    * Name of the ActiveMQ Artemis Message size header.
    */
   public static final SimpleString ACTIVEMQ_SIZE = SimpleString.toSimpleString("AMQSize");

   /**
    * Name of the ActiveMQ Artemis Address header
    */
   public static final SimpleString ACTIVEMQ_ADDRESS = SimpleString.toSimpleString("AMQAddress");

   /**
    * All ActiveMQ Artemis headers are prepended by this prefix.
    */
   public static final SimpleString ACTIVEMQ_PREFIX = SimpleString.toSimpleString("AMQ");

   /**
    * Proton protocol stores JMSMessageID as NATIVE_MESSAGE_ID
    */
   public static final String NATIVE_MESSAGE_ID = "NATIVE_MESSAGE_ID";

   private FilterConstants() {
      // Utility class
   }
}
