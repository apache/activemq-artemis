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
package org.apache.activemq.api.core;

/**
 * Constants representing pre-defined message attributes that can be referenced in ActiveMQ core
 * filter expressions.
 */
public final class FilterConstants
{
   /**
    * Name of the ActiveMQ UserID header.
    */
   public static final SimpleString ACTIVEMQ_USERID = new SimpleString("HQUserID");

   /**
    * Name of the ActiveMQ Message expiration header.
    */
   public static final SimpleString ACTIVEMQ_EXPIRATION = new SimpleString("HQExpiration");

   /**
    * Name of the ActiveMQ Message durable header.
    */
   public static final SimpleString ACTIVEMQ_DURABLE = new SimpleString("HQDurable");

   /**
    * Value for the Durable header when the message is non-durable.
    */
   public static final SimpleString NON_DURABLE = new SimpleString("NON_DURABLE");

   /**
    * Value for the Durable header when the message is durable.
    */
   public static final SimpleString DURABLE = new SimpleString("DURABLE");

   /**
    * Name of the ActiveMQ Message timestamp header.
    */
   public static final SimpleString ACTIVEMQ_TIMESTAMP = new SimpleString("HQTimestamp");

   /**
    * Name of the ActiveMQ Message priority header.
    */
   public static final SimpleString ACTIVEMQ_PRIORITY = new SimpleString("HQPriority");

   /**
    * Name of the ActiveMQ Message size header.
    */
   public static final SimpleString ACTIVEMQ_SIZE = new SimpleString("HQSize");

   /**
    * All ActiveMQ headers are prepended by this prefix.
    */
   public static final SimpleString ACTIVEMQ_PREFIX = new SimpleString("HQ");

   private FilterConstants()
   {
      // Utility class
   }
}
