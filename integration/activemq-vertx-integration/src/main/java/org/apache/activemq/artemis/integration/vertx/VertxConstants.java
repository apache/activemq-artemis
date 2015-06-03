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
package org.apache.activemq.artemis.integration.vertx;

import java.util.HashSet;
import java.util.Set;

public class VertxConstants
{
   // org.vertx.java.core.eventbus.impl.MessageFactory
   public static final int TYPE_PING = 0;
   public static final int TYPE_BUFFER = 1;
   public static final int TYPE_BOOLEAN = 2;
   public static final int TYPE_BYTEARRAY = 3;
   public static final int TYPE_BYTE = 4;
   public static final int TYPE_CHARACTER = 5;
   public static final int TYPE_DOUBLE = 6;
   public static final int TYPE_FLOAT = 7;
   public static final int TYPE_INT = 8;
   public static final int TYPE_LONG = 9;
   public static final int TYPE_SHORT = 10;
   public static final int TYPE_STRING = 11;
   public static final int TYPE_JSON_OBJECT = 12;
   public static final int TYPE_JSON_ARRAY = 13;
   public static final int TYPE_REPLY_FAILURE = 100;
   public static final int TYPE_RAWBYTES = 200;


   public static final String PORT = "port";
   public static final String HOST = "host";
   public static final String QUEUE_NAME = "queue";
   public static final String VERTX_ADDRESS = "vertx-address";
   public static final String VERTX_PUBLISH = "publish";
   public static final String VERTX_QUORUM_SIZE = "quorum-size";
   public static final String VERTX_HA_GROUP = "ha-group";

   public static final Set<String> ALLOWABLE_INCOMING_CONNECTOR_KEYS;
   public static final Set<String> REQUIRED_INCOMING_CONNECTOR_KEYS;
   public static final Set<String> ALLOWABLE_OUTGOING_CONNECTOR_KEYS;
   public static final Set<String> REQUIRED_OUTGOING_CONNECTOR_KEYS;
   public static final int INITIAL_MESSAGE_BUFFER_SIZE = 50;
   public static final String VERTX_MESSAGE_REPLYADDRESS = "VertxMessageReplyAddress";
   public static final String VERTX_MESSAGE_TYPE = "VertxMessageType";

   static
   {
      ALLOWABLE_INCOMING_CONNECTOR_KEYS = new HashSet<String>();
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(PORT);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(HOST);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(QUEUE_NAME);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(VERTX_ADDRESS);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(VERTX_QUORUM_SIZE);
      ALLOWABLE_INCOMING_CONNECTOR_KEYS.add(VERTX_HA_GROUP);

      REQUIRED_INCOMING_CONNECTOR_KEYS = new HashSet<String>();
      REQUIRED_INCOMING_CONNECTOR_KEYS.add(QUEUE_NAME);

      ALLOWABLE_OUTGOING_CONNECTOR_KEYS = new HashSet<String>();
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(PORT);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(HOST);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(QUEUE_NAME);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(VERTX_ADDRESS);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(VERTX_PUBLISH);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(VERTX_QUORUM_SIZE);
      ALLOWABLE_OUTGOING_CONNECTOR_KEYS.add(VERTX_HA_GROUP);

      REQUIRED_OUTGOING_CONNECTOR_KEYS = new HashSet<String>();
      REQUIRED_OUTGOING_CONNECTOR_KEYS.add(QUEUE_NAME);
   }
}
