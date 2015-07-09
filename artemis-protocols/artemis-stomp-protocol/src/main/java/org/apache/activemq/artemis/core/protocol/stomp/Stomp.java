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
package org.apache.activemq.artemis.core.protocol.stomp;

/**
 * The standard verbs and headers used for the <a href="http://stomp.codehaus.org/">STOMP</a> protocol.
 *
 * @version $Revision: 57 $
 */
public interface Stomp
{
   String NULL = "\u0000";

   String NEWLINE = "\n";

   public interface Commands
   {
      String CONNECT = "CONNECT";

      String SEND = "SEND";

      String DISCONNECT = "DISCONNECT";

      String SUBSCRIBE = "SUBSCRIBE";

      String UNSUBSCRIBE = "UNSUBSCRIBE";

      String BEGIN = "BEGIN";

      String COMMIT = "COMMIT";

      String ABORT = "ABORT";

      String ACK = "ACK";

      //1.1
      String NACK = "NACK";

      String STOMP = "STOMP";
   }

   public interface Responses
   {
      String CONNECTED = "CONNECTED";

      String ERROR = "ERROR";

      String MESSAGE = "MESSAGE";

      String RECEIPT = "RECEIPT";
   }

   public interface Headers
   {
      String SEPARATOR = ":";

      String RECEIPT_REQUESTED = "receipt";

      String TRANSACTION = "transaction";

      String CONTENT_LENGTH = "content-length";

      String ACCEPT_VERSION = "accept-version";

      String CONTENT_TYPE = "content-type";

      public interface Response
      {
         String RECEIPT_ID = "receipt-id";
      }

      public interface Send
      {
         String DESTINATION = "destination";

         String CORRELATION_ID = "correlation-id";

         String REPLY_TO = "reply-to";

         String EXPIRATION_TIME = "expires";

         String PRIORITY = "priority";

         String TYPE = "type";

         Object PERSISTENT = "persistent";
      }

      public interface Message
      {
         String MESSAGE_ID = "message-id";

         String DESTINATION = "destination";

         String CORRELATION_ID = "correlation-id";

         String EXPIRATION_TIME = "expires";

         String REPLY_TO = "reply-to";

         String PRORITY = "priority";

         String REDELIVERED = "redelivered";

         String TIMESTAMP = "timestamp";

         String TYPE = "type";

         String SUBSCRIPTION = "subscription";

         String ACK = "ack";
      }

      public interface Subscribe
      {
         String DESTINATION = "destination";

         String ACK_MODE = "ack";

         String ID = "id";

         String SELECTOR = "selector";

         String DURABLE_SUBSCRIBER_NAME = "durable-subscriber-name";

         String NO_LOCAL = "no-local";

         public interface AckModeValues
         {
            String AUTO = "auto";

            String CLIENT = "client";

            String CLIENT_INDIVIDUAL = "client-individual";
         }
      }

      public interface Unsubscribe
      {
         String DESTINATION = "destination";

         String ID = "id";

         String DURABLE_SUBSCRIBER_NAME = "durable-subscriber-name";
      }

      public interface Connect
      {
         String LOGIN = "login";

         String PASSCODE = "passcode";

         String CLIENT_ID = "client-id";

         String REQUEST_ID = "request-id";

         //1.1
         String ACCEPT_VERSION = "accept-version";
         String HOST = "host";

         Object HEART_BEAT = "heart-beat";
      }

      public interface Error
      {
         String MESSAGE = "message";

         //1.1
         String VERSION = "version";
      }

      public interface Connected
      {
         String SESSION = "session";

         String RESPONSE_ID = "response-id";

         //1.1
         String VERSION = "version";

         String SERVER = "server";

         String HEART_BEAT = "heart-beat";
      }

      public interface Ack
      {
         String MESSAGE_ID = "message-id";

         //1.1
         String SUBSCRIPTION = "subscription";

         //1.2
         String ID = "id";
      }
   }
}
