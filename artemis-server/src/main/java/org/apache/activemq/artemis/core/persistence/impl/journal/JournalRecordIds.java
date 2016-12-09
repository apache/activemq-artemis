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
package org.apache.activemq.artemis.core.persistence.impl.journal;

/**
 * These record IDs definitions are meant to be public.
 * <p>
 * If any other component or any test needs to validate user-record-types from the Journal directly
 * This is where the definitions will exist and this is what these tests should be using to verify
 * the IDs.
 */
public final class JournalRecordIds {

   // grouping journal record type

   public static final byte GROUP_RECORD = 20;

   // BindingsImpl journal record type

   public static final byte QUEUE_BINDING_RECORD = 21;

   public static final byte QUEUE_STATUS_RECORD = 22;

   /**
    * Records storing the current recordID number.
    *
    * @see org.apache.activemq.artemis.utils.IDGenerator
    * @see BatchingIDGenerator
    */
   public static final byte ID_COUNTER_RECORD = 24;

   public static final byte ADDRESS_SETTING_RECORD = 25;

   public static final byte SECURITY_RECORD = 26;

   // Message journal record types

   /**
    * This is used when a large message is created but not yet stored on the system.
    * <p>
    * We use this to avoid temporary files missing
    */
   public static final byte ADD_LARGE_MESSAGE_PENDING = 29;

   public static final byte ADD_LARGE_MESSAGE = 30;

   public static final byte ADD_MESSAGE = 31;

   public static final byte ADD_REF = 32;

   public static final byte ACKNOWLEDGE_REF = 33;

   public static final byte UPDATE_DELIVERY_COUNT = 34;

   public static final byte PAGE_TRANSACTION = 35;

   public static final byte SET_SCHEDULED_DELIVERY_TIME = 36;

   public static final byte DUPLICATE_ID = 37;

   public static final byte HEURISTIC_COMPLETION = 38;

   public static final byte ACKNOWLEDGE_CURSOR = 39;

   public static final byte PAGE_CURSOR_COUNTER_VALUE = 40;

   public static final byte PAGE_CURSOR_COUNTER_INC = 41;

   public static final byte PAGE_CURSOR_COMPLETE = 42;

   public static final byte PAGE_CURSOR_PENDING_COUNTER = 43;

   public static final byte ADDRESS_BINDING_RECORD = 44;
}
