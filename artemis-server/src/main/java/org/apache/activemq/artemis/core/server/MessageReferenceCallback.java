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

package org.apache.activemq.artemis.core.server;

/** This is to be used in cases where a message delivery happens on an executor.
 *  Most MessageReference implementations will allow execution, and if it does,
 *  and the protocol requires an execution per message, this callback may be used.
 *
 *  At the time of this implementation only AMQP was used. */
public interface MessageReferenceCallback {
   void executeDelivery(MessageReference reference);
}
