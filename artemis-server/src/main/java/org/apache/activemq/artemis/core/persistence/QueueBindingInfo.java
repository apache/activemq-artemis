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
package org.apache.activemq.artemis.core.persistence;

import java.util.List;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.QueueStatusEncoding;

public interface QueueBindingInfo {

   long getId();

   SimpleString getAddress();

   SimpleString getQueueName();

   /**
    * used to rename the queue in case of a duplication during load time
    *
    * @param newName
    */
   void replaceQueueName(SimpleString newName);

   SimpleString getFilterString();

   boolean isAutoCreated();

   SimpleString getUser();

   void addQueueStatusEncoding(QueueStatusEncoding status);

   List<QueueStatusEncoding> getQueueStatusEncodings();

   int getMaxConsumers();

   void setMaxConsumers(int maxConsumers);

   boolean isDeleteOnNoConsumers();

   void setDeleteOnNoConsumers();
}
