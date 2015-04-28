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
package org.apache.activemq.artemis.core.client.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;

public class AddressQueryImpl implements ClientSession.AddressQuery
{
   private final boolean exists;

   private final ArrayList<SimpleString> queueNames;

   private final boolean autoCreateJmsQueues;

   public AddressQueryImpl(final boolean exists, final List<SimpleString> queueNames, final boolean autoCreateJmsQueues)
   {
      this.exists = exists;
      this.queueNames = new ArrayList<SimpleString>(queueNames);
      this.autoCreateJmsQueues = autoCreateJmsQueues;
   }

   public List<SimpleString> getQueueNames()
   {
      return queueNames;
   }

   public boolean isExists()
   {
      return exists;
   }

   public boolean isAutoCreateJmsQueues()
   {
      return autoCreateJmsQueues;
   }
}
