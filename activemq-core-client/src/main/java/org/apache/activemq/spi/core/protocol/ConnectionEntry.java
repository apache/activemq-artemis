/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.spi.core.protocol;

import java.util.concurrent.Executor;


/**
 * A ConnectionEntry
 *
 * @author Tim Fox
 *
 *
 */
public class ConnectionEntry
{
   public final RemotingConnection connection;

   public volatile long lastCheck;

   public volatile long ttl;

   public final Executor connectionExecutor;

   public Object getID()
   {
      return connection.getID();
   }

   public ConnectionEntry(final RemotingConnection connection, final Executor connectionExecutor, final long lastCheck, final long ttl)
   {
      this.connection = connection;

      this.lastCheck = lastCheck;

      this.ttl = ttl;

      this.connectionExecutor = connectionExecutor;
   }
}
