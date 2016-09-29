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
package org.apache.activemq.artemis.api.core.client;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;

/**
 * A member of the topology.
 *
 * Each TopologyMember represents a single server and possibly any backup server that may take over
 * its duties (using the nodeId of the original server).
 */
public interface TopologyMember {

   /**
    * Returns the {@code backup-group-name} of the live server and backup servers associated with
    * Topology entry.
    * <p>
    * This is a server configuration value. A (remote) backup will only work with live servers that
    * have a matching {@code backup-group-name}.
    * <p>
    * This value does not apply to "shared-storage" backup and live pairs.
    *
    * @return the {@code backup-group-name}
    */
   String getBackupGroupName();

   /**
    * Returns the {@code scale-down-group-name} of the live server with this Topology entry.
    * <p>
    * This is a server configuration value. a live server will only send its messages to another live server
    * with matching {@code scale-down-group-name}.
    * <p>
    *
    * @return the {@code scale-down-group-name}
    */
   String getScaleDownGroupName();

   /**
    * @return configuration relative to the live server
    */
   TransportConfiguration getLive();

   /**
    * Returns the TransportConfiguration relative to the backup server if any.
    *
    * @return a {@link TransportConfiguration} for the backup, or null} if the live server has no
    * backup server.
    */
   TransportConfiguration getBackup();

   /**
    * Returns the nodeId of the server.
    *
    * @return the nodeId
    */
   String getNodeId();

   /**
    * @return long value representing a unique event ID
    */
   long getUniqueEventID();

   /**
    * Returns true if this TopologyMember is the target of this remoting connection
    *
    * @param connection
    * @return
    */
   boolean isMember(RemotingConnection connection);

   /**
    * Returns true if this configuration is the target of this remoting connection
    *
    * @param configuration
    * @return
    */
   boolean isMember(TransportConfiguration configuration);

   String toURI();
}
