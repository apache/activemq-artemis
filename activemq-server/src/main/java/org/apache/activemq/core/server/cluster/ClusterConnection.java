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
package org.apache.activemq.core.server.cluster;

import java.util.Map;

import org.apache.activemq.api.core.Pair;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClusterTopologyListener;
import org.apache.activemq.core.client.impl.Topology;
import org.apache.activemq.core.server.ActiveMQComponent;
import org.apache.activemq.core.server.ActiveMQServer;

/**
 * A ClusterConnection
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 23 Jan 2009 14:51:55
 *
 *
 */
public interface ClusterConnection extends ActiveMQComponent, ClusterTopologyListener
{
   SimpleString getName();

   String getNodeID();

   ActiveMQServer getServer();

   void nodeAnnounced(long eventUID, String nodeID, String backupGroupName, String scaleDownGroupName, Pair<TransportConfiguration, TransportConfiguration> connectorPair, boolean backup);

   void addClusterTopologyListener(ClusterTopologyListener listener);

   void removeClusterTopologyListener(ClusterTopologyListener listener);

   /**
    * Only used for tests?
    * @return a Map of node ID and addresses
    */
   Map<String, String> getNodes();

   TransportConfiguration getConnector();

   Topology getTopology();

   void flushExecutor();

   // for debug
   String describe();

   void informClusterOfBackup();

   boolean isNodeActive(String id);

   /**
    * Verifies whether user and password match the ones configured for this ClusterConnection.
    * @param clusterUser
    * @param clusterPassword
    * @return {@code true} if username and password match, {@code false} otherwise.
    */
   boolean verify(String clusterUser, String clusterPassword);

   void removeRecord(String targetNodeID);

   void disconnectRecord(String targetNodeID);
}
