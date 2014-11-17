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
package org.apache.activemq.core.client.impl;

import java.util.concurrent.Executor;

import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.api.core.Pair;
import org.apache.activemq.spi.core.remoting.ClientProtocolManager;

/**
 * A ServerLocatorInternal
 *
 * @author Tim Fox
 *
 *
 */
public interface ServerLocatorInternal extends ServerLocator
{
   void start(Executor executor) throws Exception;

   void factoryClosed(final ClientSessionFactory factory);

   AfterConnectInternalListener getAfterConnectInternalListener();

   ServerLocatorInternal setAfterConnectionInternalListener(AfterConnectInternalListener listener);

   /** Used to better identify Cluster Connection Locators on logs. To facilitate eventual debugging.
    *
    *  This method used to be on tests interface, but I'm now making it part of the public interface since*/
   ServerLocatorInternal setIdentity(String identity);

   ServerLocatorInternal setNodeID(String nodeID);

   String getNodeID();

   void cleanup();

   // Reset this Locator back as if it never received any topology
   void resetToInitialConnectors();

   ClientSessionFactoryInternal connect() throws HornetQException;

   /**
    * Like {@link #connect()} but it does not log warnings if it fails to connect.
    * @throws HornetQException
    */
   ClientSessionFactoryInternal connectNoWarnings() throws HornetQException;

   void notifyNodeUp(long uniqueEventID, String nodeID, String backupGroupName, String scaleDownGroupName,
                     Pair<TransportConfiguration, TransportConfiguration> connectorPair, boolean last);

   /**
    *
    * @param uniqueEventID 0 means get the previous ID +1
    * @param nodeID
    */
   void notifyNodeDown(long uniqueEventID, String nodeID);

   ServerLocatorInternal setClusterConnection(boolean clusterConnection);

   boolean isClusterConnection();

   TransportConfiguration getClusterTransportConfiguration();

   ServerLocatorInternal setClusterTransportConfiguration(TransportConfiguration tc);

   Topology getTopology();

   ClientProtocolManager newProtocolManager();

   boolean isConnectable();
}
