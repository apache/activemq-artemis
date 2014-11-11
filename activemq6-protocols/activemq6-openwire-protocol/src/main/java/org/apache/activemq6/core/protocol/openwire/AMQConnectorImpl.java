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
package org.apache.activemq6.core.protocol.openwire;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq6.core.protocol.openwire.amq.AMQConnector;
import org.apache.activemq6.core.protocol.openwire.amq.AMQConnectorStatistics;
import org.apache.activemq6.spi.core.remoting.Acceptor;

public class AMQConnectorImpl implements AMQConnector
{
   private Acceptor acceptor;

   public AMQConnectorImpl(Acceptor acceptorUsed)
   {
      this.acceptor = acceptorUsed;
   }

   @Override
   public BrokerInfo getBrokerInfo()
   {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public AMQConnectorStatistics getStatistics()
   {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public boolean isUpdateClusterClients()
   {
      // TODO Auto-generated method stub
      return false;
   }

   @Override
   public boolean isRebalanceClusterClients()
   {
      // TODO Auto-generated method stub
      return false;
   }

   @Override
   public void updateClientClusterInfo()
   {
      // TODO Auto-generated method stub

   }

   @Override
   public boolean isUpdateClusterClientsOnRemove()
   {
      // TODO Auto-generated method stub
      return false;
   }

   @Override
   public int connectionCount()
   {
      // TODO Auto-generated method stub
      return 0;
   }

   @Override
   public boolean isAllowLinkStealing()
   {
      // TODO Auto-generated method stub
      return false;
   }

   @Override
   public ConnectionControl getConnectionControl()
   {
      return new ConnectionControl();
   }

   @Override
   public void onStarted(OpenWireConnection connection)
   {
      // TODO Auto-generated method stub

   }

   @Override
   public void onStopped(OpenWireConnection connection)
   {
      // TODO Auto-generated method stub

   }

   public int getMaximumConsumersAllowedPerConnection()
   {
      return 1000000;//this belongs to configuration, now hardcoded
   }

   public int getMaximumProducersAllowedPerConnection()
   {
      return 1000000;//this belongs to configuration, now hardcoded
   }

   public boolean isAuditNetworkProducers()
   {
      return false;
   }

}
