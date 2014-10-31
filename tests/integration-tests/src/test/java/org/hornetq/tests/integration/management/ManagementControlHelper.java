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
package org.hornetq.tests.integration.management;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.AcceptorControl;
import org.hornetq.api.core.management.AddressControl;
import org.hornetq.api.core.management.BridgeControl;
import org.hornetq.api.core.management.BroadcastGroupControl;
import org.hornetq.api.core.management.ClusterConnectionControl;
import org.hornetq.api.core.management.DivertControl;
import org.hornetq.api.core.management.HornetQServerControl;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.api.core.management.QueueControl;
import org.hornetq.api.jms.management.ConnectionFactoryControl;
import org.hornetq.api.jms.management.JMSQueueControl;
import org.hornetq.api.jms.management.JMSServerControl;
import org.hornetq.api.jms.management.TopicControl;

/**
 * A ManagementControlHelper
 *
 * @author jmesnil
 *
 */
public class ManagementControlHelper
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static AcceptorControl createAcceptorControl(final String name, final MBeanServer mbeanServer) throws Exception
   {
      return (AcceptorControl)ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getAcceptorObjectName(name),
                                                                  AcceptorControl.class,
                                                                  mbeanServer);
   }

   public static BroadcastGroupControl createBroadcastGroupControl(final String name, final MBeanServer mbeanServer) throws Exception
   {
      return (BroadcastGroupControl)ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getBroadcastGroupObjectName(name),
                                                                        BroadcastGroupControl.class,
                                                                        mbeanServer);
   }

   public static BridgeControl createBridgeControl(final String name, final MBeanServer mbeanServer) throws Exception
   {
      return (BridgeControl)ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getBridgeObjectName(name),
                                                                BridgeControl.class,
                                                                mbeanServer);
   }

   public static DivertControl createDivertControl(final String name, final MBeanServer mbeanServer) throws Exception
   {
      return (DivertControl)ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getDivertObjectName(name),
                                                                DivertControl.class,
                                                                mbeanServer);
   }

   public static ClusterConnectionControl createClusterConnectionControl(final String name,
                                                                         final MBeanServer mbeanServer) throws Exception
   {
      return (ClusterConnectionControl)ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getClusterConnectionObjectName(name),
                                                                           ClusterConnectionControl.class,
                                                                           mbeanServer);
   }

   public static HornetQServerControl createHornetQServerControl(final MBeanServer mbeanServer) throws Exception
   {
      return (HornetQServerControl)ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getHornetQServerObjectName(),
                                                                       HornetQServerControl.class,
                                                                       mbeanServer);
   }

   public static QueueControl createQueueControl(final SimpleString address,
                                                 final SimpleString name,
                                                 final MBeanServer mbeanServer) throws Exception
   {
      return (QueueControl)ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getQueueObjectName(address,
                                                                                                            name),
                                                               QueueControl.class,
                                                               mbeanServer);
   }

   public static AddressControl createAddressControl(final SimpleString address, final MBeanServer mbeanServer) throws Exception
   {
      return (AddressControl)ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getAddressObjectName(address),
                                                                 AddressControl.class,
                                                                 mbeanServer);
   }

   public static JMSQueueControl createJMSQueueControl(final Queue queue, final MBeanServer mbeanServer) throws Exception
   {
      return ManagementControlHelper.createJMSQueueControl(queue.getQueueName(), mbeanServer);
   }

   public static JMSQueueControl createJMSQueueControl(final String name, final MBeanServer mbeanServer) throws Exception
   {
      return (JMSQueueControl)ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(name),
                                                                  JMSQueueControl.class,
                                                                  mbeanServer);
   }

   public static JMSServerControl createJMSServerControl(final MBeanServer mbeanServer) throws Exception
   {
      return (JMSServerControl)ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getJMSServerObjectName(),
                                                                   JMSServerControl.class,
                                                                   mbeanServer);
   }

   public static ConnectionFactoryControl createConnectionFactoryControl(final String name,
                                                                         final MBeanServer mbeanServer) throws Exception
   {
      return (ConnectionFactoryControl)ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getConnectionFactoryObjectName(name),
                                                                           ConnectionFactoryControl.class,
                                                                           mbeanServer);
   }

   public static TopicControl createTopicControl(final Topic topic, final MBeanServer mbeanServer) throws Exception
   {
      return (TopicControl)ManagementControlHelper.createProxy(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topic.getTopicName()),
                                                               TopicControl.class,
                                                               mbeanServer);
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static Object createProxy(final ObjectName objectName,
                                     final Class mbeanInterface,
                                     final MBeanServer mbeanServer)
   {
      return MBeanServerInvocationHandler.newProxyInstance(mbeanServer, objectName, mbeanInterface, false);
   }

   // Inner classes -------------------------------------------------

}
