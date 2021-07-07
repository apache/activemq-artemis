/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.management.impl;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.BrokerBalancerControl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.balancing.BrokerBalancer;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.utils.JsonLoader;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import javax.management.NotCompliantMBeanException;

public class BrokerBalancerControlImpl extends AbstractControl implements BrokerBalancerControl {
   private final BrokerBalancer balancer;

   public BrokerBalancerControlImpl(final BrokerBalancer balancer, final StorageManager storageManager) throws NotCompliantMBeanException {
      super(BrokerBalancerControl.class, storageManager);
      this.balancer = balancer;
   }

   @Override
   public Object getTarget(String key) {
      Target target = balancer.getTarget(key);

      if (target != null) {
         TransportConfiguration connector = target.getConnector();

         return new Object[] {
            target.getNodeID(),
            target.isLocal(),
            connector == null ? null : new Object[] {
               connector.getName(),
               connector.getFactoryClassName(),
               connector.getParams()
            }
         };
      }

      return null;
   }

   @Override
   public String getTargetAsJSON(String key) {
      Target target = balancer.getTarget(key);

      if (target != null) {
         TransportConfiguration connector = target.getConnector();

         return JsonLoader.createObjectBuilder()
            .add("nodeID", target.getNodeID())
            .add("local", target.isLocal())
            .add("connector", connector == null ? null : connector.toJson()).build().toString();
      }

      return null;
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(BrokerBalancerControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(BrokerBalancerControl.class);
   }
}
