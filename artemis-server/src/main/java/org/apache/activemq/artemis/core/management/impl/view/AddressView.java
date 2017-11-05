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
package org.apache.activemq.artemis.core.management.impl.view;

import javax.json.JsonObjectBuilder;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.core.management.impl.view.predicate.AddressFilterPredicate;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.utils.JsonLoader;
import org.jboss.logging.Logger;

public class AddressView extends ActiveMQAbstractView<AddressControl> {

   private static final Logger logger = Logger.getLogger(AddressView.class);

   private static final String defaultSortColumn = "creationTime";

   private final ActiveMQServer server;

   public AddressView(ActiveMQServer server) {
      super();
      this.server = server;
      this.predicate = new AddressFilterPredicate(server);
   }

   @Override
   public Class getClassT() {
      return AddressInfo.class;
   }

   @Override
   public JsonObjectBuilder toJson(AddressControl addressControl) {

      AddressInfo address = server.getAddressInfo(new SimpleString(addressControl.getAddress()));
      JsonObjectBuilder obj = JsonLoader.createObjectBuilder().add("id", toString(address.getId())).add("name", addressControl.getAddress()).add("routingTypes", toString(address.getRoutingTypes()));

      try {
         obj.add("queueCount", toString(addressControl.getQueueNames().length));
      } catch (Exception e) {
         if (logger.isDebugEnabled()) {
            logger.debug("setting queueCount", e);
         }
         obj.add("queueCount", "0");
      }

      try {
         obj.add("addressMemorySize", toString(addressControl.getAddressSize()));
      } catch (Exception e) {
         if (logger.isDebugEnabled()) {
            logger.debug("setting addressMemorySize", e);
         }
         obj.add("addressMemorySize", "0");
      }

      try {
         obj.add("paging", toString(addressControl.isPaging()));
      } catch (Exception e) {
         if (logger.isDebugEnabled()) {
            logger.debug("setting paging", e);
         }
         obj.add("paging", "false");
      }

      try {
         obj.add("numberOfPages", toString(addressControl.getNumberOfPages()));
      } catch (Exception e) {
         if (logger.isDebugEnabled()) {
            logger.debug("setting numberOfPages", e);
         }
         obj.add("numberOfPages", "0");
      }

      try {
         obj.add("numberOfBytesPerPage", toString(addressControl.getNumberOfBytesPerPage()));
      } catch (Exception e) {
         if (logger.isDebugEnabled()) {
            logger.debug("setting numberOfBytesPerPage", e);
         }
         obj.add("numberOfBytesPerPage", "0");
      }

      return obj;
   }

   @Override
   public String getDefaultOrderColumn() {
      return defaultSortColumn;
   }
}
