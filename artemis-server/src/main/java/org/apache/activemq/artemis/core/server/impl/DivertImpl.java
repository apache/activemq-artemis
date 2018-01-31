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
package org.apache.activemq.artemis.core.server.impl;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.Divert;
import org.apache.activemq.artemis.core.server.DivertConfigurationRoutingType;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.transformer.Transformer;
import org.jboss.logging.Logger;

/**
 * A DivertImpl simply diverts a message to a different forwardAddress
 */
public class DivertImpl implements Divert {

   private static final Logger logger = Logger.getLogger(DivertImpl.class);

   private final PostOffice postOffice;

   private final SimpleString forwardAddress;

   private final SimpleString uniqueName;

   private final SimpleString routingName;

   private final boolean exclusive;

   private final Filter filter;

   private final Transformer transformer;

   private final StorageManager storageManager;

   private final DivertConfigurationRoutingType routingType;

   public DivertImpl(final SimpleString forwardAddress,
                     final SimpleString uniqueName,
                     final SimpleString routingName,
                     final boolean exclusive,
                     final Filter filter,
                     final Transformer transformer,
                     final PostOffice postOffice,
                     final StorageManager storageManager,
                     final DivertConfigurationRoutingType routingType) {
      this.forwardAddress = forwardAddress;

      this.uniqueName = uniqueName;

      this.routingName = routingName;

      this.exclusive = exclusive;

      this.filter = filter;

      this.transformer = transformer;

      this.postOffice = postOffice;

      this.storageManager = storageManager;

      this.routingType = routingType;
   }

   @Override
   public void route(final Message message, final RoutingContext context) throws Exception {
      // We must make a copy of the message, otherwise things like returning credits to the page won't work
      // properly on ack, since the original address will be overwritten

      if (logger.isTraceEnabled()) {
         logger.trace("Diverting message " + message + " into " + this);
      }

      Message copy = null;

      // Shouldn't copy if it's not routed anywhere else
      if (!forwardAddress.equals(context.getAddress(message))) {
         long id = storageManager.generateID();
         copy = message.copy(id);

         // This will set the original MessageId, and the original address
         copy.referenceOriginalMessage(message, this.getUniqueName().toString());

         copy.setAddress(forwardAddress);

         copy.setExpiration(message.getExpiration());

         copy.reencode();

         switch (routingType) {
            case ANYCAST:
               copy.setRoutingType(RoutingType.ANYCAST);
               break;
            case MULTICAST:
               copy.setRoutingType(RoutingType.MULTICAST);
               break;
            case STRIP:
               copy.setRoutingType(null);
               break;
            case PASS:
               break;
         }

         if (transformer != null) {
            copy = transformer.transform(copy);
         }
      } else {
         copy = message;
      }

      postOffice.route(copy, context.getTransaction(), false);
   }

   @Override
   public void routeWithAck(Message message, RoutingContext context) throws Exception {
      route(message, context);
   }

   @Override
   public SimpleString getRoutingName() {
      return routingName;
   }

   @Override
   public SimpleString getUniqueName() {
      return uniqueName;
   }

   @Override
   public boolean isExclusive() {
      return exclusive;
   }

   @Override
   public Filter getFilter() {
      return filter;
   }

   @Override
   public Transformer getTransformer() {
      return transformer;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString() {
      return "DivertImpl [routingName=" + routingName +
         ", uniqueName=" +
         uniqueName +
         ", forwardAddress=" +
         forwardAddress +
         ", exclusive=" +
         exclusive +
         ", filter=" +
         filter +
         ", transformer=" +
         transformer +
         "]";
   }

}
