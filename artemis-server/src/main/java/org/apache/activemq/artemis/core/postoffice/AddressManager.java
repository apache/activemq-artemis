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
package org.apache.activemq.artemis.core.postoffice;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.transaction.Transaction;

/**
 * Used to maintain addresses and BindingsImpl.
 */
public interface AddressManager {

   boolean addBinding(Binding binding) throws Exception;

   /**
    * This will use a Transaction as we need to confirm the queue was removed
    *
    * @param uniqueName
    * @param tx
    * @return
    * @throws Exception
    */
   Binding removeBinding(SimpleString uniqueName, Transaction tx) throws Exception;

   Bindings getBindingsForRoutingAddress(SimpleString address) throws Exception;

   Bindings getMatchingBindings(SimpleString address) throws Exception;

   SimpleString getMatchingQueue(SimpleString address, RoutingType routingType) throws Exception;

   SimpleString getMatchingQueue(SimpleString address, SimpleString queueName, RoutingType routingType) throws Exception;

   void clear();

   Binding getBinding(SimpleString queueName);

   Map<SimpleString, Binding> getBindings();

   Set<SimpleString> getAddresses();

   /**
    * @param addressInfo
    * @return true if the address was added, false if it wasn't added
    */
   boolean addAddressInfo(AddressInfo addressInfo);

   AddressInfo updateAddressInfo(SimpleString addressName,
                                 Collection<RoutingType> routingTypes) throws Exception;

   /**
    * @param addressInfo
    * @return the same provided {@code addressInfo} if the address was added, another if it was updated
    */
   AddressInfo addOrUpdateAddressInfo(AddressInfo addressInfo);

   AddressInfo removeAddressInfo(SimpleString address);

   AddressInfo getAddressInfo(SimpleString address);

}
