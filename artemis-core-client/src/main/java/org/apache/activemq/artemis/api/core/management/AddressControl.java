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
package org.apache.activemq.artemis.api.core.management;

/**
 * An AddressControl is used to manage an address.
 */
public interface AddressControl {

   /**
    * Returns the managed address.
    */
   @Attribute(desc = "managed address")
   String getAddress();

   /**
    * Returns the roles (name and permissions) associated with this address.
    */
   @Attribute(desc = "roles (name and permissions) associated with this address")
   Object[] getRoles() throws Exception;

   /**
    * Returns the roles  (name and permissions) associated with this address
    * using JSON serialization.
    * <br>
    * Java objects can be recreated from JSON serialization using {@link RoleInfo#from(String)}.
    */
   @Attribute(desc = "roles  (name and permissions) associated with this address using JSON serialization")
   String getRolesAsJSON() throws Exception;

   /**
    * Returns the number of estimated bytes being used by the queue(s), used to control paging and blocking.
    */
   @Attribute(desc = "the number of estimated bytes being used by the queue(s), used to control paging and blocking")
   long getAddressSize() throws Exception;

   /**
    * Returns the sum of messages on queue(s), including messages in delivery.
    */
   @Attribute(desc = "the sum of messages on queue(s), including messages in delivery")
   long getNumberOfMessages() throws Exception;

   /**
    * Returns the names of the queues bound to this address.
    */
   @Attribute(desc = "names of the queue(s) bound to this address")
   String[] getQueueNames() throws Exception;

   /**
    * Returns the number of pages used by this address.
    */
   @Attribute(desc = "number of pages used by this address")
   int getNumberOfPages() throws Exception;

   /**
    * Returns whether this address is paging.
    *
    * @throws Exception
    */
   @Attribute(desc = "whether this address is paging")
   boolean isPaging() throws Exception;

   /**
    * Returns the number of bytes used by each page for this address.
    */
   @Attribute(desc = "number of bytes used by each page for this address")
   long getNumberOfBytesPerPage() throws Exception;

   /**
    * Returns the names of all bindings (both queues and diverts) bound to this address
    */
   @Attribute(desc = "names of all bindings (both queues and diverts) bound to this address")
   String[] getBindingNames() throws Exception;
}
