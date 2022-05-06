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

import javax.management.MBeanOperationInfo;
import java.util.Map;

/**
 * An AddressControl is used to manage an address.
 */
public interface AddressControl {
   String ROUTED_MESSAGE_COUNT_DESCRIPTION = "number of messages routed to one or more bindings";
   String UNROUTED_MESSAGE_COUNT_DESCRIPTION = "number of messages not routed to any bindings";
   String ADDRESS_SIZE_DESCRIPTION = "the number of estimated bytes being used by all the queue(s) bound to this address; used to control paging and blocking";
   String NUMBER_OF_PAGES_DESCRIPTION = "number of pages used by this address";

   /**
    * Returns the managed address.
    */
   @Attribute(desc = "managed address")
   String getAddress();

   /*
   * Whether multicast routing is enabled for this address
   * */
   @Attribute(desc = "Get the routing types enabled on this address")
   String[] getRoutingTypes();

   /*
   * Whether multicast routing is enabled for this address
   * */
   @Attribute(desc = "Get the routing types enabled on this address as JSON")
   String getRoutingTypesAsJSON() throws Exception;

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
    * Returns the number of estimated bytes being used by all the queue(s) bound to this address; used to control paging and blocking.
    */
   @Attribute(desc = ADDRESS_SIZE_DESCRIPTION)
   long getAddressSize();

   @Operation(desc = "Schedule Page Cleanup on this address")
   void schedulePageCleanup() throws Exception;

   /**
    * Returns the sum of messages on queue(s), including messages in delivery.
    */
   @Attribute(desc = "the sum of messages on queue(s), including messages in delivery")
   long getNumberOfMessages() throws Exception;

   /**
    * Returns the names of the remote queue(s) bound to this address.
    */
   @Attribute(desc = "names of the remote queue(s) bound to this address")
   String[] getRemoteQueueNames() throws Exception;

   /**
    * Returns the names of the local queue(s) bound to this address.
    */
   @Attribute(desc = "names of the local queue(s) bound to this address")
   String[] getQueueNames() throws Exception;

   /**
    * Returns the names of both the local and remote queue(s) bound to this address.
    */
   @Attribute(desc = "names of both the local & remote queue(s) bound to this address")
   String[] getAllQueueNames() throws Exception;

   /**
    * Returns the number of pages used by this address.
    */
   @Attribute(desc = NUMBER_OF_PAGES_DESCRIPTION)
   int getNumberOfPages();

   /**
    * Returns whether this address is paging.
    *
    * @throws Exception
    */
   @Attribute(desc = "whether this address is paging")
   boolean isPaging() throws Exception;

   /**
    * Returns the % of memory limit that is currently in use
    *
    * @throws Exception
    */
   @Attribute(desc = "the % of memory limit (global or local) that is in use by this address")
   int getAddressLimitPercent() throws Exception;

   /**
    * Blocks message production to this address by limiting credit
    * @return true if production is blocked
    * @throws Exception
    */
   @Operation(desc = "Stops message production to this address, typically with flow control.", impact = MBeanOperationInfo.ACTION)
   boolean block() throws Exception;

   @Operation(desc = "Resumes message production to this address, if previously blocked.", impact = MBeanOperationInfo.ACTION)
   void unblock() throws Exception;

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

   @Attribute(desc = "number of messages currently in all queues bound to this address (includes scheduled, paged, and in-delivery messages)")
   long getMessageCount();

   /**
    * Returns the number of messages routed to one or more bindings
    */
   @Attribute(desc = ROUTED_MESSAGE_COUNT_DESCRIPTION)
   long getRoutedMessageCount();

   /**
    * Returns the number of messages not routed to any bindings
    */
   @Attribute(desc = UNROUTED_MESSAGE_COUNT_DESCRIPTION)
   long getUnRoutedMessageCount();


   /**
    * @param headers  the message headers and properties to set. Can only
    *                 container Strings maped to primitive types.
    * @param body     the text to send
    * @param durable
    * @param user
    * @param password @return
    * @throws Exception
    */
   @Operation(desc = "Sends a TextMessage to a password-protected address.", impact = MBeanOperationInfo.ACTION)
   String sendMessage(@Parameter(name = "headers", desc = "The headers to add to the message") Map<String, String> headers,
                      @Parameter(name = "type", desc = "A type for the message") int type,
                      @Parameter(name = "body", desc = "The body (byte[]) of the message encoded as a string using Base64") String body,
                      @Parameter(name = "durable", desc = "Whether the message is durable") boolean durable,
                      @Parameter(name = "user", desc = "The user to authenticate with") String user,
                      @Parameter(name = "password", desc = "The users password to authenticate with") String password) throws Exception;

   /**
    * @param headers  the message headers and properties to set. Can only
    *                 container Strings maped to primitive types.
    * @param body     the text to send
    * @param durable
    * @param user
    * @param password @return
    * @param createMessageId whether or not to auto generate a Message ID
    * @throws Exception
    */
   @Operation(desc = "Sends a TextMessage to a password-protected address.", impact = MBeanOperationInfo.ACTION)
   String sendMessage(@Parameter(name = "headers", desc = "The headers to add to the message") Map<String, String> headers,
                      @Parameter(name = "type", desc = "A type for the message") int type,
                      @Parameter(name = "body", desc = "The body (byte[]) of the message encoded as a string using Base64") String body,
                      @Parameter(name = "durable", desc = "Whether the message is durable") boolean durable,
                      @Parameter(name = "user", desc = "The user to authenticate with") String user,
                      @Parameter(name = "password", desc = "The users password to authenticate with") String password,
                      @Parameter(name = "createMessageId", desc = "whether or not to auto generate a Message ID") boolean createMessageId) throws Exception;


   /**
    * Pauses all the queues bound to this address. Messages are no longer delivered to all its bounded queues.
    * Newly added queue will be paused too until resume is called.
    * @throws java.lang.Exception
    */
   @Operation(desc = "Pauses the queues bound to this address", impact = MBeanOperationInfo.ACTION)
   void pause() throws Exception;

   /**
    * Pauses all the queues bound to this address. Messages are no longer delivered to all its bounded queues. Newly added queue will be paused too until resume is called.
    * @param persist if true, the pause state will be persisted.
    * @throws java.lang.Exception
    */
   @Operation(desc = "Pauses the queues bound to this address", impact = MBeanOperationInfo.ACTION)
   void pause(@Parameter(name = "persist", desc = "if true, the pause state will be persisted.") boolean persist) throws Exception;

   /**
    * Resume all the queues bound of this address. Messages are delivered again to all its bounded queues.
    * @throws java.lang.Exception
    */
   @Operation(desc = "Resumes the queues bound to this address", impact = MBeanOperationInfo.ACTION)
   void resume() throws Exception;

   @Attribute(desc = "indicates if the queues bound to this address are paused")
   boolean isPaused();

   @Attribute(desc = "whether this address is used for a retroactive address")
   boolean isRetroactiveResource();

   @Attribute(desc = "the size of the duplicate ID cache for this address")
   long getCurrentDuplicateIdCacheSize();

   @Attribute(desc = "clear the duplicate ID cache for this address both from memory and from the journal")
   boolean clearDuplicateIdCache() throws Exception;

   /**
    * Returns whether this address was created automatically in response to client action.
    */
   @Attribute(desc = "whether this address was created automatically in response to client action")
   boolean isAutoCreated();

   /**
    * Returns whether this address was created for the broker's internal use.
    */
   @Attribute(desc = "whether this address was created for the broker's internal use")
   boolean isInternal();

   /**
    * Returns whether this address is temporary.
    */
   @Attribute(desc = "whether this address is temporary")
   boolean isTemporary();

   /**
    * Purge all the queues bound of this address. Returns the total number of messages purged.
    * @throws java.lang.Exception
    */
   @Operation(desc = "Purges the queues bound to this address. Returns the total number of messages purged.", impact = MBeanOperationInfo.ACTION)
   long purge() throws Exception;

   @Operation(desc = "Makes the broker to read messages from the retention folder matching the address and filter.", impact = MBeanOperationInfo.ACTION)
   void replay(@Parameter(name = "target", desc = "Where the replay data should be sent") String target,
               @Parameter(name = "filter", desc = "Filter to apply on message selection. Null means everything matching the address") String filter) throws Exception;

   @Operation(desc = "Makes the broker to read messages from the retention folder matching the address and filter.", impact = MBeanOperationInfo.ACTION)
   void replay(@Parameter(name = "startScanDate", desc = "Start date where we will start scanning for journals to replay. Format YYYYMMDDHHMMSS") String startScan,
               @Parameter(name = "endScanDate", desc = "Finish date where we will stop scannning for journals to replay. Format YYYYMMDDHHMMSS") String endScan,
               @Parameter(name = "target", desc = "Where the replay data should be sent") String target,
               @Parameter(name = "filter", desc = "Filter to apply on message selection. Null means everything matching the address") String filter) throws Exception;
}
