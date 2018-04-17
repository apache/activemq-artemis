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
import javax.management.openmbean.CompositeData;
import java.util.Map;

/**
 * A QueueControl is used to manage a queue.
 */
public interface QueueControl {
   // Attributes ----------------------------------------------------

   /**
    * Returns the name of this queue.
    */
   @Attribute(desc = "name of this queue")
   String getName();

   /**
    * Returns the address this queue is bound to.
    */
   @Attribute(desc = "address this queue is bound to")
   String getAddress();

   /**
    * Returns this queue ID.
    */
   @Attribute(desc = "ID of this queue")
   long getID();

   /**
    * Returns whether this queue is temporary.
    */
   @Attribute(desc = "whether this queue is temporary")
   boolean isTemporary();

   /**
    * Returns whether this queue is durable.
    */
   @Attribute(desc = "whether this queue is durable")
   boolean isDurable();

   /**
    * Returns the user that is associated with creating the queue.
    */
   @Attribute(desc = "the user that created the queue")
   String getUser();

   /**
    * The routing type of this queue.
    */
   @Attribute(desc = "routing type of this queue")
   String getRoutingType();

   /**
    * Returns the filter associated with this queue.
    */
   @Attribute(desc = "filter associated with this queue")
   String getFilter();

   /**
    * Returns the number of messages currently in this queue.
    */
   @Attribute(desc = "number of messages currently in this queue (includes scheduled, paged, and in-delivery messages)")
   long getMessageCount();

   /**
    * Returns the persistent size of all messages currently in this queue. The persistent size of a message
    * is the amount of space the message would take up on disk which is used to track how much data there
    * is to consume on this queue
    */
   @Attribute(desc = "persistent size of all messages (including durable and non-durable) currently in this queue (includes scheduled, paged, and in-delivery messages)")
   long getPersistentSize();

   /**
    * Returns the number of durable messages currently in this queue.
    */
   @Attribute(desc = "number of durable messages currently in this queue (includes scheduled, paged, and in-delivery messages)")
   long getDurableMessageCount();

   /**
    * Returns the persistent size of durable messages currently in this queue. The persistent size of a message
    * is the amount of space the message would take up on disk which is used to track how much data there
    * is to consume on this queue
    */
   @Attribute(desc = "persistent size of durable messages currently in this queue (includes scheduled, paged, and in-delivery messages)")
   long getDurablePersistentSize();

   /**
    * Returns the number of scheduled messages in this queue.
    */
   @Attribute(desc = "number of scheduled messages in this queue")
   long getScheduledCount();

   /**
    * Returns the size of scheduled messages in this queue.
    */
   @Attribute(desc = "persistent size of scheduled messages in this queue")
   long getScheduledSize();

   /**
    * Returns the number of durable scheduled messages in this queue.
    */
   @Attribute(desc = "number of durable scheduled messages in this queue")
   long getDurableScheduledCount();

   /**
    * Returns the size of durable scheduled messages in this queue.
    */
   @Attribute(desc = "persistent size of durable scheduled messages in this queue")
   long getDurableScheduledSize();

   /**
    * Returns the number of consumers consuming messages from this queue.
    */
   @Attribute(desc = "number of consumers consuming messages from this queue")
   int getConsumerCount();

   /**
    * Returns the number of messages that this queue is currently delivering to its consumers.
    */
   @Attribute(desc = "number of messages that this queue is currently delivering to its consumers")
   int getDeliveringCount();

   /**
    * Returns the persistent size of messages that this queue is currently delivering to its consumers.
    */
   @Attribute(desc = "persistent size of messages that this queue is currently delivering to its consumers")
   long getDeliveringSize();

   /**
    * Returns the number of durable messages that this queue is currently delivering to its consumers.
    */
   @Attribute(desc = "number of durable messages that this queue is currently delivering to its consumers")
   int getDurableDeliveringCount();

   /**
    * Returns the size of durable messages that this queue is currently delivering to its consumers.
    */
   @Attribute(desc = "persistent size of durable messages that this queue is currently delivering to its consumers")
   long getDurableDeliveringSize();

   /**
    * Returns the number of messages added to this queue since it was created.
    */
   @Attribute(desc = "number of messages added to this queue since it was created")
   long getMessagesAdded();

   /**
    * Returns the number of messages added to this queue since it was created.
    */
   @Attribute(desc = "number of messages acknowledged from this queue since it was created")
   long getMessagesAcknowledged();

   /**
    * Returns the number of messages expired from this queue since it was created.
    */
   @Attribute(desc = "number of messages expired from this queue since it was created")
   long getMessagesExpired();

   /**
    * Returns the number of messages removed from this queue since it was created due to exceeding the max delivery attempts.
    */
   @Attribute(desc = "number of messages removed from this queue since it was created due to exceeding the max delivery attempts")
   long getMessagesKilled();

   /**
    * Returns the first message on the queue as JSON
    */
   @Attribute(desc = "first message on the queue as JSON")
   String getFirstMessageAsJSON() throws Exception;

   /**
    * Returns the timestamp of the first message in milliseconds.
    */
   @Attribute(desc = "timestamp of the first message in milliseconds")
   Long getFirstMessageTimestamp() throws Exception;

   /**
    * Returns the age of the first message in milliseconds.
    */
   @Attribute(desc = "age of the first message in milliseconds")
   Long getFirstMessageAge() throws Exception;

   /**
    * Returns the expiry address associated with this queue.
    */
   @Attribute(desc = "expiry address associated with this queue")
   String getExpiryAddress();

   /**
    * Returns the dead-letter address associated with this queue.
    */
   @Attribute(desc = "dead-letter address associated with this queue")
   String getDeadLetterAddress();

   /**
    *
    */
   @Attribute(desc = "maximum number of consumers allowed on this queue at any one time")
   int getMaxConsumers();

   /**
    *
    */
   @Attribute(desc = "delete this queue when the last consumer disconnects")
   boolean isPurgeOnNoConsumers();

   /**
    *
    */
   @Attribute(desc = "If the queue should route exclusively to one consumer")
   boolean isExclusive();

   /**
    *
    */
   @Attribute(desc = "is this queue a last value queue")
   boolean isLastValue();

   // Operations ----------------------------------------------------

   /**
    * Lists all the messages scheduled for delivery for this queue.
    * <br>
    * 1 Map represents 1 message, keys are the message's properties and headers, values are the corresponding values.
    */
   @Operation(desc = "List the messages scheduled for delivery", impact = MBeanOperationInfo.INFO)
   Map<String, Object>[] listScheduledMessages() throws Exception;

   /**
    * Lists all the messages scheduled for delivery for this queue using JSON serialization.
    */
   @Operation(desc = "List the messages scheduled for delivery and returns them using JSON", impact = MBeanOperationInfo.INFO)
   String listScheduledMessagesAsJSON() throws Exception;

   /**
    * Lists all the messages being deliver per consumer.
    * <br>
    * The Map's key is a toString representation for the consumer. Each consumer will then return a {@code Map<String,Object>[]} same way is returned by {@link #listScheduledMessages()}
    */
   @Operation(desc = "List all messages being delivered per consumer")
   Map<String, Map<String, Object>[]> listDeliveringMessages() throws Exception;

   /**
    * Executes a conversion of {@link #listDeliveringMessages()} to JSON
    *
    * @return
    * @throws Exception
    */
   @Operation(desc = "list all messages being delivered per consumer using JSON form")
   String listDeliveringMessagesAsJSON() throws Exception;

   /**
    * Lists all the messages in this queue matching the specified filter.
    * <br>
    * 1 Map represents 1 message, keys are the message's properties and headers, values are the corresponding values.
    * <br>
    * Using {@code null} or an empty filter will list <em>all</em> messages from this queue.
    */
   @Operation(desc = "List all the messages in the queue matching the given filter", impact = MBeanOperationInfo.INFO)
   Map<String, Object>[] listMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   /**
    * Lists all the messages in this queue matching the specified filter using JSON serialization.
    * <br>
    * Using {@code null} or an empty filter will list <em>all</em> messages from this queue.
    */
   @Operation(desc = "List all the messages in the queue matching the given filter and returns them using JSON", impact = MBeanOperationInfo.INFO)
   String listMessagesAsJSON(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   /**
    * Counts the number of messages in this queue matching the specified filter.
    * <br>
    * Using {@code null} or an empty filter will count <em>all</em> messages from this queue.
    */
   @Operation(desc = "Returns the number of the messages in the queue matching the given filter", impact = MBeanOperationInfo.INFO)
   long countMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Returns the number of the messages in the queue", impact = MBeanOperationInfo.INFO)
   long countMessages() throws Exception;

   /**
    * Removes the message corresponding to the specified message ID.
    *
    * @return {@code true} if the message was removed, {@code false} else
    */
   @Operation(desc = "Remove the message corresponding to the given messageID", impact = MBeanOperationInfo.ACTION)
   boolean removeMessage(@Parameter(name = "messageID", desc = "A message ID") long messageID) throws Exception;

   /**
    * Removes all the message corresponding to the specified filter.
    * <br>
    * Using {@code null} or an empty filter will remove <em>all</em> messages from this queue.
    *
    * @return the number of removed messages
    */
   @Operation(desc = "Remove the messages corresponding to the given filter (and returns the number of removed messages)", impact = MBeanOperationInfo.ACTION)
   int removeMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   /**
    * Removes all the message corresponding to the specified filter.
    * <br>
    * Using {@code null} or an empty filter will remove <em>all</em> messages from this queue.
    *
    * @return the number of removed messages
    */
   @Operation(desc = "Remove the messages corresponding to the given filter (and returns the number of removed messages)", impact = MBeanOperationInfo.ACTION)
   int removeMessages(@Parameter(name = "flushLimit", desc = "Limit to flush transactions during the operation to avoid OutOfMemory") int flushLimit,
                      @Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   /**
    * Removes all the message from the queue.
    *
    * @return the number of removed messages
    */
   @Operation(desc = "Remove all the messages from the Queue (and returns the number of removed messages)", impact = MBeanOperationInfo.ACTION)
   int removeAllMessages() throws Exception;

   /**
    * Expires all the message corresponding to the specified filter.
    * <br>
    * Using {@code null} or an empty filter will expire <em>all</em> messages from this queue.
    *
    * @return the number of expired messages
    */
   @Operation(desc = "Expire the messages corresponding to the given filter (and returns the number of expired messages)", impact = MBeanOperationInfo.ACTION)
   int expireMessages(@Parameter(name = "filter", desc = "A message filter") String filter) throws Exception;

   /**
    * Expires the message corresponding to the specified message ID.
    *
    * @return {@code true} if the message was expired, {@code false} else
    */
   @Operation(desc = "Remove the message corresponding to the given messageID", impact = MBeanOperationInfo.ACTION)
   boolean expireMessage(@Parameter(name = "messageID", desc = "A message ID") long messageID) throws Exception;

   /**
    * Retries the message corresponding to the given messageID to the original queue.
    * This is appropriate on dead messages on Dead letter queues only.
    *
    * @param messageID
    * @return {@code true} if the message was retried, {@code false} else
    * @throws Exception
    */
   @Operation(desc = "Retry the message corresponding to the given messageID to the original queue", impact = MBeanOperationInfo.ACTION)
   boolean retryMessage(@Parameter(name = "messageID", desc = "A message ID") long messageID) throws Exception;

   /**
    * Retries all messages on a DLQ to their respective original queues.
    * This is appropriate on dead messages on Dead letter queues only.
    *
    * @return the number of retried messages.
    * @throws Exception
    */
   @Operation(desc = "Retry all messages on a DLQ to their respective original queues", impact = MBeanOperationInfo.ACTION)
   int retryMessages() throws Exception;

   /**
    * Moves the message corresponding to the specified message ID to the specified other queue.
    *
    * @return {@code true} if the message was moved, {@code false} else
    */
   @Operation(desc = "Move the message corresponding to the given messageID to another queue. rejectDuplicate=false on this case", impact = MBeanOperationInfo.ACTION)
   boolean moveMessage(@Parameter(name = "messageID", desc = "A message ID") long messageID,
                       @Parameter(name = "otherQueueName", desc = "The name of the queue to move the message to") String otherQueueName) throws Exception;

   /**
    * Moves the message corresponding to the specified message ID to the specified other queue.
    *
    * @return {@code true} if the message was moved, {@code false} else
    */
   @Operation(desc = "Move the message corresponding to the given messageID to another queue", impact = MBeanOperationInfo.ACTION)
   boolean moveMessage(@Parameter(name = "messageID", desc = "A message ID") long messageID,
                       @Parameter(name = "otherQueueName", desc = "The name of the queue to move the message to") String otherQueueName,
                       @Parameter(name = "rejectDuplicates", desc = "Reject messages identified as duplicate by the duplicate message") boolean rejectDuplicates) throws Exception;

   /**
    * Moves all the message corresponding to the specified filter  to the specified other queue.
    * RejectDuplicates = false on this case
    * <br>
    * Using {@code null} or an empty filter will move <em>all</em> messages from this queue.
    *
    * @return the number of moved messages
    */
   @Operation(desc = "Move the messages corresponding to the given filter (and returns the number of moved messages). RejectDuplicates=false on this case.", impact = MBeanOperationInfo.ACTION)
   int moveMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter,
                    @Parameter(name = "otherQueueName", desc = "The name of the queue to move the messages to") String otherQueueName) throws Exception;

   /**
    * Moves all the message corresponding to the specified filter  to the specified other queue.
    * <br>
    * Using {@code null} or an empty filter will move <em>all</em> messages from this queue.
    *
    * @return the number of moved messages
    */
   @Operation(desc = "Move the messages corresponding to the given filter (and returns the number of moved messages)", impact = MBeanOperationInfo.ACTION)
   int moveMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter,
                    @Parameter(name = "otherQueueName", desc = "The name of the queue to move the messages to") String otherQueueName,
                    @Parameter(name = "rejectDuplicates", desc = "Reject messages identified as duplicate by the duplicate message") boolean rejectDuplicates) throws Exception;

   @Operation(desc = "Move the messages corresponding to the given filter (and returns the number of moved messages)", impact = MBeanOperationInfo.ACTION)
   int moveMessages(@Parameter(name = "flushLimit", desc = "Limit to flush transactions during the operation to avoid OutOfMemory") int flushLimit,
                    @Parameter(name = "filter", desc = "A message filter (can be empty)") String filter,
                    @Parameter(name = "otherQueueName", desc = "The name of the queue to move the messages to") String otherQueueName,
                    @Parameter(name = "rejectDuplicates", desc = "Reject messages identified as duplicate by the duplicate message") boolean rejectDuplicates) throws Exception;

   /**
    * Sends the message corresponding to the specified message ID to this queue's dead letter address.
    *
    * @return {@code true} if the message was sent to the dead letter address, {@code false} else
    */
   @Operation(desc = "Send the message corresponding to the given messageID to this queue's Dead Letter Address", impact = MBeanOperationInfo.ACTION)
   boolean sendMessageToDeadLetterAddress(@Parameter(name = "messageID", desc = "A message ID") long messageID) throws Exception;

   /**
    * Sends all the message corresponding to the specified filter to this queue's dead letter address.
    * <br>
    * Using {@code null} or an empty filter will send <em>all</em> messages from this queue.
    *
    * @return the number of sent messages
    */
   @Operation(desc = "Send the messages corresponding to the given filter to this queue's Dead Letter Address", impact = MBeanOperationInfo.ACTION)
   int sendMessagesToDeadLetterAddress(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filterStr) throws Exception;

   /**
    * @param headers  the message headers and properties to set. Can only
    *                 container Strings maped to primitive types.
    * @param body     the text to send
    * @param durable
    * @param user
    * @param password @return
    * @throws Exception
    */
   @Operation(desc = "Sends a TextMessage to a password-protected destination.", impact = MBeanOperationInfo.ACTION)
   String sendMessage(@Parameter(name = "headers", desc = "The headers to add to the message") Map<String, String> headers,
                      @Parameter(name = "type", desc = "A type for the message") int type,
                      @Parameter(name = "body", desc = "The body (byte[]) of the message encoded as a string using Base64") String body,
                      @Parameter(name = "durable", desc = "Whether the message is durable") boolean durable,
                      @Parameter(name = "user", desc = "The user to authenticate with") String user,
                      @Parameter(name = "password", desc = "The users password to authenticate with") String password) throws Exception;

   /**
    * Changes the message's priority corresponding to the specified message ID to the specified priority.
    *
    * @param newPriority between 0 and 9 inclusive.
    * @return {@code true} if the message priority was changed
    */
   @Operation(desc = "Change the priority of the message corresponding to the given messageID", impact = MBeanOperationInfo.ACTION)
   boolean changeMessagePriority(@Parameter(name = "messageID", desc = "A message ID") long messageID,
                                 @Parameter(name = "newPriority", desc = "the new priority (between 0 and 9)") int newPriority) throws Exception;

   /**
    * Changes the priority for all the message corresponding to the specified filter to the specified priority.
    * <br>
    * Using {@code null} or an empty filter will change <em>all</em> messages from this queue.
    *
    * @return the number of changed messages
    */
   @Operation(desc = "Change the priority of the messages corresponding to the given filter", impact = MBeanOperationInfo.ACTION)
   int changeMessagesPriority(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter,
                              @Parameter(name = "newPriority", desc = "the new priority (between 0 and 9)") int newPriority) throws Exception;

   /**
    * Lists the message counter for this queue.
    */
   @Operation(desc = "List the message counters", impact = MBeanOperationInfo.INFO)
   String listMessageCounter() throws Exception;

   /**
    * Resets the message counter for this queue.
    */
   @Operation(desc = "Reset the message counters", impact = MBeanOperationInfo.INFO)
   void resetMessageCounter() throws Exception;

   /**
    * Lists the message counter for this queue as a HTML table.
    */
   @Operation(desc = "List the message counters as HTML", impact = MBeanOperationInfo.INFO)
   String listMessageCounterAsHTML() throws Exception;

   /**
    * Lists the message counter history for this queue.
    */
   @Operation(desc = "List the message counters history", impact = MBeanOperationInfo.INFO)
   String listMessageCounterHistory() throws Exception;

   /**
    * Lists the message counter history for this queue as a HTML table.
    */
   @Operation(desc = "List the message counters history HTML", impact = MBeanOperationInfo.INFO)
   String listMessageCounterHistoryAsHTML() throws Exception;

   /**
    * Pauses the queue. Messages are no longer delivered to its consumers.
    */
   @Operation(desc = "Pauses the Queue", impact = MBeanOperationInfo.ACTION)
   void pause() throws Exception;

   /**
    * Pauses the queue. Messages are no longer delivered to its consumers.
    */
   @Operation(desc = "Pauses the Queue", impact = MBeanOperationInfo.ACTION)
   void pause(@Parameter(name = "persist", desc = "if true, the pause state will be persisted.") boolean persist) throws Exception;

   /**
    * Resumes the queue. Messages are again delivered to its consumers.
    */
   @Operation(desc = "Resumes delivery of queued messages and gets the queue out of paused state. It will also affected the state of a persisted pause.", impact = MBeanOperationInfo.ACTION)
   void resume() throws Exception;

   @Operation(desc = "List all the existent consumers on the Queue")
   String listConsumersAsJSON() throws Exception;

   /**
    * Returns whether the queue is paused.
    */
   @Attribute(desc = "whether the queue is paused")
   boolean isPaused() throws Exception;

   @Operation(desc = "Browse Messages", impact = MBeanOperationInfo.ACTION)
   CompositeData[] browse() throws Exception;

   @Operation(desc = "Browse Messages", impact = MBeanOperationInfo.ACTION)
   CompositeData[] browse(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Browse Messages", impact = MBeanOperationInfo.ACTION)
   CompositeData[] browse(@Parameter(name = "page", desc = "Current page") int page,
                          @Parameter(name = "pageSize", desc = "Page size") int pageSize) throws Exception;

   /**
    * Resets the MessagesAdded property
    */
   @Operation(desc = "Resets the MessagesAdded property", impact = MBeanOperationInfo.ACTION)
   void resetMessagesAdded() throws Exception;

   /**
    * Resets the MessagesAdded property
    */
   @Operation(desc = "Resets the MessagesAcknowledged property", impact = MBeanOperationInfo.ACTION)
   void resetMessagesAcknowledged() throws Exception;

   /**
    * Resets the MessagesExpired property
    */
   @Operation(desc = "Resets the MessagesExpired property", impact = MBeanOperationInfo.ACTION)
   void resetMessagesExpired() throws Exception;

   /**
    * Resets the MessagesExpired property
    */
   @Operation(desc = "Resets the MessagesKilled property", impact = MBeanOperationInfo.ACTION)
   void resetMessagesKilled() throws Exception;

   /**
    * it will flush one cycle on internal executors, so you would be sure that any pending tasks are done before you call
    * any other measure.
    * It is useful if you need the exact number of counts on a message
    */
   @Operation(desc = "Flush internal executors", impact = MBeanOperationInfo.ACTION)
   void flushExecutor();

   /**
    * Will reset the all the groups.
    * This is useful if you want a complete rebalance of the groups to consumers
    */
   @Operation(desc = "Resets all groups", impact = MBeanOperationInfo.ACTION)
   void resetAllGroups();

   /**
    * Will reset the group matching the given groupID.
    * This is useful if you want the given group to be rebalanced to the consumers
    */
   @Operation(desc = "Reset the specified group", impact = MBeanOperationInfo.ACTION)
   void resetGroup(@Parameter(name = "groupID", desc = "ID of group to reset") String groupID);

   /**
    * Will return the current number of active groups.
    */
   @Attribute(desc = "Get the current number of active groups")
   int getGroupCount();


   @Operation(desc = "List all the existent group to consumers mappings on the Queue")
   String listGroupsAsJSON() throws Exception;

}
