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
package org.apache.activemq.artemis.api.jms.management;

import java.util.Map;

import javax.management.MBeanOperationInfo;

import org.apache.activemq.artemis.api.core.management.Operation;
import org.apache.activemq.artemis.api.core.management.Parameter;

/**
 * A JMSQueueControl is used to manage a JMS queue.
 */
public interface JMSQueueControl extends DestinationControl
{
   // Attributes ----------------------------------------------------

   /**
    * Returns the expiry address associated to this queue.
    */
   String getExpiryAddress();

   /**
    * Returns the dead-letter address associated to this queue.
    */
   String getDeadLetterAddress();

   /**
    * Returns the number of scheduled messages in this queue.
    */
   long getScheduledCount();

   /**
    * Returns the number of consumers consuming messages from this queue.
    */
   int getConsumerCount();

   /**
    * returns the selector for the queue
    */
   String getSelector();

   /**
    * Returns the first message on the queue as JSON
    */
   String getFirstMessageAsJSON() throws Exception;

   /**
    * Returns the timestamp of the first message in milliseconds.
    */
   Long getFirstMessageTimestamp() throws Exception;

   /**
    * Returns the age of the first message in milliseconds.
    */
   Long getFirstMessageAge() throws Exception;

   // Operations ----------------------------------------------------

   /**
    * Returns the Registry bindings associated  to this connection factory.
    */
   @Operation(desc = "Returns the list of Registry bindings associated")
   String[] getRegistryBindings();

   /**
    * Add the JNDI binding to this destination
    */
   @Operation(desc = "Adds the queue to another Registry binding")
   void addBinding(@Parameter(name = "binding", desc = "the name of the binding for the registry") String binding) throws Exception;

   /**
    * Lists all the JMS messages in this queue matching the specified filter.
    * <br>
    * 1 Map represents 1 message, keys are the message's properties and headers, values are the corresponding values.
    * <br>
    * Using {@code null} or an empty filter will list <em>all</em> messages from this queue.
    */
   @Operation(desc = "List all messages in the queue which matches the filter", impact = MBeanOperationInfo.INFO)
   Map<String, Object>[] listMessages(@Parameter(name = "filter", desc = "A JMS Message filter") String filter) throws Exception;

   /**
    * Lists all the JMS messages in this queue matching the specified filter using JSON serialization.
    * <br>
    * Using {@code null} or an empty filter will list <em>all</em> messages from this queue.
    */
   @Operation(desc = "List all messages in the queue which matches the filter and return them using JSON", impact = MBeanOperationInfo.INFO)
   String listMessagesAsJSON(@Parameter(name = "filter", desc = "A JMS Message filter (can be empty)") String filter) throws Exception;

   /**
    * Counts the number of messages in this queue matching the specified filter.
    * <br>
    * Using {@code null} or an empty filter will count <em>all</em> messages from this queue.
    */
   @Operation(desc = "Returns the number of the messages in the queue matching the given filter", impact = MBeanOperationInfo.INFO)
   long countMessages(@Parameter(name = "filter", desc = "A JMS message filter (can be empty)") String filter) throws Exception;

   /**
    * Removes the message corresponding to the specified message ID.
    *
    * @return {@code true} if the message was removed, {@code false} else
    */
   @Operation(desc = "Remove the message corresponding to the given messageID", impact = MBeanOperationInfo.ACTION)
   boolean removeMessage(@Parameter(name = "messageID", desc = "A message ID") String messageID) throws Exception;

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
    * Expires all the message corresponding to the specified filter.
    * <br>
    * Using {@code null} or an empty filter will expire <em>all</em> messages from this queue.
    *
    * @return the number of expired messages
    */
   @Operation(desc = "Expire the messages corresponding to the given filter (and returns the number of expired messages)", impact = MBeanOperationInfo.ACTION)
   int expireMessages(@Parameter(name = "filter", desc = "A message filter (can be empty)") String filter) throws Exception;

   /**
    * Expires the message corresponding to the specified message ID.
    *
    * @return {@code true} if the message was expired, {@code false} else
    */
   @Operation(desc = "Expire the message corresponding to the given messageID", impact = MBeanOperationInfo.ACTION)
   boolean expireMessage(@Parameter(name = "messageID", desc = "A message ID") String messageID) throws Exception;

   /**
    * Sends the message corresponding to the specified message ID to this queue's dead letter address.
    *
    * @return {@code true} if the message was sent to the dead letter address, {@code false} else
    */
   @Operation(desc = "Send the message corresponding to the given messageID to this queue's Dead Letter Address", impact = MBeanOperationInfo.ACTION)
   boolean sendMessageToDeadLetterAddress(@Parameter(name = "messageID", desc = "A message ID") String messageID) throws Exception;

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
    * Changes the message's priority corresponding to the specified message ID to the specified priority.
    *
    * @param newPriority between 0 and 9 inclusive.
    *
    * @return {@code true} if the message priority was changed
    */
   @Operation(desc = "Change the priority of the message corresponding to the given messageID", impact = MBeanOperationInfo.ACTION)
   boolean changeMessagePriority(@Parameter(name = "messageID", desc = "A message ID") String messageID,
                                 @Parameter(name = "newPriority", desc = "the new priority (between 0 and 9)") int newPriority) throws Exception;

   /**
    * Changes the priority for all the message corresponding to the specified filter to the specified priority.
    * <br>
    * Using {@code null} or an empty filter will change <em>all</em> messages from this queue.
    *
    * @return the number of changed messages
    */
   @Operation(desc = "Change the priority of the messages corresponding to the given filter", impact = MBeanOperationInfo.ACTION)
   int changeMessagesPriority(@Parameter(name = "filter", desc = "A message filter") String filter,
                              @Parameter(name = "newPriority", desc = "the new priority (between 0 and 9)") int newPriority) throws Exception;
   /**
    * Moves the message corresponding to the specified message ID to the specified other queue.
    *
    * @return {@code true} if the message was moved, {@code false} else
    */
   @Operation(desc = "Move the message corresponding to the given messageID to another queue, ignoring duplicates (rejectDuplicates=false on this case)", impact = MBeanOperationInfo.ACTION)
   boolean moveMessage(@Parameter(name = "messageID", desc = "A message ID") String messageID,
                       @Parameter(name = "otherQueueName", desc = "The name of the queue to move the message to") String otherQueueName) throws Exception;


   /**
    * Moves the message corresponding to the specified message ID to the specified other queue.
    *
    * @return {@code true} if the message was moved, {@code false} else
    */
   @Operation(desc = "Move the message corresponding to the given messageID to another queue", impact = MBeanOperationInfo.ACTION)
   boolean moveMessage(@Parameter(name = "messageID", desc = "A message ID") String messageID,
                       @Parameter(name = "otherQueueName", desc = "The name of the queue to move the message to") String otherQueueName,
                       @Parameter(name = "rejectDuplicates", desc = "Reject messages identified as duplicate by the duplicate message") boolean rejectDuplicates) throws Exception;

   /**
    * Moves all the message corresponding to the specified filter  to the specified other queue.
    * RejectDuplicates=false on this case
    * <br>
    * Using {@code null} or an empty filter will move <em>all</em> messages from this queue.
    *
    * @return the number of moved messages
    */
   @Operation(desc = "Move the messages corresponding to the given filter (and returns the number of moved messages). rejectDuplicates=false on this case", impact = MBeanOperationInfo.ACTION)
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
   @Operation(desc = "List the message counters history as HTML", impact = MBeanOperationInfo.INFO)
   String listMessageCounterHistoryAsHTML() throws Exception;

   /**
    * Pauses the queue. Messages are no longer delivered to its consumers.
    */
   @Operation(desc = "Pause the queue.", impact = MBeanOperationInfo.ACTION)
   void pause() throws Exception;

   /**
    * Returns whether the queue is paused.
    */
   @Operation(desc = "Returns true if the queue is paused.", impact = MBeanOperationInfo.INFO)
   boolean isPaused() throws Exception;

   /**
    * Resumes the queue. Messages are again delivered to its consumers.
    */
   @Operation(desc = "Resume the queue.", impact = MBeanOperationInfo.ACTION)
   void resume() throws Exception;

   @Operation(desc = "List all the existent consumers on the Queue")
   String listConsumersAsJSON() throws Exception;

   /**
    * it will flush one cycle on internal executors, so you would be sure that any pending tasks are done before you call
    * any other measure.
    * It is useful if you need the exact number of counts on a message
    */
   void flushExecutor();

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

}
