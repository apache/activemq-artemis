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

import javax.management.MBeanOperationInfo;
import javax.management.openmbean.CompositeData;
import java.util.Map;

import org.apache.activemq.artemis.api.core.management.Attribute;
import org.apache.activemq.artemis.api.core.management.Operation;
import org.apache.activemq.artemis.api.core.management.Parameter;

/**
 * A JMSQueueControl is used to manage a JMS queue.
 */
public interface JMSQueueControl extends DestinationControl {
   // Attributes ----------------------------------------------------

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
    * Returns the number of scheduled messages in this queue.
    */
   @Attribute(desc = "number of scheduled messages in this queue")
   long getScheduledCount();

   /**
    * Returns the number of consumers consuming messages from this queue.
    */
   @Attribute(desc = "number of consumers consuming messages from this queue")
   int getConsumerCount();

   /**
    * Returns the number of messages expired from this queue since it was created.
    */
   @Attribute(desc = "the number of messages expired from this queue since it was created")
   long getMessagesExpired();

   /**
    * Returns the number of messages removed from this queue since it was created due to exceeding the max delivery attempts.
    */
   @Attribute(desc = "number of messages removed from this queue since it was created due to exceeding the max delivery attempts")
   long getMessagesKilled();

   /**
    * returns the selector for the queue
    */
   @Attribute(desc = "selector for the queue")
   String getSelector();

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

   // Operations ----------------------------------------------------

   /**
    * Returns the Registry bindings associated with this queue.
    */
   @Attribute(desc = "Returns the list of Registry bindings associated with this queue")
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
   @Override
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
    * Sends a TextMesage to the destination.
    *
    * @param body the text to send
    * @return the message id of the message sent.
    * @throws Exception
    */
   @Operation(desc = "Sends a TextMessage to a password-protected destination.", impact = MBeanOperationInfo.ACTION)
   String sendTextMessage(@Parameter(name = "body") String body) throws Exception;

   /**
    * Sends a TextMessage to the destination.
    *
    * @param properties the message properties to set as a comma sep name=value list. Can only
    *                   contain Strings maped to primitive types or JMS properties. eg: body=hi,JMSReplyTo=Queue2
    * @return the message id of the message sent.
    * @throws Exception
    */
   @Operation(desc = "Sends a TextMessage to a password-protected destination.", impact = MBeanOperationInfo.ACTION)
   String sendTextMessageWithProperties(String properties) throws Exception;

   /**
    * Sends a TextMesage to the destination.
    *
    * @param headers the message headers and properties to set. Can only
    *                container Strings maped to primitive types.
    * @param body    the text to send
    * @return the message id of the message sent.
    * @throws Exception
    */
   @Operation(desc = "Sends a TextMessage to a password-protected destination.", impact = MBeanOperationInfo.ACTION)
   String sendTextMessage(@Parameter(name = "headers") Map<String, String> headers,
                          @Parameter(name = "body") String body) throws Exception;

   /**
    * Sends a TextMesage to the destination.
    *
    * @param body     the text to send
    * @param user
    * @param password
    * @return
    * @throws Exception
    */
   @Operation(desc = "Sends a TextMessage to a password-protected destination.", impact = MBeanOperationInfo.ACTION)
   String sendTextMessage(@Parameter(name = "body") String body,
                          @Parameter(name = "user") String user,
                          @Parameter(name = "password") String password) throws Exception;

   /**
    * @param headers  the message headers and properties to set. Can only
    *                 container Strings maped to primitive types.
    * @param body     the text to send
    * @param user
    * @param password
    * @return
    * @throws Exception
    */
   @Operation(desc = "Sends a TextMessage to a password-protected destination.", impact = MBeanOperationInfo.ACTION)
   String sendTextMessage(@Parameter(name = "headers") Map<String, String> headers,
                          @Parameter(name = "body") String body,
                          @Parameter(name = "user") String user,
                          @Parameter(name = "password") String password) throws Exception;

   /**
    * Changes the message's priority corresponding to the specified message ID to the specified priority.
    *
    * @param newPriority between 0 and 9 inclusive.
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
    * Retries the message corresponding to the given messageID to the original queue.
    * This is appropriate on dead messages on Dead letter queues only.
    *
    * @param messageID
    * @return {@code true} if the message was retried, {@code false} else
    * @throws Exception
    */
   @Operation(desc = "Retry the message corresponding to the given messageID to the original queue", impact = MBeanOperationInfo.ACTION)
   boolean retryMessage(@Parameter(name = "messageID", desc = "A message ID") String messageID) throws Exception;

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
   @Attribute(desc = "Returns true if the queue is paused.")
   boolean isPaused() throws Exception;

   /**
    * Resumes the queue. Messages are again delivered to its consumers.
    */
   @Operation(desc = "Resume the queue.", impact = MBeanOperationInfo.ACTION)
   void resume() throws Exception;

   /**
    * Resumes the queue. Messages are again delivered to its consumers.
    */
   @Operation(desc = "Browse the queue.", impact = MBeanOperationInfo.ACTION)
   CompositeData[] browse() throws Exception;

   /**
    * Resumes the queue. Messages are again delivered to its consumers.
    */
   @Operation(desc = "Browse the queue.", impact = MBeanOperationInfo.ACTION)
   CompositeData[] browse(String filter) throws Exception;

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
