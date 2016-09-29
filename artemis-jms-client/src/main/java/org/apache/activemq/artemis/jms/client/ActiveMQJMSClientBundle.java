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
package org.apache.activemq.artemis.jms.client;

import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.ActiveMQInvalidFilterExpressionException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Logger Code 12
 *
 * each message id must be 6 digits long starting with 10, the 3rd digit should be 9
 *
 * so 129000 to 129999
 */
@MessageBundle(projectCode = "AMQ")
public interface ActiveMQJMSClientBundle {

   ActiveMQJMSClientBundle BUNDLE = Messages.getBundle(ActiveMQJMSClientBundle.class);

   @Message(id = 129000, value = "Invalid filter: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInvalidFilterExpressionException invalidFilter(@Cause Throwable e, SimpleString filter);

   @Message(id = 129001, value = "Invalid Subscription Name. It is required to set the subscription name")
   ActiveMQIllegalStateException invalidSubscriptionName();

   @Message(id = 129002, value = "Destination {0} does not exist", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQNonExistentQueueException destinationDoesNotExist(SimpleString destination);

   @Message(id = 129003, value = "name cannot be null")
   IllegalArgumentException nameCannotBeNull();

   @Message(id = 129004, value = "name cannot be empty")
   IllegalArgumentException nameCannotBeEmpty();

   @Message(id = 129005, value = "It is illegal to call this method from within a Message Listener")
   IllegalStateRuntimeException callingMethodFromListenerRuntime();

   @Message(id = 129006, value = "It is illegal to call this method from within a Message Listener")
   IllegalStateException callingMethodFromListener();

   @Message(id = 129007, value = "It is illegal to call this method from within a Completion Listener")
   IllegalStateRuntimeException callingMethodFromCompletionListenerRuntime();

   @Message(id = 129008, value = "It is illegal to call this method from within a Completion Listener")
   IllegalStateException callingMethodFromCompletionListener();

   @Message(id = 129009, value = "Null {0} is not allowed", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nullArgumentNotAllowed(String type);

   @Message(id = 129010, value = "Topic (Destination) cannot be null")
   InvalidDestinationException nullTopic();

   @Message(id = 129011, value = "LargeMessage streaming is only possible on ByteMessage or StreamMessage")
   IllegalStateException onlyValidForByteOrStreamMessages();

   @Message(id = 129012, value = "The property name ''{0}'' is not a valid java identifier.",
      format = Message.Format.MESSAGE_FORMAT)
   JMSRuntimeException invalidJavaIdentifier(String propertyName);

   @Message(id = 129013, value = "Message is read-only")
   MessageNotWriteableException messageNotWritable();

   @Message(id = 129014, value = "Message is write-only")
   MessageNotReadableException messageNotReadable();

   @Message(id = 129015, value = "Illegal deliveryMode value: {0}", format = Message.Format.MESSAGE_FORMAT)
   JMSException illegalDeliveryMode(int deliveryMode);
}
