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
package org.apache.activemq.artemis.protocol.amqp.logger;

import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPIllegalStateException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInvalidFieldException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPNotFoundException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPSecurityException;
import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Logger Code 11
 * <p>
 * Each message id must be 6 digits long starting with 11, the 3rd digit should be 9. So the range
 * is from 119000 to 119999.
 * <p>
 * Once released, methods should not be deleted as they may be referenced by knowledge base
 * articles. Unused methods should be marked as deprecated.
 */
@MessageBundle(projectCode = "AMQ")
public interface ActiveMQAMQPProtocolMessageBundle {

   ActiveMQAMQPProtocolMessageBundle BUNDLE = Messages.getBundle(ActiveMQAMQPProtocolMessageBundle.class);

   @Message(id = 119000, value = "target address not set")
   ActiveMQAMQPInvalidFieldException targetAddressNotSet();

   @Message(id = 119001, value = "error creating temporary queue, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPInternalErrorException errorCreatingTemporaryQueue(String message);

   @Message(id = 119002, value = "target address does not exist")
   ActiveMQAMQPNotFoundException addressDoesntExist();

   @Message(id = 119003, value = "error finding temporary queue, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPNotFoundException errorFindingTemporaryQueue(String message);

   @Message(id = 119005, value = "error creating consumer, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPInternalErrorException errorCreatingConsumer(String message);

   @Message(id = 119006, value = "error starting consumer, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException errorStartingConsumer(String message);

   @Message(id = 119007, value = "error acknowledging message {0}, {1}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException errorAcknowledgingMessage(String messageID, String message);

   @Message(id = 119008, value = "error cancelling message {0}, {1}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException errorCancellingMessage(String messageID, String message);

   @Message(id = 119010, value = "source address does not exist")
   ActiveMQAMQPNotFoundException sourceAddressDoesntExist();

   @Message(id = 119011, value = "source address not set")
   ActiveMQAMQPInvalidFieldException sourceAddressNotSet();

   @Message(id = 119012, value = "error rolling back coordinator: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException errorRollingbackCoordinator(String message);

   @Message(id = 119013, value = "error committing coordinator: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException errorCommittingCoordinator(String message);

   @Message(id = 119014, value = "Transaction not found: xid={0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException txNotFound(String xidToString);

   @Message(id = 119015, value = "not authorized to create consumer, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPSecurityException securityErrorCreatingConsumer(String message);

   @Message(id = 119016, value = "not authorized to create temporary destination, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPSecurityException securityErrorCreatingTempDestination(String message);

   @Message(id = 119017, value = "not authorized to create producer, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPSecurityException securityErrorCreatingProducer(String message);

   @Message(id = 119018, value = "link is missing an offered capability declaration {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException missingOfferedCapability(String capability);

   @Message(id = 119019, value = "There is no brokerID defined on the target connection. Connection will be closed.", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException missingBrokerID();

   @Message(id = 119020, value = "The Broker Connection Open Callback Has Timed Out.", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException brokerConnectionTimeout();

   @Message(id = 119021, value = "The broker connection had a remote link closed unexpectedly", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException brokerConnectionRemoteLinkClosed();

   @Message(id = 119022, value = "The broker connection is trying to connect to itself. Check your configuration.", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException brokerConnectionMirrorItself();

   @Message(id = 119023, value =  "Sender link refused for address {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException senderLinkRefused(String address);

   @Message(id = 119024, value = "link is missing a desired capability declaration {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException missingDesiredCapability(String capability);


}
