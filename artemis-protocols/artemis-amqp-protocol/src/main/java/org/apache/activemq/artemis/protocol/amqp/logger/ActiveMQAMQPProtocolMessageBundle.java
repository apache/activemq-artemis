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
 * Each message id must be 6 digits long starting with 10, the 3rd digit should be 9. So the range
 * is from 219000 to 119999.
 * <p>
 * Once released, methods should not be deleted as they may be referenced by knowledge base
 * articles. Unused methods should be marked as deprecated.
 */
@MessageBundle(projectCode = "AMQ")
public interface ActiveMQAMQPProtocolMessageBundle {

   ActiveMQAMQPProtocolMessageBundle BUNDLE = Messages.getBundle(ActiveMQAMQPProtocolMessageBundle.class);

   @Message(id = 219000, value = "target address not set")
   ActiveMQAMQPInvalidFieldException targetAddressNotSet();

   @Message(id = 219001, value = "error creating temporary queue, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPInternalErrorException errorCreatingTemporaryQueue(String message);

   @Message(id = 219002, value = "target address does not exist")
   ActiveMQAMQPNotFoundException addressDoesntExist();

   @Message(id = 219003, value = "error finding temporary queue, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPNotFoundException errorFindingTemporaryQueue(String message);

   @Message(id = 219005, value = "error creating consumer, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPInternalErrorException errorCreatingConsumer(String message);

   @Message(id = 219006, value = "error starting consumer, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException errorStartingConsumer(String message);

   @Message(id = 219007, value = "error acknowledging message {0}, {1}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException errorAcknowledgingMessage(String messageID, String message);

   @Message(id = 219008, value = "error cancelling message {0}, {1}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException errorCancellingMessage(String messageID, String message);

   @Message(id = 219010, value = "source address does not exist")
   ActiveMQAMQPNotFoundException sourceAddressDoesntExist();

   @Message(id = 219011, value = "source address not set")
   ActiveMQAMQPInvalidFieldException sourceAddressNotSet();

   @Message(id = 219012, value = "error rolling back coordinator: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException errorRollingbackCoordinator(String message);

   @Message(id = 219013, value = "error committing coordinator: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException errorCommittingCoordinator(String message);

   @Message(id = 219014, value = "Transaction not found: xid={0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPIllegalStateException txNotFound(String xidToString);

   @Message(id = 219015, value = "not authorized to create consumer, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPSecurityException securityErrorCreatingConsumer(String message);

   @Message(id = 219016, value = "not authorized to create temporary destination, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPSecurityException securityErrorCreatingTempDestination(String message);

   @Message(id = 219017, value = "not authorized to create producer, {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAMQPSecurityException securityErrorCreatingProducer(String message);

}
