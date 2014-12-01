/**
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
package org.apache.activemq.core.client;


import org.apache.activemq.api.core.ActiveMQAddressFullException;
import org.apache.activemq.api.core.ActiveMQConnectionTimedOutException;
import org.apache.activemq.api.core.ActiveMQDisconnectedException;
import org.apache.activemq.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.api.core.ActiveMQInterceptorRejectedPacketException;
import org.apache.activemq.api.core.ActiveMQInternalErrorException;
import org.apache.activemq.api.core.ActiveMQLargeMessageException;
import org.apache.activemq.api.core.ActiveMQLargeMessageInterruptedException;
import org.apache.activemq.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.api.core.ActiveMQTransactionOutcomeUnknownException;
import org.apache.activemq.api.core.ActiveMQTransactionRolledBackException;
import org.apache.activemq.api.core.ActiveMQUnBlockedException;
import org.apache.activemq.core.cluster.DiscoveryGroup;
import org.apache.activemq.spi.core.remoting.Connection;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;
import org.jboss.logging.Messages;
import org.w3c.dom.Node;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/12/12
 *
 * Logger Code 11
 *
 * each message id must be 6 digits long starting with 10, the 3rd digit should be 9
 *
 * so 119000 to 119999
 */
@MessageBundle(projectCode = "AMQ")
public interface ActiveMQClientMessageBundle
{
   ActiveMQClientMessageBundle BUNDLE = Messages.getBundle(ActiveMQClientMessageBundle.class);

   @Message(id = 119000, value = "ClientSession closed while creating session", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException clientSessionClosed();

   @Message(id = 119001, value = "Failed to create session", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException failedToCreateSession(@Cause Throwable t);

   @Message(id = 119002, value = "Internal Error! ClientSessionFactoryImpl::createSessionInternal "
                                          + "just reached a condition that was not supposed to happen. "
                                      + "Please inform this condition to the ActiveMQ team", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException clietSessionInternal();

   @Message(id = 119003, value = "Queue can not be both durable and temporary", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException queueMisConfigured();

   @Message(id = 119004, value = "Failed to initialise session factory", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException failedToInitialiseSessionFactory(@Cause Exception e);

   @Message(id = 119005, value = "Exception in Netty transport", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException nettyError();

   @Message(id = 119006, value =  "Channel disconnected", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQNotConnectedException channelDisconnected();

   @Message(id = 119007, value =  "Cannot connect to server(s). Tried with all available servers.", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQNotConnectedException cannotConnectToServers();

   @Message(id = 119008, value =  "Failed to connect to any static connectors", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQNotConnectedException cannotConnectToStaticConnectors(@Cause Exception e);

   @Message(id = 119009, value =  "Failed to connect to any static connectors", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQNotConnectedException cannotConnectToStaticConnectors2();

   @Message(id = 119010, value =  "Connection is destroyed", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQNotConnectedException connectionDestroyed();

   @Message(id = 119011, value =  "Did not receive data from server for {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQConnectionTimedOutException connectionTimedOut(Connection transportConnection);

   @Message(id = 119012, value =  "Timed out waiting to receive initial broadcast from cluster", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQConnectionTimedOutException connectionTimedOutInInitialBroadcast();

   @Message(id = 119013, value =  "Timed out waiting to receive cluster topology. Group:{0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQConnectionTimedOutException connectionTimedOutOnReceiveTopology(DiscoveryGroup discoveryGroup);

   @Message(id = 119014, value =  "Timed out waiting for response when sending packet {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQConnectionTimedOutException timedOutSendingPacket(Byte type);

   @Message(id = 119015, value =  "The connection was disconnected because of server shutdown", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQDisconnectedException disconnected();

   @Message(id = 119016, value =  "Connection failure detected. Unblocking a blocking call that will never get a resp" +
         "onse", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQUnBlockedException unblockingACall(@Cause Throwable t);

   @Message(id = 119017, value =  "Consumer is closed", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQObjectClosedException consumerClosed();

   @Message(id = 119018, value =  "Producer is closed", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQObjectClosedException producerClosed();

   @Message(id = 119019, value =  "Session is closed", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQObjectClosedException sessionClosed();

   @Message(id = 119020, value =  "Cannot call receive(...) - a MessageHandler is set", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException messageHandlerSet();

   @Message(id = 119021, value =  "Cannot set MessageHandler - consumer is in receive(...)", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException inReceive();

   @Message(id = 119022, value =  "Header size ({0}) is too big, use the messageBody for large data, or increase minLargeMessageSize",
         format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException headerSizeTooBig(Integer headerSize);

   @Message(id = 119023, value =  "The large message lost connection with its session, either because of a rollback or a closed session", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException largeMessageLostSession();

   @Message(id = 119024, value =  "Could not select a TransportConfiguration to create SessionFactory", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException noTCForSessionFactory();

   @Message(id = 119025, value = "Error saving the message body", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQLargeMessageException errorSavingBody(@Cause Exception e);

   @Message(id = 119026, value =  "Error reading the LargeMessageBody", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQLargeMessageException errorReadingBody(@Cause Exception e);

   @Message(id = 119027, value =  "Error closing stream from LargeMessageBody", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQLargeMessageException errorClosingLargeMessage(@Cause Exception e);

   @Message(id = 119028, value =  "Timeout waiting for LargeMessage Body", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQLargeMessageException timeoutOnLargeMessage();

   @Message(id = 119029, value =  "Error writing body of message", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQLargeMessageException errorWritingLargeMessage(@Cause Exception e);

   @Message(id = 119030, value =  "The transaction was rolled back on failover to a backup server", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQTransactionRolledBackException txRolledBack();

   @Message(id = 119031, value =  "The transaction was rolled back on failover however commit may have been successful" +
         "", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQTransactionOutcomeUnknownException txOutcomeUnknown();

   @Message(id = 119032, value = "Invalid type: {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidType(Object type);

   @Message(id = 119033, value = "Invalid type: {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidEncodeType(Object type);

   @Message(id = 119034, value = "Params for management operations must be of the following type: int long double String boolean Map or array thereof but found {0}",
         format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidManagementParam(Object type);

   @Message(id = 119035, value = "Invalid window size {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidWindowSize(Integer size);

   @Message(id = 119036, value = "No operation mapped to int {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noOperationMapped(Integer operation);

   @Message(id = 119037, value = "Invalid last Received Command ID: {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidCommandID(Integer lastReceivedCommandID);

   @Message(id = 119038, value = "Cannot find channel with id {0} to close", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noChannelToClose(Long id);

   @Message(id = 119039, value = "Close Listener cannot be null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException closeListenerCannotBeNull();

   @Message(id = 119040, value = "Fail Listener cannot be null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException failListenerCannotBeNull();

   @Message(id = 119041, value = "Connection already exists with id {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException connectionExists(Object id);

   @Message(id = 119042, value = "Invalid argument null listener", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nullListener();

   @Message(id = 119043, value = "Invalid argument null handler", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nullHandler();

   @Message(id = 119044, value = "No available codec to decode password!", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noCodec();

   @Message(id = 119045, value = "the first node to be compared is null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException firstNodeNull();

   @Message(id = 119046, value = "the second node to be compared is null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException secondNodeNull();

   @Message(id = 119047, value = "nodes have different node names", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nodeHaveDifferentNames();

   @Message(id = 119048, value = "nodes hava a different number of attributes", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nodeHaveDifferentAttNumber();

   @Message(id = 119049, value = "attribute {0}={1} does not match", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException attsDontMatch(String name, String value);

   @Message(id = 119050, value = "one node has children and the other does not" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException oneNodeHasChildren();

   @Message(id = 119051, value = "nodes hava a different number of children" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nodeHasDifferentChildNumber();

   @Message(id = 119052, value = "Element {0} requires a valid Boolean value, but ''{1}'' cannot be parsed as a Boolean" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException mustBeBoolean(Node elem, String value);

   @Message(id = 119053, value = "Element {0} requires a valid Double value, but ''{1}'' cannot be parsed as a Double" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException mustBeDouble(Node elem, String value);

   @Message(id = 119054, value = "Element {0} requires a valid Integer value, but ''{1}'' cannot be parsed as a Integer" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException mustBeInteger(Node elem, String value);

   @Message(id = 119055, value = "Element {0} requires a valid Long value, but ''{1}'' cannot be parsed as a Long" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException mustBeLong(Node elem, String value);

   @Message(id = 119056, value = "Failed to get decoder" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException failedToGetDecoder(@Cause Exception e);

   @Message(id = 119057, value = "Error decoding password" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException errordecodingPassword(@Cause Exception e);

   @Message(id = 119058, value = "Address \"{0}\" is full. Message encode size = {1}B", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAddressFullException addressIsFull(String addressName, int size);

   @Message(id = 119059, value = "Interceptor {0} rejected packet in a blocking call. This call will never complete."
         , format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInterceptorRejectedPacketException interceptorRejectedPacket(String interceptionResult);

   @Message(id = 119060, value = "Large Message Transmission interrupted on consumer shutdown."
         , format = Message.Format.MESSAGE_FORMAT)
   ActiveMQLargeMessageInterruptedException largeMessageInterrupted();

   @Message(id = 119061, value =  "error decoding AMQP frame", format = Message.Format.MESSAGE_FORMAT)
   String decodeError();

}
