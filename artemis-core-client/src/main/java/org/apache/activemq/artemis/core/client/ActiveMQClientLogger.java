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
package org.apache.activemq.artemis.core.client;

import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.w3c.dom.Node;

import java.net.UnknownHostException;

/**
 * Logger Code 21
 * <p>
 * Each message id must be 6 digits long starting with 21, the 3rd digit donates the level so
 *
 * <pre>
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 * </pre>
 *
 * so an INFO message would be 211000 to 211999.
 * <p>
 * Once released, methods should not be deleted as they may be referenced by knowledge base
 * articles. Unused methods should be marked as deprecated.
 */
@MessageLogger(projectCode = "AMQ")
public interface ActiveMQClientLogger extends BasicLogger {

   /**
    * The default logger.
    */
   ActiveMQClientLogger LOGGER = Logger.getMessageLogger(ActiveMQClientLogger.class, ActiveMQClientLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 211000, value = "**** Dumping session creation stacks ****", format = Message.Format.MESSAGE_FORMAT)
   void dumpingSessionStacks();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 211001, value = "session created", format = Message.Format.MESSAGE_FORMAT)
   void dumpingSessionStack(@Cause Exception e);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 211002, value = "Started {0} Netty Connector version {1} to {2}:{3,number,#}", format = Message.Format.MESSAGE_FORMAT)
   void startedNettyConnector(String connectorType, String version, String host, Integer port);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 211003, value = "Started InVM Connector", format = Message.Format.MESSAGE_FORMAT)
   void startedInVMConnector();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212000, value = "{0}", format = Message.Format.MESSAGE_FORMAT)
   void warn(String message);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212001, value = "Error on clearing messages", format = Message.Format.MESSAGE_FORMAT)
   void errorClearingMessages(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212002, value = "Timed out waiting for handler to complete processing", format = Message.Format.MESSAGE_FORMAT)
   void timeOutWaitingForProcessing();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212003, value = "Unable to close session", format = Message.Format.MESSAGE_FORMAT)
   void unableToCloseSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212004, value = "Failed to connect to server.", format = Message.Format.MESSAGE_FORMAT)
   void failedToConnectToServer();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212005, value = "Tried {0} times to connect. Now giving up on reconnecting it.", format = Message.Format.MESSAGE_FORMAT)
   void failedToConnectToServer(Integer reconnectAttempts);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212007,
      value = "connector.create or connectorFactory.createConnector should never throw an exception, implementation is badly behaved, but we will deal with it anyway.",
      format = Message.Format.MESSAGE_FORMAT)
   void createConnectorException(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212008,
      value = "I am closing a core ClientSessionFactory you left open. Please make sure you close all ClientSessionFactories explicitly " + "before letting them go out of scope! {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void factoryLeftOpen(@Cause Exception e, int i);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212009, value = "resetting session after failure", format = Message.Format.MESSAGE_FORMAT)
   void resettingSessionAfterFailure();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212010, value = "Server is starting, retry to create the session {0}", format = Message.Format.MESSAGE_FORMAT)
   void retryCreateSessionSeverStarting(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212011, value = "committing transaction after failover occurred, any non persistent messages may be lost", format = Message.Format.MESSAGE_FORMAT)
   void commitAfterFailover();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212012, value = "failure occurred during commit throwing XAException", format = Message.Format.MESSAGE_FORMAT)
   void failoverDuringCommit();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212014, value = "failover occurred during prepare rolling back", format = Message.Format.MESSAGE_FORMAT)
   void failoverDuringPrepareRollingBack();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212015, value = "failover occurred during prepare rolling back", format = Message.Format.MESSAGE_FORMAT)
   void errorDuringPrepare(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212016,
      value = "I am closing a core ClientSession you left open. Please make sure you close all ClientSessions explicitly before letting them go out of scope! {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void clientSessionNotClosed(@Cause Exception e, int identity);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212017, value = "error adding packet", format = Message.Format.MESSAGE_FORMAT)
   void errorAddingPacket(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212018, value = "error calling cancel", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingCancel(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212019, value = "error reading index", format = Message.Format.MESSAGE_FORMAT)
   void errorReadingIndex(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212020, value = "error setting index", format = Message.Format.MESSAGE_FORMAT)
   void errorSettingIndex(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212021, value = "error resetting index", format = Message.Format.MESSAGE_FORMAT)
   void errorReSettingIndex(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212022, value = "error reading LargeMessage file cache", format = Message.Format.MESSAGE_FORMAT)
   void errorReadingCache(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212023, value = "error closing LargeMessage file cache", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingCache(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212024, value = "Exception during finalization for LargeMessage file cache", format = Message.Format.MESSAGE_FORMAT)
   void errorFinalisingCache(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212025, value = "did not connect the cluster connection to other nodes", format = Message.Format.MESSAGE_FORMAT)
   void errorConnectingToNodes(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212026, value = "Timed out waiting for pool to terminate", format = Message.Format.MESSAGE_FORMAT)
   void timedOutWaitingForTermination();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212027, value = "Timed out waiting for scheduled pool to terminate", format = Message.Format.MESSAGE_FORMAT)
   void timedOutWaitingForScheduledPoolTermination();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212028, value = "error starting server locator", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingLocator(@Cause Exception e);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 212029,
      value = "Closing a Server Locator left open. Please make sure you close all Server Locators explicitly before letting them go out of scope! {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void serverLocatorNotClosed(@Cause Exception e, int identity);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212030, value = "error sending topology", format = Message.Format.MESSAGE_FORMAT)
   void errorSendingTopology(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212031, value = "error sending topology", format = Message.Format.MESSAGE_FORMAT)
   void errorSendingTopologyNodedown(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212032, value = "Timed out waiting to stop discovery thread", format = Message.Format.MESSAGE_FORMAT)
   void timedOutStoppingDiscovery();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212033, value = "unable to send notification when discovery group is stopped", format = Message.Format.MESSAGE_FORMAT)
   void errorSendingNotifOnDiscoveryStop(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212034,
      value = "There are more than one servers on the network broadcasting the same node id. " + "You will see this message exactly once (per node) if a node is restarted, in which case it can be safely " + "ignored. But if it is logged continuously it means you really do have more than one node on the same network " + "active concurrently with the same node id. This could occur if you have a backup node active at the same time as " + "its live node. nodeID={0}",
      format = Message.Format.MESSAGE_FORMAT)
   void multipleServersBroadcastingSameNode(String nodeId);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212035, value = "error receiving packet in discovery", format = Message.Format.MESSAGE_FORMAT)
   void errorReceivingPacketInDiscovery(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212036,
      value = "Can not find packet to clear: {0} last received command id first stored command id {1}",
      format = Message.Format.MESSAGE_FORMAT)
   void cannotFindPacketToClear(Integer lastReceivedCommandID, Integer firstStoredCommandID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212037, value = "Connection failure to {0} has been detected: {1} [code={2}]", format = Message.Format.MESSAGE_FORMAT)
   void connectionFailureDetected(String remoteAddress, String message, ActiveMQExceptionType type);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212038, value = "Failure in calling interceptor: {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingInterceptor(@Cause Throwable e, Interceptor interceptor);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212040, value = "Timed out waiting for netty ssl close future to complete", format = Message.Format.MESSAGE_FORMAT)
   void timeoutClosingSSL();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212041, value = "Timed out waiting for netty channel to close", format = Message.Format.MESSAGE_FORMAT)
   void timeoutClosingNettyChannel();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212042, value = "Timed out waiting for packet to be flushed", format = Message.Format.MESSAGE_FORMAT)
   void timeoutFlushingPacket();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212043, value = "Property {0} must be an Integer, it is {1}", format = Message.Format.MESSAGE_FORMAT)
   void propertyNotInteger(String propName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212044, value = "Property {0} must be a Long, it is {1}", format = Message.Format.MESSAGE_FORMAT)
   void propertyNotLong(String propName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212045, value = "Property {0} must be a Boolean, it is {1}", format = Message.Format.MESSAGE_FORMAT)
   void propertyNotBoolean(String propName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212046, value = "Cannot find activemq-version.properties on classpath: {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void noVersionOnClasspath(String classpath);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212047, value = "Warning: JVM allocated more data what would make results invalid {0}:{1}", format = Message.Format.MESSAGE_FORMAT)
   void jvmAllocatedMoreMemory(Long totalMemory1, Long totalMemory2);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212048, value = "Random address ({0}) was already in use, trying another time",
      format = Message.Format.MESSAGE_FORMAT)
   void broadcastGroupBindErrorRetry(String hostAndPort, @Cause Throwable t);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212049,
      value = "Could not bind to {0} ({1} address); " +
         "make sure your discovery group-address is of the same type as the IP stack (IPv4 or IPv6)." +
         "\nIgnoring discovery group-address, but this may lead to cross talking.",
      format = Message.Format.MESSAGE_FORMAT)
   void ioDiscoveryError(String hostAddress, String s);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212050, value = "Compressed large message tried to read {0} bytes from stream {1}",
      format = Message.Format.MESSAGE_FORMAT)
   void compressedLargeMessageError(int length, int nReadBytes);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212051,
      value = "Invalid concurrent session usage. Sessions are not supposed to be used by more than one thread concurrently.",
      format = Message.Format.MESSAGE_FORMAT)
   void invalidConcurrentSessionUsage(@Cause Throwable t);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212052,
      value = "Packet {0} was answered out of sequence due to a previous server timeout and it''s being ignored",
      format = Message.Format.MESSAGE_FORMAT)
   void packetOutOfOrder(Object obj, @Cause Throwable t);

   /**
    * Warns about usage of {@link org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler} or JMS's {@code CompletionWindow} with
    * confirmations disabled (confirmationWindowSize=-1).
    */
   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212053,
      value = "CompletionListener/SendAcknowledgementHandler used with confirmationWindowSize=-1. Enable confirmationWindowSize to receive acks from server!",
      format = Message.Format.MESSAGE_FORMAT)
   void confirmationWindowDisabledWarning();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212054,
      value = "Destination address={0} is blocked. If the system is configured to block make sure you consume messages on this configuration.",
      format = Message.Format.MESSAGE_FORMAT)
   void outOfCreditOnFlowControl(String address);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212055, value = "Unable to close consumer", format = Message.Format.MESSAGE_FORMAT)
   void unableToCloseConsumer(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212056, value = "local-bind-address specified for broadcast group but no local-bind-port. Using random port for UDP Broadcast ({0})",
      format = Message.Format.MESSAGE_FORMAT)
   void broadcastGroupBindError(String hostAndPort);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212057, value = "Large Message Streaming is taking too long to flush on back pressure.",
      format = Message.Format.MESSAGE_FORMAT)
   void timeoutStreamingLargeMessage();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212058, value = "Unable to get a message.",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToGetMessage(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212059, value = "Failed to clean up: {0} ",
           format = Message.Format.MESSAGE_FORMAT)
   void failedCleaningUp(String target);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212060, value = "Unexpected null data received from DiscoveryEndpoint ",
           format = Message.Format.MESSAGE_FORMAT)
   void unexpectedNullDataReceived();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212061, value = "Failed to perform force close ",
           format = Message.Format.MESSAGE_FORMAT)
   void failedForceClose(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212062, value = "Failed to perform post actions on message processing ",
           format = Message.Format.MESSAGE_FORMAT)
   void failedPerformPostActionsOnMessage(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212063, value = "Unable to handle connection failure ",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToHandleConnectionFailure(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212064, value = "Unable to receive cluster topology ",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToReceiveClusterTopology(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212065, value = "{0} getting exception when receiving broadcasting ",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToReceiveBroadcast(@Cause Exception e, String target);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212066, value = "failed to parse int property ",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToParseValue(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212067, value = "failed to get system property ",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToGetProperty(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212068, value = "Couldn't finish the client globalThreadPool in less than 10 seconds, interrupting it now ",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToProcessGlobalThreadPoolIn10Sec();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212069, value = "Couldn't finish the client scheduled in less than 10 seconds, interrupting it now ",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToProcessScheduledlIn10Sec();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212070, value = "Unable to initialize VersionLoader ",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToInitVersionLoader(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212071, value = "Unable to check Epoll availability ",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToCheckEpollAvailability(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212072, value = "Failed to change channel state to ReadyForWriting ",
           format = Message.Format.MESSAGE_FORMAT)
   void failedToSetChannelReadyForWriting(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212073, value = "Unable to check KQueue availability ",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToCheckKQueueAvailability(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212074, value = "SendAcknowledgementHandler will not be asynchronous without setting up confirmation window size",
      format = Message.Format.MESSAGE_FORMAT)
   void confirmationNotSet();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212075, value = "KQueue is not available, please add to the classpath or configure useKQueue=false to remove this warning",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToCheckKQueueAvailabilityNoClass();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212076, value = "Epoll is not available, please add to the classpath or configure useEpoll=false to remove this warning",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToCheckEpollAvailabilitynoClass();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212077, value = "Timed out waiting to receive initial broadcast from cluster. Retry {0} of {1}",
           format = Message.Format.MESSAGE_FORMAT)
   void broadcastTimeout(int retry, int maxretry);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212078, value = "Connection factory parameter ignored {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void connectionFactoryParameterIgnored(String parameterName);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214000, value = "Failed to call onMessage", format = Message.Format.MESSAGE_FORMAT)
   void onMessageError(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214001, value = "failed to cleanup session", format = Message.Format.MESSAGE_FORMAT)
   void failedToCleanupSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214002, value = "Failed to execute failure listener", format = Message.Format.MESSAGE_FORMAT)
   void failedToExecuteListener(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214003, value = "Failed to handle failover", format = Message.Format.MESSAGE_FORMAT)
   void failedToHandleFailover(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214004, value = "XA end operation failed ", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingEnd(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214005, value = "XA start operation failed {0} code:{1}", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingStart(String message, Integer code);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214006, value = "Session is not XA", format = Message.Format.MESSAGE_FORMAT)
   void sessionNotXA();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214007, value = "Received exception asynchronously from server", format = Message.Format.MESSAGE_FORMAT)
   void receivedExceptionAsynchronously(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214008, value = "Failed to handle packet", format = Message.Format.MESSAGE_FORMAT)
   void failedToHandlePacket(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214009, value = "Failed to stop discovery group", format = Message.Format.MESSAGE_FORMAT)
   void failedToStopDiscovery(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214010, value = "Failed to receive datagram", format = Message.Format.MESSAGE_FORMAT)
   void failedToReceiveDatagramInDiscovery(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214011, value = "Failed to call discovery listener", format = Message.Format.MESSAGE_FORMAT)
   void failedToCallListenerInDiscovery(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214012, value = "Unexpected error handling packet {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorHandlingPacket(@Cause Throwable t, Packet packet);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214013, value = "Failed to decode packet", format = Message.Format.MESSAGE_FORMAT)
   void errorDecodingPacket(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214014, value = "Failed to execute failure listener", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingFailureListener(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214015, value = "Failed to execute connection life cycle listener", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingLifeCycleListener(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214016, value = "Failed to create netty connection", format = Message.Format.MESSAGE_FORMAT)
   void errorCreatingNettyConnection(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214017, value = "Caught unexpected Throwable", format = Message.Format.MESSAGE_FORMAT)
   void caughtunexpectedThrowable(@Cause Throwable t);

   @Deprecated
   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214018, value = "Failed to invoke getTextContent() on node {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorOnXMLTransform(@Cause Throwable t, Node n);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214019, value = "Invalid configuration", format = Message.Format.MESSAGE_FORMAT)
   void errorOnXMLTransformInvalidConf(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214020, value = "Exception happened while stopping Discovery BroadcastEndpoint {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingDiscoveryBroadcastEndpoint(Object endpoint, @Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214021, value = "Invalid cipher suite specified. Supported cipher suites are: {0}", format = Message.Format.MESSAGE_FORMAT)
   void invalidCipherSuite(String validSuites);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214022, value = "Invalid protocol specified. Supported protocols are: {0}", format = Message.Format.MESSAGE_FORMAT)
   void invalidProtocol(String validProtocols);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 214023, value = "HTTP Handshake failed, received %s")
   void httpHandshakeFailed(Object msg);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214024, value = "HTTP upgrade not supported by remote acceptor")
   void httpUpgradeNotSupportedByRemoteAcceptor();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214025, value = "Invalid type {0}, Using default connection factory at {1}", format = Message.Format.MESSAGE_FORMAT)
   void invalidCFType(String type, String uri);

   @LogMessage(level = Logger.Level.TRACE)
   @Message(id = 214026,
      value = "Failure captured on connectionID={0}, performing failover or reconnection now",
      format = Message.Format.MESSAGE_FORMAT)
   void failoverOrReconnect(Object connectionID, @Cause Throwable cause);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 214027,
      value = "Replaying commands for channelID={0} with lastCommandID from the server={1}",
      format = Message.Format.MESSAGE_FORMAT)
   void replayingCommands(Object connectionID, int lastConfirmedCommandID);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 214028,
      value = "Couldn't reattach session {0}, performing as a failover operation now and recreating objects",
      format = Message.Format.MESSAGE_FORMAT)
   void reconnectCreatingNewSession(long id);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214029, value = "Unexpected response from HTTP server: %s")
   void unexpectedResponseFromHttpServer(Object response);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214030, value = "Failed to bind {0}={1}", format = Message.Format.MESSAGE_FORMAT)
   void failedToBind(String p1, String p2, @Cause Throwable cause);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214031, value = "Failed to decode buffer, disconnect immediately.", format = Message.Format.MESSAGE_FORMAT)
   void disconnectOnErrorDecoding(@Cause Throwable cause);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214032, value = "Unable to initialize VersionLoader ",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToInitVersionLoaderError(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214033, value = "Cannot resolve host ",
           format = Message.Format.MESSAGE_FORMAT)
   void unableToResolveHost(@Cause UnknownHostException e);
}
