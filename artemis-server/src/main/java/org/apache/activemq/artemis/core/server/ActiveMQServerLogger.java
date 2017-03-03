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
package org.apache.activemq.artemis.core.server;

/**
 * Logger Code 22
 *
 * each message id must be 6 digits long starting with 10, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 101000 to 101999
 */

import javax.transaction.xa.Xid;
import java.io.File;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import io.netty.channel.Channel;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.BackupReplicationStartFailedMessage;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.core.server.cluster.impl.BridgeImpl;
import org.apache.activemq.artemis.core.server.cluster.impl.ClusterConnectionImpl;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.utils.FutureLatch;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.w3c.dom.Node;

@MessageLogger(projectCode = "AMQ")
public interface ActiveMQServerLogger extends BasicLogger {

   /**
    * The default logger.
    */
   ActiveMQServerLogger LOGGER = Logger.getMessageLogger(ActiveMQServerLogger.class, ActiveMQServerLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 223000, value = "Received Interrupt Exception whilst waiting for component to shutdown: {0}", format = Message.Format.MESSAGE_FORMAT)
   void interruptWhilstStoppingComponent(String componentClassName);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221000, value = "{0} Message Broker is starting with configuration {1}", format = Message.Format.MESSAGE_FORMAT)
   void serverStarting(String type, Configuration configuration);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221001, value = "Apache ActiveMQ Artemis Message Broker version {0} [{1}, nodeID={2}] {3}", format = Message.Format.MESSAGE_FORMAT)
   void serverStarted(String fullVersion, String name, SimpleString nodeId, String identity);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221002, value = "Apache ActiveMQ Artemis Message Broker version {0} [{1}] stopped, uptime {2}", format = Message.Format.MESSAGE_FORMAT)
   void serverStopped(String version, SimpleString nodeId, String uptime);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221003, value = "Deploying queue {0}", format = Message.Format.MESSAGE_FORMAT)
   void deployQueue(SimpleString queueName);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221004, value = "{0}", format = Message.Format.MESSAGE_FORMAT)
   void dumpServerInfo(String serverInfo);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221005, value = "Deleting pending large message as it was not completed: {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void deletingPendingMessage(Pair<Long, Long> msgToDelete);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221006, value = "Waiting to obtain live lock", format = Message.Format.MESSAGE_FORMAT)
   void awaitingLiveLock();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221007, value = "Server is now live", format = Message.Format.MESSAGE_FORMAT)
   void serverIsLive();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221008, value = "live server wants to restart, restarting server in backup", format = Message.Format.MESSAGE_FORMAT)
   void awaitFailBack();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221109, value = "Apache ActiveMQ Artemis Backup Server version {0} [{1}] started, waiting live to fail before it gets active",
      format = Message.Format.MESSAGE_FORMAT)
   void backupServerStarted(String version, SimpleString nodeID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221010, value = "Backup Server is now live", format = Message.Format.MESSAGE_FORMAT)
   void backupServerIsLive();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221011, value = "Server {0} is now live", format = Message.Format.MESSAGE_FORMAT)
   void serverIsLive(String identity);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221012, value = "Using AIO Journal", format = Message.Format.MESSAGE_FORMAT)
   void journalUseAIO();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221013, value = "Using NIO Journal", format = Message.Format.MESSAGE_FORMAT)
   void journalUseNIO();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221014, value = "{0}% loaded", format = Message.Format.MESSAGE_FORMAT)
   void percentLoaded(Long percent);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221015, value = "Can not find queue {0} while reloading ACKNOWLEDGE_CURSOR, deleting record now",
      format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueueReloading(Long queueID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221016,
      value = "Can not find queue {0} while reloading PAGE_CURSOR_COUNTER_VALUE, deleting record now",
      format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueueReloadingPage(Long queueID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221017, value = "Can not find queue {0} while reloading PAGE_CURSOR_COUNTER_INC, deleting record now",
      format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueueReloadingPageCursor(Long queueID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221018, value = "Large message: {0} did not have any associated reference, file will be deleted",
      format = Message.Format.MESSAGE_FORMAT)
   void largeMessageWithNoRef(Long messageID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221019, value = "Deleting unreferenced message id={0} from the journal", format = Message.Format.MESSAGE_FORMAT)
   void journalUnreferencedMessage(Long messageID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221020, value = "Started Acceptor at {0}:{1,number,#} for protocols [{2}]", format = Message.Format.MESSAGE_FORMAT)
   void startedAcceptor(String host, Integer port, String enabledProtocols);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221021, value = "failed to remove connection", format = Message.Format.MESSAGE_FORMAT)
   void errorRemovingConnection();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221022, value = "unable to start connector service: {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingConnectorService(@Cause Throwable e, String name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221023, value = "unable to stop connector service: {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingConnectorService(@Cause Throwable e, String name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221024, value = "Backup server {0} is synchronized with live-server.", format = Message.Format.MESSAGE_FORMAT)
   void backupServerSynched(ActiveMQServerImpl server);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221025, value = "Replication: sending {0} (size={1}) to backup. {2}", format = Message.Format.MESSAGE_FORMAT)
   void journalSynch(JournalFile jf, Long size, SequentialFile file);

   @LogMessage(level = Logger.Level.INFO)
   @Message(
      id = 221026,
      value = "Bridge {0} connected to forwardingAddress={1}. {2} does not have any bindings. Messages will be ignored until a binding is created.",
      format = Message.Format.MESSAGE_FORMAT)
   void bridgeNoBindings(SimpleString name, SimpleString forwardingAddress, SimpleString address);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221027, value = "Bridge {0} is connected", format = Message.Format.MESSAGE_FORMAT)
   void bridgeConnected(BridgeImpl name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221028, value = "Bridge is stopping, will not retry", format = Message.Format.MESSAGE_FORMAT)
   void bridgeStopping();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221029, value = "stopped bridge {0}", format = Message.Format.MESSAGE_FORMAT)
   void bridgeStopped(SimpleString name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221030, value = "paused bridge {0}", format = Message.Format.MESSAGE_FORMAT)
   void bridgePaused(SimpleString name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221031, value = "backup announced", format = Message.Format.MESSAGE_FORMAT)
   void backupAnnounced();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221032, value = "Waiting to become backup node", format = Message.Format.MESSAGE_FORMAT)
   void waitingToBecomeBackup();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221033, value = "** got backup lock", format = Message.Format.MESSAGE_FORMAT)
   void gotBackupLock();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221034, value = "Waiting {0} to obtain live lock", format = Message.Format.MESSAGE_FORMAT)
   void waitingToObtainLiveLock(String timeoutMessage);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221035, value = "Live Server Obtained live lock", format = Message.Format.MESSAGE_FORMAT)
   void obtainedLiveLock();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221036, value = "Message with duplicate ID {0} was already set at {1}. Move from {2} being ignored and message removed from {3}",
      format = Message.Format.MESSAGE_FORMAT)
   void messageWithDuplicateID(Object duplicateProperty,
                               SimpleString toAddress,
                               SimpleString address,
                               SimpleString simpleString);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221037, value = "{0} to become ''live''", format = Message.Format.MESSAGE_FORMAT)
   void becomingLive(ActiveMQServer server);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221038, value = "Configuration option ''{0}'' is deprecated. Consult the manual for details.",
      format = Message.Format.MESSAGE_FORMAT)
   void deprecatedConfigurationOption(String deprecatedOption);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221039, value = "Restarting as Replicating backup server after live restart",
      format = Message.Format.MESSAGE_FORMAT)
   void restartingReplicatedBackupAfterFailback();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221040, value = "Remote group coordinators has not started.", format = Message.Format.MESSAGE_FORMAT)
   void remoteGroupCoordinatorsNotStarted();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221041, value = "Cannot find queue {0} while reloading PAGE_CURSOR_COMPLETE, deleting record now",
      format = Message.Format.MESSAGE_FORMAT)
   void cantFindQueueOnPageComplete(long queueID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221042,
      value = "Bridge {0} timed out waiting for the completion of {1} messages, we will just shutdown the bridge after 10 seconds wait",
      format = Message.Format.MESSAGE_FORMAT)
   void timedOutWaitingCompletions(String bridgeName, long numberOfMessages);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221043, value = "Protocol module found: [{1}]. Adding protocol support for: {0}", format = Message.Format.MESSAGE_FORMAT)
   void addingProtocolSupport(String protocolKey, String moduleName);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221045, value = "libaio is not available, switching the configuration into NIO", format = Message.Format.MESSAGE_FORMAT)
   void switchingNIO();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221046, value = "Unblocking message production on address ''{0}''; size is currently: {1} bytes; max-size-bytes: {2}", format = Message.Format.MESSAGE_FORMAT)
   void unblockingMessageProduction(SimpleString addressName, long currentSize, long maxSize);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221047, value = "Backup Server has scaled down to live server", format = Message.Format.MESSAGE_FORMAT)
   void backupServerScaledDown();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221048, value = "Consumer {0}:{1} attached to queue ''{2}'' from {3} identified as ''slow.'' Expected consumption rate: {4} msgs/second; actual consumption rate: {5} msgs/second.", format = Message.Format.MESSAGE_FORMAT)
   void slowConsumerDetected(String sessionID,
                             long consumerID,
                             String queueName,
                             String remoteAddress,
                             float slowConsumerThreshold,
                             float consumerRate);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221049, value = "Activating Replica for node: {0}", format = Message.Format.MESSAGE_FORMAT)
   void activatingReplica(SimpleString nodeID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221050, value = "Activating Shared Store Slave", format = Message.Format.MESSAGE_FORMAT)
   void activatingSharedStoreSlave();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221051, value = "Populating security roles from LDAP at: {0}", format = Message.Format.MESSAGE_FORMAT)
   void populatingSecurityRolesFromLDAP(String url);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221052, value = "Deploying topic {0}", format = Message.Format.MESSAGE_FORMAT)
   void deployTopic(SimpleString topicName);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221053,
      value = "Disallowing use of vulnerable protocol ''{0}'' on acceptor ''{1}''. See http://www.oracle.com/technetwork/topics/security/poodlecve-2014-3566-2339408.html for more details.",
      format = Message.Format.MESSAGE_FORMAT)
   void disallowedProtocol(String protocol, String acceptorName);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221054, value = "libaio was found but the filesystem does not support AIO. Switching the configuration into NIO. Journal path: {0}", format = Message.Format.MESSAGE_FORMAT)
   void switchingNIOonPath(String journalPath);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221055, value = "There were too many old replicated folders upon startup, removing {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void removingBackupData(String path);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 221056, value = "Reloading configuration ...{0}",
      format = Message.Format.MESSAGE_FORMAT)
   void reloadingConfiguration(String module);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222000, value = "ActiveMQServer is being finalized and has not been stopped. Please remember to stop the server before letting it go out of scope",
      format = Message.Format.MESSAGE_FORMAT)
   void serverFinalisedWIthoutBeingSTopped();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222001, value = "Error closing sessions while stopping server", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingSessionsWhileStoppingServer(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222002, value = "Timed out waiting for pool to terminate {0}. Interrupting all its threads!", format = Message.Format.MESSAGE_FORMAT)
   void timedOutStoppingThreadpool(ExecutorService service);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222004, value = "Must specify an address for each divert. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void divertWithNoAddress();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222005, value = "Must specify a forwarding address for each divert. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void divertWithNoForwardingAddress();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222006, value = "Binding already exists with name {0}, divert will not be deployed", format = Message.Format.MESSAGE_FORMAT)
   void divertBindingAlreadyExists(SimpleString bindingName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222007, value = "Security risk! Apache ActiveMQ Artemis is running with the default cluster admin user and default password. Please see the cluster chapter in the ActiveMQ Artemis User Guide for instructions on how to change this.", format = Message.Format.MESSAGE_FORMAT)
   void clusterSecurityRisk();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222008, value = "unable to restart server, please kill and restart manually", format = Message.Format.MESSAGE_FORMAT)
   void serverRestartWarning();

   @LogMessage(level = Logger.Level.WARN)
   void serverRestartWarning(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222009, value = "Unable to announce backup for replication. Trying to stop the server.", format = Message.Format.MESSAGE_FORMAT)
   void replicationStartProblem(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222010, value = "Critical IO Error, shutting down the server. file={1}, message={0}", format = Message.Format.MESSAGE_FORMAT)
   void ioCriticalIOError(String message, String file, @Cause Throwable code);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222011, value = "Error stopping server", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingServer(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222012, value = "Timed out waiting for backup activation to exit", format = Message.Format.MESSAGE_FORMAT)
   void backupActivationProblem();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222013, value = "Error when trying to start replication", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingReplication(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222014, value = "Error when trying to stop replication", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingReplication(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222016, value = "Cannot deploy a connector with no name specified.", format = Message.Format.MESSAGE_FORMAT)
   void connectorWithNoName();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222017, value = "There is already a connector with name {0} deployed. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void connectorAlreadyDeployed(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(
      id = 222018,
      value = "AIO was not located on this platform, it will fall back to using pure Java NIO. If your platform is Linux, install LibAIO to enable the AIO journal",
      format = Message.Format.MESSAGE_FORMAT)
   void AIONotFound();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222019, value = "There is already a discovery group with name {0} deployed. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void discoveryGroupAlreadyDeployed(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222020, value = "error scanning for URL''s", format = Message.Format.MESSAGE_FORMAT)
   void errorScanningURLs(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222021, value = "problem undeploying {0}", format = Message.Format.MESSAGE_FORMAT)
   void problemUndeployingNode(@Cause Exception e, Node node);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222022, value = "Timed out waiting for paging cursor to stop {0} {1}", format = Message.Format.MESSAGE_FORMAT)
   void timedOutStoppingPagingCursor(FutureLatch future, Executor executor);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222023, value = "problem cleaning page address {0}", format = Message.Format.MESSAGE_FORMAT)
   void problemCleaningPageAddress(@Cause Exception e, SimpleString address);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222024, value = "Could not complete operations on IO context {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void problemCompletingOperations(OperationContext e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222025, value = "Problem cleaning page subscription counter", format = Message.Format.MESSAGE_FORMAT)
   void problemCleaningPagesubscriptionCounter(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222026, value = "Error on cleaning up cursor pages", format = Message.Format.MESSAGE_FORMAT)
   void problemCleaningCursorPages(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222027, value = "Timed out flushing executors for paging cursor to stop {0}", format = Message.Format.MESSAGE_FORMAT)
   void timedOutFlushingExecutorsPagingCursor(PageSubscription pageSubscription);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222028, value = "Could not find page cache for page {0} removing it from the journal",
      format = Message.Format.MESSAGE_FORMAT)
   void pageNotFound(PagePosition pos);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222029,
      value = "Could not locate page transaction {0}, ignoring message on position {1} on address={2} queue={3}",
      format = Message.Format.MESSAGE_FORMAT)
   void pageSubscriptionCouldntLoad(long transactionID, PagePosition position, SimpleString address, SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222030, value = "File {0} being renamed to {1}.invalidPage as it was loaded partially. Please verify your data.", format = Message.Format.MESSAGE_FORMAT)
   void pageInvalid(String fileName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222031, value = "Error while deleting page file", format = Message.Format.MESSAGE_FORMAT)
   void pageDeleteError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222032, value = "page finalise error", format = Message.Format.MESSAGE_FORMAT)
   void pageFinaliseError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222033, value = "Page file {0} had incomplete records at position {1} at record number {2}", format = Message.Format.MESSAGE_FORMAT)
   void pageSuspectFile(String fileName, int position, int msgNumber);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222034, value = "Can not delete page transaction id={0}", format = Message.Format.MESSAGE_FORMAT)
   void pageTxDeleteError(@Cause Exception e, long recordID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222035, value = "Directory {0} did not have an identification file {1}",
      format = Message.Format.MESSAGE_FORMAT)
   void pageStoreFactoryNoIdFile(String s, String addressFile);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222036, value = "Timed out on waiting PagingStore {0} to shutdown", format = Message.Format.MESSAGE_FORMAT)
   void pageStoreTimeout(SimpleString address);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222037, value = "IO Error, impossible to start paging", format = Message.Format.MESSAGE_FORMAT)
   void pageStoreStartIOError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222038, value = "Starting paging on address ''{0}''; size is currently: {1} bytes; max-size-bytes: {2}", format = Message.Format.MESSAGE_FORMAT)
   void pageStoreStart(SimpleString storeName, long addressSize, long maxSize);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222039, value = "Messages sent to address ''{0}'' are being dropped; size is currently: {1} bytes; max-size-bytes: {2}", format = Message.Format.MESSAGE_FORMAT)
   void pageStoreDropMessages(SimpleString storeName, long addressSize, long maxSize);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222040, value = "Server is stopped", format = Message.Format.MESSAGE_FORMAT)
   void serverIsStopped();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222041, value = "Cannot find queue {0} to update delivery count", format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueueDelCount(Long queueID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222042, value = "Cannot find message {0} to update delivery count", format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindMessageDelCount(Long msg);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222043, value = "Message for queue {0} which does not exist. This message will be ignored.", format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueueForMessage(Long queueID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222044, value = "It was not possible to delete message {0}", format = Message.Format.MESSAGE_FORMAT)
   void journalErrorDeletingMessage(@Cause Exception e, Long messageID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222045, value = "Message in prepared tx for queue {0} which does not exist. This message will be ignored.", format = Message.Format.MESSAGE_FORMAT)
   void journalMessageInPreparedTX(Long queueID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222046, value = "Failed to remove reference for {0}", format = Message.Format.MESSAGE_FORMAT)
   void journalErrorRemovingRef(Long messageID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222047, value = "Can not find queue {0} while reloading ACKNOWLEDGE_CURSOR",
      format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueueReloadingACK(Long queueID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222048, value = "PAGE_CURSOR_COUNTER_VALUE record used on a prepared statement, invalid state", format = Message.Format.MESSAGE_FORMAT)
   void journalPAGEOnPrepared();

   @LogMessage(level = Logger.Level.WARN)
   @Message(
      id = 222049,
      value = "InternalError: Record type {0} not recognized. Maybe you are using journal files created on a different version",
      format = Message.Format.MESSAGE_FORMAT)
   void journalInvalidRecordType(Byte recordType);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222050, value = "Can not locate recordType={0} on loadPreparedTransaction//deleteRecords",
      format = Message.Format.MESSAGE_FORMAT)
   void journalInvalidRecordTypeOnPreparedTX(Byte recordType);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222051, value = "Journal Error", format = Message.Format.MESSAGE_FORMAT)
   void journalError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222052, value = "error incrementing delay detection", format = Message.Format.MESSAGE_FORMAT)
   void errorIncrementDelayDeletionCount(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222053, value = "Error on copying large message {0} for DLA or Expiry", format = Message.Format.MESSAGE_FORMAT)
   void lareMessageErrorCopying(@Cause Exception e, LargeServerMessage largeServerMessage);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222054, value = "Error on executing IOCallback", format = Message.Format.MESSAGE_FORMAT)
   void errorExecutingAIOCallback(@Cause Throwable t);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222055, value = "Error on deleting duplicate cache", format = Message.Format.MESSAGE_FORMAT)
   void errorDeletingDuplicateCache(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222056, value = "Did not route to any bindings for address {0} and sendToDLAOnNoRoute is true but there is no DLA configured for the address, the message will be ignored.",
      format = Message.Format.MESSAGE_FORMAT)
   void noDLA(SimpleString address);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222057, value = "It was not possible to add references due to an IO error code {0} message = {1}",
      format = Message.Format.MESSAGE_FORMAT)
   void ioErrorAddingReferences(Integer errorCode, String errorMessage);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222059, value = "Duplicate message detected - message will not be routed. Message information:\n{0}", format = Message.Format.MESSAGE_FORMAT)
   void duplicateMessageDetected(org.apache.activemq.artemis.api.core.Message message);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222060, value = "Error while confirming large message completion on rollback for recordID={0}", format = Message.Format.MESSAGE_FORMAT)
   void journalErrorConfirmingLargeMessage(@Cause Throwable e, Long messageID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222061, value = "Client connection failed, clearing up resources for session {0}", format = Message.Format.MESSAGE_FORMAT)
   void clientConnectionFailed(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222062, value = "Cleared up resources for session {0}", format = Message.Format.MESSAGE_FORMAT)
   void clearingUpSession(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222063, value = "Error processing IOCallback code = {0} message = {1}", format = Message.Format.MESSAGE_FORMAT)
   void errorProcessingIOCallback(Integer errorCode, String errorMessage);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 222065, value = "Client is not being consistent on the request versioning. It just sent a version id={0} while it informed {1} previously", format = Message.Format.MESSAGE_FORMAT)
   void incompatibleVersionAfterConnect(int version, int clientVersion);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222066, value = "Reattach request from {0} failed as there is no confirmationWindowSize configured, which may be ok for your system", format = Message.Format.MESSAGE_FORMAT)
   void reattachRequestFailed(String remoteAddress);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222067, value = "Connection failure has been detected: {0} [code={1}]", format = Message.Format.MESSAGE_FORMAT)
   void connectionFailureDetected(String message, ActiveMQExceptionType type);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222069, value = "error cleaning up stomp connection", format = Message.Format.MESSAGE_FORMAT)
   void errorCleaningStompConn(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222070, value = "Stomp Transactional acknowledgement is not supported", format = Message.Format.MESSAGE_FORMAT)
   void stompTXAckNorSupported();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222071, value = "Interrupted while waiting for stomp heartbeat to die", format = Message.Format.MESSAGE_FORMAT)
   void errorOnStompHeartBeat(@Cause InterruptedException e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222072, value = "Timed out flushing channel on InVMConnection", format = Message.Format.MESSAGE_FORMAT)
   void timedOutFlushingInvmChannel();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212074, value = "channel group did not completely close", format = Message.Format.MESSAGE_FORMAT)
   void nettyChannelGroupError();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222075, value = "{0} is still connected to {1}", format = Message.Format.MESSAGE_FORMAT)
   void nettyChannelStillOpen(Channel channel, SocketAddress remoteAddress);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222076, value = "channel group did not completely unbind", format = Message.Format.MESSAGE_FORMAT)
   void nettyChannelGroupBindError();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222077, value = "{0} is still bound to {1}", format = Message.Format.MESSAGE_FORMAT)
   void nettyChannelStillBound(Channel channel, SocketAddress remoteAddress);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222078, value = "Error instantiating remoting interceptor {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorCreatingRemotingInterceptor(@Cause Exception e, String interceptorClass);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222079, value = "The following keys are invalid for configuring the acceptor: {0} the acceptor will not be started.",
      format = Message.Format.MESSAGE_FORMAT)
   void invalidAcceptorKeys(String s);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222080, value = "Error instantiating remoting acceptor {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorCreatingAcceptor(@Cause Exception e, String factoryClassName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222081, value = "Timed out waiting for remoting thread pool to terminate", format = Message.Format.MESSAGE_FORMAT)
   void timeoutRemotingThreadPool();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222082, value = "error on connection failure check", format = Message.Format.MESSAGE_FORMAT)
   void errorOnFailureCheck(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222083, value = "The following keys are invalid for configuring the connector service: {0} the connector will not be started.",
      format = Message.Format.MESSAGE_FORMAT)
   void connectorKeysInvalid(String s);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222084, value = "The following keys are required for configuring the connector service: {0} the connector will not be started.",
      format = Message.Format.MESSAGE_FORMAT)
   void connectorKeysMissing(String s);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222085, value = "Packet {0} can not be processed by the ReplicationEndpoint",
      format = Message.Format.MESSAGE_FORMAT)
   void invalidPacketForReplication(Packet packet);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222086, value = "error handling packet {0} for replication", format = Message.Format.MESSAGE_FORMAT)
   void errorHandlingReplicationPacket(@Cause Exception e, Packet packet);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222087, value = "Replication Error while closing the page on backup", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingPageOnReplication(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222088, value = "Journal comparison mismatch:\n{0}", format = Message.Format.MESSAGE_FORMAT)
   void journalcomparisonMismatch(String s);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222089, value = "Replication Error deleting large message ID = {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorDeletingLargeMessage(@Cause Exception e, long messageId);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222090, value = "Replication Large MessageID {0}  is not available on backup server. Ignoring replication message", format = Message.Format.MESSAGE_FORMAT)
   void largeMessageNotAvailable(long messageId);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222091, value = "The backup node has been shut-down, replication will now stop", format = Message.Format.MESSAGE_FORMAT)
   void replicationStopOnBackupShutdown();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222092, value = "Connection to the backup node failed, removing replication now", format = Message.Format.MESSAGE_FORMAT)
   void replicationStopOnBackupFail(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222093, value = "Timed out waiting to stop Bridge", format = Message.Format.MESSAGE_FORMAT)
   void timedOutWaitingToStopBridge();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222094, value = "Bridge unable to send message {0}, will try again once bridge reconnects", format = Message.Format.MESSAGE_FORMAT)
   void bridgeUnableToSendMessage(@Cause Exception e, MessageReference ref);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222095, value = "Connection failed with failedOver={0}", format = Message.Format.MESSAGE_FORMAT)
   void bridgeConnectionFailed(Boolean failedOver);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222096, value = "Error on querying binding on bridge {0}. Retrying in 100 milliseconds", format = Message.Format.MESSAGE_FORMAT)
   void errorQueryingBridge(@Cause Throwable t, SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222097, value = "Address {0} does not have any bindings, retry #({1})",
      format = Message.Format.MESSAGE_FORMAT)
   void errorQueryingBridge(SimpleString address, Integer retryCount);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222098, value = "Server is starting, retry to create the session for bridge {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingBridge(SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222099, value = "Bridge {0} is unable to connect to destination. It will be disabled.", format = Message.Format.MESSAGE_FORMAT)
   void errorConnectingBridge(@Cause Exception e, Bridge bridge);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222100, value = "ServerLocator was shutdown, can not retry on opening connection for bridge",
      format = Message.Format.MESSAGE_FORMAT)
   void bridgeLocatorShutdown();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222101, value = "Bridge {0} achieved {1} maxattempts={2} it will stop retrying to reconnect", format = Message.Format.MESSAGE_FORMAT)
   void bridgeAbortStart(SimpleString name, Integer retryCount, Integer reconnectAttempts);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222102, value = "Unexpected exception while trying to reconnect", format = Message.Format.MESSAGE_FORMAT)
   void errorReConnecting(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222103, value = "transaction with xid {0} timed out", format = Message.Format.MESSAGE_FORMAT)
   void timedOutXID(Xid xid);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222104, value = "IO Error completing the transaction, code = {0}, message = {1}", format = Message.Format.MESSAGE_FORMAT)
   void ioErrorOnTX(Integer errorCode, String errorMessage);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222105, value = "Could not finish context execution in 10 seconds",
      format = Message.Format.MESSAGE_FORMAT)
   void errorCompletingContext(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222106, value = "Replacing incomplete LargeMessage with ID={0}", format = Message.Format.MESSAGE_FORMAT)
   void replacingIncompleteLargeMessage(Long messageID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222107, value = "Cleared up resources for session {0}", format = Message.Format.MESSAGE_FORMAT)
   void clientConnectionFailedClearingSession(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222108, value = "unable to send notification when broadcast group is stopped",
      format = Message.Format.MESSAGE_FORMAT)
   void broadcastGroupClosed(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222109, value = "Timed out waiting for write lock on consumer. Check the Thread dump", format = Message.Format.MESSAGE_FORMAT)
   void timeoutLockingConsumer();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222110, value = "no queue IDs defined!,  originalMessage  = {0}, copiedMessage = {1}, props={2}",
      format = Message.Format.MESSAGE_FORMAT)
   void noQueueIdDefined(org.apache.activemq.artemis.api.core.Message message, org.apache.activemq.artemis.api.core.Message messageCopy, SimpleString idsHeaderName);

   @LogMessage(level = Logger.Level.TRACE)
   @Message(id = 222111, value = "exception while invoking {0} on {1}",
      format = Message.Format.MESSAGE_FORMAT)
   void managementOperationError(@Cause Exception e, String op, String resourceName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222112, value = "exception while retrieving attribute {0} on {1}",
      format = Message.Format.MESSAGE_FORMAT)
   void managementAttributeError(@Cause Exception e, String att, String resourceName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222113, value = "On ManagementService stop, there are {0} unexpected registered MBeans: {1}",
      format = Message.Format.MESSAGE_FORMAT)
   void managementStopError(Integer size, List<String> unexpectedResourceNames);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222114, value = "Unable to delete group binding info {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void unableToDeleteGroupBindings(@Cause Exception e, SimpleString groupId);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222115, value = "Error closing serverLocator={0}",
      format = Message.Format.MESSAGE_FORMAT)
   void errorClosingServerLocator(@Cause Exception e, ServerLocatorInternal clusterLocator);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222116, value = "unable to start broadcast group {0}", format = Message.Format.MESSAGE_FORMAT)
   void unableToStartBroadcastGroup(@Cause Exception e, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222117, value = "unable to start cluster connection {0}", format = Message.Format.MESSAGE_FORMAT)
   void unableToStartClusterConnection(@Cause Exception e, SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222118, value = "unable to start Bridge {0}", format = Message.Format.MESSAGE_FORMAT)
   void unableToStartBridge(@Cause Exception e, SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222119, value = "No connector with name {0}. backup cannot be announced.",
      format = Message.Format.MESSAGE_FORMAT)
   void announceBackupNoConnector(String connectorName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222120, value = "no cluster connections defined, unable to announce backup", format = Message.Format.MESSAGE_FORMAT)
   void announceBackupNoClusterConnections();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222121, value = "Must specify a unique name for each bridge. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeNotUnique();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222122, value = "Must specify a queue name for each bridge. This one {0} will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeNoQueue(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222123, value = "Forward address is not specified on bridge {0}. Will use original message address instead", format = Message.Format.MESSAGE_FORMAT)
   void bridgeNoForwardAddress(String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222124, value = "There is already a bridge with name {0} deployed. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeAlreadyDeployed(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222125, value = "No queue found with name {0} bridge {1} will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeQueueNotFound(String queueName, String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222126, value = "No discovery group found with name {0} bridge will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeNoDiscoveryGroup(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222127, value = "Must specify a unique name for each cluster connection. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void clusterConnectionNotUnique();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222128, value = "Must specify an address for each cluster connection. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void clusterConnectionNoForwardAddress();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222129, value = "No connector with name {0}. The cluster connection will not be deployed.",
      format = Message.Format.MESSAGE_FORMAT)
   void clusterConnectionNoConnector(String connectorName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222130,
      value = "Cluster Configuration  {0} already exists. The cluster connection will not be deployed.",
      format = Message.Format.MESSAGE_FORMAT)
   void clusterConnectionAlreadyExists(String connectorName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222131, value = "No discovery group with name {0}. The cluster connection will not be deployed.",
      format = Message.Format.MESSAGE_FORMAT)
   void clusterConnectionNoDiscoveryGroup(String discoveryGroupName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222132, value = "There is already a broadcast-group with name {0} deployed. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void broadcastGroupAlreadyExists(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(
      id = 222133,
      value = "There is no connector deployed with name {0}. The broadcast group with name {1} will not be deployed.",
      format = Message.Format.MESSAGE_FORMAT)
   void broadcastGroupNoConnector(String connectorName, String bgName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222134, value = "No connector defined with name {0}. The bridge will not be deployed.",
      format = Message.Format.MESSAGE_FORMAT)
   void noConnector(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222135, value = "Stopping Redistributor, Timed out waiting for tasks to complete", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingRedistributor();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222136, value = "IO Error during redistribution, errorCode = {0} message = {1}", format = Message.Format.MESSAGE_FORMAT)
   void ioErrorRedistributing(Integer errorCode, String errorMessage);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222137, value = "Unable to announce backup, retrying", format = Message.Format.MESSAGE_FORMAT)
   void errorAnnouncingBackup(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222138, value = "Local Member is not set at on ClusterConnection {0}", format = Message.Format.MESSAGE_FORMAT)
   void noLocalMemborOnClusterConnection(ClusterConnectionImpl clusterConnection);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222139, value = "{0}::Remote queue binding {1} has already been bound in the post office. Most likely cause for this is you have a loop in your cluster due to cluster max-hops being too large or you have multiple cluster connections to the same nodes using overlapping addresses",
      format = Message.Format.MESSAGE_FORMAT)
   void remoteQueueAlreadyBoundOnClusterConnection(Object messageFlowRecord, SimpleString clusterName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222141, value = "Node Manager can not open file {0}", format = Message.Format.MESSAGE_FORMAT)
   void nodeManagerCantOpenFile(@Cause Exception e, File file);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222142, value = "Error on resetting large message deliver - {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorResttingLargeMessage(@Cause Throwable e, Object deliverer);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222143, value = "Timed out waiting for executor to complete", format = Message.Format.MESSAGE_FORMAT)
   void errorTransferringConsumer();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222144, value = "Queue could not finish waiting executors. Try increasing the thread pool size",
      format = Message.Format.MESSAGE_FORMAT)
   void errorFlushingExecutorsOnQueue();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222145, value = "Error expiring reference {0} 0n queue", format = Message.Format.MESSAGE_FORMAT)
   void errorExpiringReferencesOnQueue(@Cause Exception e, MessageReference ref);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222146, value = "Message has expired. No bindings for Expiry Address {0} so dropping it", format = Message.Format.MESSAGE_FORMAT)
   void errorExpiringReferencesNoBindings(SimpleString expiryAddress);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222147, value = "Message has expired. No expiry queue configured for queue {0} so dropping it", format = Message.Format.MESSAGE_FORMAT)
   void errorExpiringReferencesNoQueue(SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222148, value = "Message {0} has exceeded max delivery attempts. No bindings for Dead Letter Address {1} so dropping it",
      format = Message.Format.MESSAGE_FORMAT)
   void messageExceededMaxDelivery(MessageReference ref, SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222149, value = "Message {0} has reached maximum delivery attempts, sending it to Dead Letter Address {1} from {2}",
      format = Message.Format.MESSAGE_FORMAT)
   void messageExceededMaxDeliverySendtoDLA(MessageReference ref, SimpleString name, SimpleString simpleString);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222150, value = "Message {0} has exceeded max delivery attempts. No Dead Letter Address configured for queue {1} so dropping it",
      format = Message.Format.MESSAGE_FORMAT)
   void messageExceededMaxDeliveryNoDLA(MessageReference ref, SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222151, value = "removing consumer which did not handle a message, consumer={0}, message={1}",
      format = Message.Format.MESSAGE_FORMAT)
   void removingBadConsumer(@Cause Throwable e, Consumer consumer, MessageReference reference);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222152, value = "Unable to decrement reference counting on queue",
      format = Message.Format.MESSAGE_FORMAT)
   void errorDecrementingRefCount(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222153, value = "Unable to remove message id = {0} please remove manually",
      format = Message.Format.MESSAGE_FORMAT)
   void errorRemovingMessage(@Cause Throwable e, Long messageID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222154, value = "Error checking DLQ",
      format = Message.Format.MESSAGE_FORMAT)
   void errorCheckingDLQ(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222155, value = "Failed to register as backup. Stopping the server.",
      format = Message.Format.MESSAGE_FORMAT)
   void errorRegisteringBackup();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222156, value = "Less than {0}%\n{1}\nYou are in danger of running out of RAM. Have you set paging parameters on your addresses? (See user manual \"Paging\" chapter)",
      format = Message.Format.MESSAGE_FORMAT)
   void memoryError(Integer memoryWarningThreshold, String info);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222157, value = "Error completing callback on replication manager",
      format = Message.Format.MESSAGE_FORMAT)
   void errorCompletingCallbackOnReplicationManager(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222158, value = "{0} backup activation thread did not finish.", format = Message.Format.MESSAGE_FORMAT)
   void backupActivationDidntFinish(ActiveMQServer server);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222159, value = "unable to send notification when broadcast group is stopped", format = Message.Format.MESSAGE_FORMAT)
   void broadcastBridgeStoppedError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222160, value = "unable to send notification when broadcast group is stopped", format = Message.Format.MESSAGE_FORMAT)
   void notificationBridgeStoppedError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222161, value = "Group Handler timed-out waiting for sendCondition", format = Message.Format.MESSAGE_FORMAT)
   void groupHandlerSendTimeout();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 222162, value = "Moving data directory {0} to {1}", format = Message.Format.MESSAGE_FORMAT)
   void backupMovingDataAway(String oldPath, String newPath);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222163, value = "Server is being completely stopped, since this was a replicated backup there may be journal files that need cleaning up. The Apache ActiveMQ Artemis broker will have to be manually restarted.",
      format = Message.Format.MESSAGE_FORMAT)
   void stopReplicatedBackupAfterFailback();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222164, value = "Error when trying to start replication {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingReplication(BackupReplicationStartFailedMessage.BackupRegistrationProblem problem);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222165, value = "No Dead Letter Address configured for queue {0} in AddressSettings",
      format = Message.Format.MESSAGE_FORMAT)
   void AddressSettingsNoDLA(SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222166, value = "No Expiry Address configured for queue {0} in AddressSettings",
      format = Message.Format.MESSAGE_FORMAT)
   void AddressSettingsNoExpiryAddress(SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222167, value = "Group Binding not available so deleting {0} groups from {1}, groups will be bound to another node",
      format = Message.Format.MESSAGE_FORMAT)
   void groupingQueueRemoved(int size, SimpleString clusterName);

   @SuppressWarnings("deprecation")
   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222168, value = "The ''" + TransportConstants.PROTOCOL_PROP_NAME + "'' property is deprecated. If you want this Acceptor to support multiple protocols, use the ''" + TransportConstants.PROTOCOLS_PROP_NAME + "'' property, e.g. with value ''CORE,AMQP,STOMP''",
      format = Message.Format.MESSAGE_FORMAT)
   void warnDeprecatedProtocol();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222169, value = "You have old legacy clients connected to the queue {0} and we can''t disconnect them, these clients may just hang",
      format = Message.Format.MESSAGE_FORMAT)
   void warnDisconnectOldClient(String queueName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222170, value = "Bridge {0} forwarding address {1} has confirmation-window-size ({2}) greater than address'' max-size-bytes'' ({3})",
      format = Message.Format.MESSAGE_FORMAT)
   void bridgeConfirmationWindowTooSmall(String bridgeName, String address, int windowConfirmation, long maxSizeBytes);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222171, value = "Bridge {0} forwarding address {1} could not be resolved on address-settings configuration",
      format = Message.Format.MESSAGE_FORMAT)
   void bridgeCantFindAddressConfig(String bridgeName, String forwardingAddress);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222172, value = "Queue {0} was busy for more than {1} milliseconds. There are possibly consumers hanging on a network operation",
      format = Message.Format.MESSAGE_FORMAT)
   void queueBusy(String name, long timeout);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222173, value = "Queue {0} is duplicated during reload. This queue will be renamed as {1}", format = Message.Format.MESSAGE_FORMAT)
   void queueDuplicatedRenaming(String name, String newName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222174, value = "Queue {0}, on address={1}, is taking too long to flush deliveries. Watch out for frozen clients.", format = Message.Format.MESSAGE_FORMAT)
   void timeoutFlushInTransit(String queueName, String addressName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222175, value = "Bridge {0} could not find configured connectors", format = Message.Format.MESSAGE_FORMAT)
   void bridgeCantFindConnectors(String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222176,
      value = "A session that was already doing XA work on {0} is replacing the xid by {1} " + ". This was most likely caused from a previous communication timeout",
      format = Message.Format.MESSAGE_FORMAT)
   void xidReplacedOnXStart(String xidOriginalToString, String xidReplacedToString);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222177, value = "Wrong configuration for role, {0} is not a valid permission",
      format = Message.Format.MESSAGE_FORMAT)
   void rolePermissionConfigurationError(String permission);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222178, value = "Error during recovery of page counters",
      format = Message.Format.MESSAGE_FORMAT)
   void errorRecoveringPageCounter(@Cause Throwable error);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222181, value = "Unable to scaleDown messages", format = Message.Format.MESSAGE_FORMAT)
   void failedToScaleDown(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222182, value = "Missing cluster-configuration for scale-down-clustername {0}", format = Message.Format.MESSAGE_FORMAT)
   void missingClusterConfigForScaleDown(String scaleDownCluster);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222183, value = "Blocking message production on address ''{0}''; size is currently: {1} bytes; max-size-bytes: {2}", format = Message.Format.MESSAGE_FORMAT)
   void blockingMessageProduction(SimpleString addressName, long currentSize, long maxSize);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222184,
      value = "Unable to recover group bindings in SCALE_DOWN mode, only FULL backup server can do this",
      format = Message.Format.MESSAGE_FORMAT)
   void groupBindingsOnRecovery();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222185,
      value = "no cluster connection for specified replication cluster",
      format = Message.Format.MESSAGE_FORMAT)
   void noClusterConnectionForReplicationCluster();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222186,
      value = "unable to authorise cluster control",
      format = Message.Format.MESSAGE_FORMAT)
   void clusterControlAuthfailure();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222187,
      value = "Failed to activate replicated backup",
      format = Message.Format.MESSAGE_FORMAT)
   void activateReplicatedBackupFailed(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222188,
      value = "Unable to find target queue for node {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void unableToFindTargetQueue(String targetNodeID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222189,
      value = "Failed to activate shared store slave",
      format = Message.Format.MESSAGE_FORMAT)
   void activateSharedStoreSlaveFailed(@Cause Throwable e);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 222190, value = "Deleting old data directory {0} as the max folders is set to 0", format = Message.Format.MESSAGE_FORMAT)
   void backupDeletingData(String oldPath);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222191,
      value = "Could not find any configured role for user {0}.",
      format = Message.Format.MESSAGE_FORMAT)
   void cannotFindRoleForUser(String user);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222192,
      value = "Could not delete: {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void couldNotDeleteTempFile(String tempFileName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222193,
      value = "Memory Limit reached. Producer ({0}) stopped to prevent flooding {1} (blocking for {2}s). See http://activemq.apache.org/producer-flow-control.html for more info.",
      format = Message.Format.MESSAGE_FORMAT)
   void memoryLimitReached(String producerID, String address, long duration);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222194,
      value = "PageCursorInfo == null on address {0}, pos = {1}, queue = {2}.",
      format = Message.Format.MESSAGE_FORMAT)
   void nullPageCursorInfo(String address, String position, long id);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222195,
      value = "Large message {0} wasn''t found when dealing with add pending large message",
      format = Message.Format.MESSAGE_FORMAT)
   void largeMessageNotFound(long id);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222196,
      value = "Could not find binding with id={0} on routeFromCluster for message={1} binding = {2}",
      format = Message.Format.MESSAGE_FORMAT)
   void bindingNotFound(long id, String message, String binding);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222197,
      value = "Internal error! Delivery logic has identified a non delivery and still handled a consumer!",
      format = Message.Format.MESSAGE_FORMAT)
   void nonDeliveryHandled();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222198,
      value = "Could not flush ClusterManager executor ({0}) in 10 seconds, verify your thread pool size",
      format = Message.Format.MESSAGE_FORMAT)
   void couldNotFlushClusterManager(String manager);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222199,
      value = "Thread dump: {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void threadDump(String manager);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222200,
      value = "Could not finish executor on {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void couldNotFinishExecutor(String clusterConnection);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222201,
      value = "Timed out waiting for backup activation to exit",
      format = Message.Format.MESSAGE_FORMAT)
   void backupActivationTimeout();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222202,
      value = "{0}: <{1}> should not be set to the same value as <{2}>.  " +
         "If a system is under high load, or there is a minor network delay, " +
         "there is a high probability of a cluster split/failure due to connection timeout.",
      format = Message.Format.MESSAGE_FORMAT)
   void connectionTTLEqualsCheckPeriod(String connectionName, String ttl, String checkPeriod);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222203, value = "Classpath lacks a protocol-manager for protocol {0}, Protocol being ignored on acceptor {1}",
      format = Message.Format.MESSAGE_FORMAT)
   void noProtocolManagerFound(String protocol, String host);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222204, value = "Duplicated Acceptor {0} with parameters {1} classFactory={2} duplicated on the configuration", format = Message.Format.MESSAGE_FORMAT)
   void duplicatedAcceptor(String name, String parameters, String classFactory);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222205, value = "OutOfMemoryError possible! There are currently {0} addresses with a total max-size-bytes of {1} bytes, but the maximum memory available is {2} bytes.", format = Message.Format.MESSAGE_FORMAT)
   void potentialOOME(long addressCount, long totalMaxSizeBytes, long maxMemory);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222206, value = "Connection limit of {0} reached. Refusing connection from {1}.", format = Message.Format.MESSAGE_FORMAT)
   void connectionLimitReached(long connectionsAllowed, String address);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222207, value = "The backup server is not responding promptly introducing latency beyond the limit. Replication server being disconnected now.",
      format = Message.Format.MESSAGE_FORMAT)
   void slowReplicationResponse();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222208, value = "SSL handshake failed for client from {0}: {1}.",
      format = Message.Format.MESSAGE_FORMAT)
   void sslHandshakeFailed(String clientAddress, String cause);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222209, value = "Could not contact group handler coordinator after 10 retries, message being routed without grouping information",
      format = Message.Format.MESSAGE_FORMAT)
   void impossibleToRouteGrouped();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222210, value = "Storage usage is beyond max-disk-usage. System will start blocking producers.",
      format = Message.Format.MESSAGE_FORMAT)
   void diskBeyondCapacity();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222211, value = "Storage is back to stable now, under max-disk-usage.",
      format = Message.Format.MESSAGE_FORMAT)
   void diskCapacityRestored();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222212, value = "Disk Full! Blocking message production on address ''{0}''. Clients will report blocked.", format = Message.Format.MESSAGE_FORMAT)
   void blockingDiskFull(SimpleString addressName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222213,
      value = "There was an issue on the network, server is isolated!",
      format = Message.Format.MESSAGE_FORMAT)
   void serverIsolatedOnNetwork();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222214,
      value = "Destination {1} has an inconsistent and negative address size={0}.",
      format = Message.Format.MESSAGE_FORMAT)
   void negativeAddressSize(long size, String destination);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222215,
      value = "Global Address Size has negative and inconsistent value as {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void negativeGlobalAddressSize(long size);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222216, value = "Security problem while creating session: {0}", format = Message.Format.MESSAGE_FORMAT)
   void securityProblemWhileCreatingSession(String message);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 222217, value = "Cannot find connector-ref {0}. The cluster-connection {1} will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void connectorRefNotFound(String connectorRef, String clusterConnection);


   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224000, value = "Failure in initialisation", format = Message.Format.MESSAGE_FORMAT)
   void initializationError(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224001, value = "Error deploying URI {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorDeployingURI(@Cause Throwable e, URI uri);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224002, value = "Error deploying URI", format = Message.Format.MESSAGE_FORMAT)
   void errorDeployingURI(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224003, value = "Error undeploying URI {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorUnDeployingURI(@Cause Throwable e, URI a);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224005, value = "Unable to deploy node {0}", format = Message.Format.MESSAGE_FORMAT)
   void unableToDeployNode(@Cause Exception e, Node node);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224006, value = "Invalid filter: {0}", format = Message.Format.MESSAGE_FORMAT)
   void invalidFilter(SimpleString filter);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224007, value = "page subscription = {0} error={1}", format = Message.Format.MESSAGE_FORMAT)
   void pageSubscriptionError(IOCallback IOCallback, String error);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224008, value = "Failed to store id", format = Message.Format.MESSAGE_FORMAT)
   void batchingIdError(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224009, value = "Cannot find message {0}", format = Message.Format.MESSAGE_FORMAT)
   void cannotFindMessage(Long id);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224010, value = "Cannot find queue messages for queueID={0} on ack for messageID={1}", format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueue(Long queue, Long id);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224011, value = "Cannot find queue messages {0} for message {1} while processing scheduled messages", format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueueScheduled(Long queue, Long id);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224012, value = "error releasing resources", format = Message.Format.MESSAGE_FORMAT)
   void largeMessageErrorReleasingResources(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224013, value = "failed to expire messages for queue", format = Message.Format.MESSAGE_FORMAT)
   void errorExpiringMessages(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224014, value = "Failed to close session", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224015, value = "Caught XA exception", format = Message.Format.MESSAGE_FORMAT)
   void caughtXaException(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224016, value = "Caught exception", format = Message.Format.MESSAGE_FORMAT)
   void caughtException(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224017, value = "Invalid packet {0}", format = Message.Format.MESSAGE_FORMAT)
   void invalidPacket(Packet packet);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224018, value = "Failed to create session", format = Message.Format.MESSAGE_FORMAT)
   void failedToCreateSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224019, value = "Failed to reattach session", format = Message.Format.MESSAGE_FORMAT)
   void failedToReattachSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224020, value = "Failed to handle create queue", format = Message.Format.MESSAGE_FORMAT)
   void failedToHandleCreateQueue(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224021, value = "Failed to decode packet", format = Message.Format.MESSAGE_FORMAT)
   void errorDecodingPacket(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224022, value = "Failed to execute failure listener", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingFailureListener(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224024, value = "Stomp Error, tx already exist! {0}", format = Message.Format.MESSAGE_FORMAT)
   void stompErrorTXExists(String txID);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224027, value = "Failed to write to handler on invm connector {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorWritingToInvmConnector(@Cause Exception e, Runnable runnable);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224028, value = "Failed to stop acceptor {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingAcceptor(String name);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224029, value = "large message sync: largeMessage instance is incompatible with it, ignoring data", format = Message.Format.MESSAGE_FORMAT)
   void largeMessageIncompatible();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224030, value = "Could not cancel reference {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorCancellingRefOnBridge(@Cause Exception e, MessageReference ref2);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224031, value = "-------------------------------Stomp begin tx: {0}", format = Message.Format.MESSAGE_FORMAT)
   void stompBeginTX(String txID);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224032, value = "Failed to pause bridge", format = Message.Format.MESSAGE_FORMAT)
   void errorPausingBridge(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224033, value = "Failed to broadcast connector configs", format = Message.Format.MESSAGE_FORMAT)
   void errorBroadcastingConnectorConfigs(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224034, value = "Failed to close consumer", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingConsumer(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224035, value = "Failed to close cluster connection flow record", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingFlowRecord(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224036, value = "Failed to update cluster connection topology", format = Message.Format.MESSAGE_FORMAT)
   void errorUpdatingTopology(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224037, value = "cluster connection Failed to handle message", format = Message.Format.MESSAGE_FORMAT)
   void errorHandlingMessage(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224038, value = "Failed to ack old reference", format = Message.Format.MESSAGE_FORMAT)
   void errorAckingOldReference(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224039, value = "Failed to expire message reference", format = Message.Format.MESSAGE_FORMAT)
   void errorExpiringRef(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224040, value = "Failed to remove consumer", format = Message.Format.MESSAGE_FORMAT)
   void errorRemovingConsumer(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224041, value = "Failed to deliver", format = Message.Format.MESSAGE_FORMAT)
   void errorDelivering(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224042, value = "Error while restarting the backup server: {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorRestartingBackupServer(@Cause Exception e, ActiveMQServer backup);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224043, value = "Failed to send forced delivery message", format = Message.Format.MESSAGE_FORMAT)
   void errorSendingForcedDelivery(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224044, value = "error acknowledging message", format = Message.Format.MESSAGE_FORMAT)
   void errorAckingMessage(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224045, value = "Failed to run large message deliverer", format = Message.Format.MESSAGE_FORMAT)
   void errorRunningLargeMessageDeliverer(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224046, value = "Exception while browser handled from {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorBrowserHandlingMessage(@Cause Exception e, MessageReference current);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224047, value = "Failed to delete large message file", format = Message.Format.MESSAGE_FORMAT)
   void errorDeletingLargeMessageFile(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224048, value = "Failed to remove temporary queue {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorRemovingTempQueue(@Cause Exception e, SimpleString bindingName);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224049, value = "Cannot find consumer with id {0}", format = Message.Format.MESSAGE_FORMAT)
   void cannotFindConsumer(long consumerID);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224050, value = "Failed to close connection {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingConnection(ServerSessionImpl serverSession);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224051, value = "Failed to call notification listener", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingNotifListener(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224052, value = "Unable to call Hierarchical Repository Change Listener", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingRepoListener(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224053, value = "failed to timeout transaction, xid:{0}", format = Message.Format.MESSAGE_FORMAT)
   void errorTimingOutTX(@Cause Exception e, Xid xid);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224054, value = "exception while stopping the replication manager", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingReplicationManager(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224055, value = "Bridge Failed to ack", format = Message.Format.MESSAGE_FORMAT)
   void bridgeFailedToAck(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224056, value = "Live server will not fail-back automatically", format = Message.Format.MESSAGE_FORMAT)
   void autoFailBackDenied();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224057, value = "Backup server that requested fail-back was not announced. Server will not stop for fail-back.",
      format = Message.Format.MESSAGE_FORMAT)
   void failbackMissedBackupAnnouncement();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224058, value = "Stopping ClusterManager. As it failed to authenticate with the cluster: {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void clusterManagerAuthenticationError(String msg);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224059, value = "Invalid cipher suite specified. Supported cipher suites are: {0}", format = Message.Format.MESSAGE_FORMAT)
   void invalidCipherSuite(String validSuites);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224060, value = "Invalid protocol specified. Supported protocols are: {0}", format = Message.Format.MESSAGE_FORMAT)
   void invalidProtocol(String validProtocols);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224061, value = "Setting both <{0}> and <ha-policy> is invalid. Please use <ha-policy> exclusively as <{0}> is deprecated. Ignoring <{0}> value.", format = Message.Format.MESSAGE_FORMAT)
   void incompatibleWithHAPolicy(String parameter);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224062, value = "Failed to send SLOW_CONSUMER notification: {0}", format = Message.Format.MESSAGE_FORMAT)
   void failedToSendSlowConsumerNotification(Notification notification, @Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224063, value = "Failed to close consumer connections for address {0}", format = Message.Format.MESSAGE_FORMAT)
   void failedToCloseConsumerConnectionsForAddress(String address, @Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224064, value = "Setting <{0}> is invalid with this HA Policy Configuration. Please use <ha-policy> exclusively or remove. Ignoring <{0}> value.", format = Message.Format.MESSAGE_FORMAT)
   void incompatibleWithHAPolicyChosen(String parameter);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224065, value = "Failed to remove auto-created queue {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorRemovingAutoCreatedQueue(@Cause Exception e, SimpleString bindingName);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224066, value = "Error opening context for LDAP", format = Message.Format.MESSAGE_FORMAT)
   void errorOpeningContextForLDAP(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224067, value = "Error populating security roles from LDAP", format = Message.Format.MESSAGE_FORMAT)
   void errorPopulatingSecurityRolesFromLDAP(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224068, value = "Unable to stop component: {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingComponent(@Cause Throwable t, String componentClassName);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224069, value = "Change detected in broker configuration file, but reload failed", format = Message.Format.MESSAGE_FORMAT)
   void configurationReloadFailed(@Cause Throwable t);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 224072, value = "Message Counter Sample Period too short: {0}", format = Message.Format.MESSAGE_FORMAT)
   void invalidMessageCounterPeriod(long value);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 224073, value = "Using MAPPED Journal", format = Message.Format.MESSAGE_FORMAT)
   void journalUseMAPPED();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 224074, value = "Failed to purge queue {0} on no consumers", format = Message.Format.MESSAGE_FORMAT)
   void failedToPurgeQueue(@Cause Exception e, SimpleString bindingName);

}
