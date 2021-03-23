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

import java.io.File;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQAddressDoesNotExistException;
import org.apache.activemq.artemis.api.core.ActiveMQAddressExistsException;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.apache.activemq.artemis.api.core.ActiveMQClusterSecurityException;
import org.apache.activemq.artemis.api.core.ActiveMQConnectionTimedOutException;
import org.apache.activemq.artemis.api.core.ActiveMQDeleteAddressException;
import org.apache.activemq.artemis.api.core.ActiveMQDisconnectedException;
import org.apache.activemq.artemis.api.core.ActiveMQDivertDoesNotExistException;
import org.apache.activemq.artemis.api.core.ActiveMQDuplicateMetaDataException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIOErrorException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.ActiveMQIncompatibleClientServerException;
import org.apache.activemq.artemis.api.core.ActiveMQInternalErrorException;
import org.apache.activemq.artemis.api.core.ActiveMQInvalidFilterExpressionException;
import org.apache.activemq.artemis.api.core.ActiveMQInvalidQueueConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQInvalidTransientQueueUseException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueMaxConsumerLimitReached;
import org.apache.activemq.artemis.api.core.ActiveMQReplicationTimeooutException;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.ActiveMQSessionCreationException;
import org.apache.activemq.artemis.api.core.ActiveMQUnexpectedRoutingTypeForAddress;
import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationSyncFileMessage;
import org.apache.activemq.artemis.core.security.CheckType;
import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Logger Code 22
 * <p>
 * Each message id must be 6 digits long starting with 22, the 3rd digit should be 9. So the range
 * is from 229000 to 229999.
 * <p>
 * Once released, methods should not be deleted as they may be referenced by knowledge base
 * articles. Unused methods should be marked as deprecated.
 */
@MessageBundle(projectCode = "AMQ")
public interface ActiveMQMessageBundle {

   ActiveMQMessageBundle BUNDLE = Messages.getBundle(ActiveMQMessageBundle.class);

   @Message(id = 229000, value = "Activation for server {0}", format = Message.Format.MESSAGE_FORMAT)
   String activationForServer(ActiveMQServer server);

   @Message(id = 229001, value = "Generating thread dump", format = Message.Format.MESSAGE_FORMAT)
   String generatingThreadDump();

   @Message(id = 229002, value = "Thread {0} name = {1} id = {2} group = {3}", format = Message.Format.MESSAGE_FORMAT)
   String threadDump(Thread key, String name, Long id, ThreadGroup threadGroup);

   @Message(id = 229003, value = "End Thread dump")
   String endThreadDump();

   @Message(id = 229004, value = "Information about server {0}\nCluster Connection:{1}", format = Message.Format.MESSAGE_FORMAT)
   String serverDescribe(String identity, String describe);

   @Message(id = 229005, value = "connections for {0} closed by management", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException connectionsClosedByManagement(String ipAddress);

   @Message(id = 229006, value = "journals are not JournalImpl. You can''t set a replicator!")
   ActiveMQInternalErrorException notJournalImpl();

   @Message(id = 229007, value = "unhandled error during replication")
   ActiveMQInternalErrorException replicationUnhandledError(@Cause Exception e);

   @Message(id = 229008, value = "Live Node contains more journals than the backup node. Probably a version match error")
   ActiveMQInternalErrorException replicationTooManyJournals();

   @Message(id = 229009, value = "Unhandled file type {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException replicationUnhandledFileType(ReplicationSyncFileMessage.FileType fileType);

   @Message(id = 229010, value = "Remote Backup can not be up-to-date!")
   ActiveMQInternalErrorException replicationBackupUpToDate();

   @Message(id = 229011, value = "unhandled data type!")
   ActiveMQInternalErrorException replicationUnhandledDataType();

   @Message(id = 229012, value = "No binding for divert {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException noBindingForDivert(SimpleString name);

   @Message(id = 229013, value = "Binding {0} is not a divert", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException bindingNotDivert(SimpleString name);

   @Message(id = 229014,
      value = "Did not receive data from {0} within the {1}ms connection TTL. The connection will now be closed.", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQConnectionTimedOutException clientExited(String remoteAddress, long ttl);

   @Message(id = 229015, value = "Must specify a name for each divert. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException divertWithNoName();

   @Message(id = 229017, value = "Queue {0} does not exist", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQNonExistentQueueException noSuchQueue(SimpleString queueName);

   @Message(id = 229018, value = "Binding already exists {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQQueueExistsException bindingAlreadyExists(Binding binding);

   @Message(id = 229019, value = "Queue {0} already exists on address {1}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQQueueExistsException queueAlreadyExists(SimpleString queueName, SimpleString addressName);

   @Message(id = 229020, value = "Invalid filter: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInvalidFilterExpressionException invalidFilter(@Cause Throwable e, SimpleString filter);

   @Message(id = 229021, value = "MessageId was not assigned to Message")
   ActiveMQIllegalStateException messageIdNotAssigned();

   @Message(id = 229022, value = "Cannot compare journals if not in sync!")
   ActiveMQIllegalStateException journalsNotInSync();

   @Message(id = 229023, value = "Connected server is not a backup server")
   ActiveMQIllegalStateException serverNotBackupServer();

   @Message(id = 229024, value = "Backup replication server is already connected to another server")
   ActiveMQIllegalStateException alreadyHaveReplicationServer();

   @Message(id = 229025, value = "Cannot delete queue {0} on binding {1} - it has consumers = {2}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException cannotDeleteQueueWithConsumers(SimpleString name, SimpleString queueName, String s);

   @Message(id = 229026, value = "Backup Server was not yet in sync with live")
   ActiveMQIllegalStateException backupServerNotInSync();

   @Message(id = 229027, value = "Could not find reference on consumer ID={0}, messageId = {1} queue = {2}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException consumerNoReference(Long id, Long messageID, SimpleString name);

   @Message(id = 229028, value = "Consumer {0} doesn''t exist on the server", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException consumerDoesntExist(long consumerID);

   @Message(id = 229029, value = "No address configured on the Server''s Session")
   ActiveMQIllegalStateException noAddress();

   @Message(id = 229030, value = "large-message not initialized on server")
   ActiveMQIllegalStateException largeMessageNotInitialised();

   @Message(id = 229031, value = "Unable to validate user from {0}. Username: {1}; SSL certificate subject DN: {2}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQSecurityException unableToValidateUser(String remoteAddress, String user, String certMessage);

   @Message(id = 229032, value = "User: {0} does not have permission=''{1}'' on address {2}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQSecurityException userNoPermissions(String username, CheckType checkType, SimpleString address);

   @Message(id = 229033, value = "Server and client versions incompatible")
   ActiveMQIncompatibleClientServerException incompatibleClientServer();

   @Message(id = 229034, value = "Server not started")
   ActiveMQSessionCreationException serverNotStarted();

   @Message(id = 229035, value = "Metadata {0}={1} had been set already", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQDuplicateMetaDataException duplicateMetadata(String key, String data);

   @Message(id = 229036, value = "Invalid type: {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidType(Object type);

   @Message(id = 229038, value = "{0} must neither be null nor empty", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException emptyOrNull(String name);

   @Message(id = 229039, value = "{0}  must be greater than 0 (actual value: {1})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException greaterThanZero(String name, Number val);

   @Message(id = 229040, value = "{0} must be a valid percentual value between 0 and 100 (actual value: {1})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException notPercent(String name, Number val);

   @Message(id = 229041, value = "{0}  must be equals to -1 or greater than 0 (actual value: {1})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException greaterThanMinusOne(String name, Number val);

   @Message(id = 229042, value = "{0}  must be equals to -1 or greater or equals to 0 (actual value: {1})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException greaterThanZeroOrMinusOne(String name, Number val);

   @Message(id = 229043, value = "{0} must be between {1} and {2} inclusive (actual value: {3})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException mustbeBetween(String name, Integer minPriority, Integer maxPriority, Object value);

   @Message(id = 229044, value = "Invalid journal type {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidJournalType(String val);

   @Message(id = 229045, value = "Invalid address full message policy type {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidAddressFullPolicyType(String val);

   @Message(id = 229046, value = "invalid value: {0} count must be greater than 0", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException greaterThanZero(Integer count);

   @Message(id = 229047, value = "invalid value: {0} sample period must be greater than 0", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException periodMustGreaterThanZero(Long newPeriod);

   @Message(id = 229048, value = "invalid new Priority value: {0}. It must be between 0 and 9 (both included)", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidNewPriority(Integer period);

   @Message(id = 229049, value = "No queue found for {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noQueueFound(String otherQueueName);

   @Message(id = 229050, value = "Only NIO and AsyncIO are supported journals")
   IllegalArgumentException invalidJournal();

   @Message(id = 229051, value = "Invalid journal type {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidJournalType2(JournalType journalType);

   @Message(id = 229052, value = "Directory {0} does not exist and cannot be created", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException cannotCreateDir(String dir);

   @Message(id = 229054, value = "Cannot convert to int")
   IllegalArgumentException cannotConvertToInt();

   @Message(id = 229055, value = "Routing name is null")
   IllegalArgumentException routeNameIsNull();

   @Message(id = 229056, value = "Cluster name is null")
   IllegalArgumentException clusterNameIsNull();

   @Message(id = 229057, value = "Address is null")
   IllegalArgumentException addressIsNull();

   @Message(id = 229058, value = "Binding type not specified")
   IllegalArgumentException bindingTypeNotSpecified();

   @Message(id = 229059, value = "Binding ID is null")
   IllegalArgumentException bindingIdNotSpecified();

   @Message(id = 229060, value = "Distance is null")
   IllegalArgumentException distancenotSpecified();

   @Message(id = 229061, value = "Connection already exists with id {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException connectionExists(Object id);

   @Message(id = 229062, value = "Acceptor with id {0} already registered", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException acceptorExists(Integer id);

   @Message(id = 229063, value = "Acceptor with id {0} not registered", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException acceptorNotExists(Integer id);

   @Message(id = 229064, value = "Unknown protocol {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException unknownProtocol(String protocol);

   @Message(id = 229065, value = "node id is null")
   IllegalArgumentException nodeIdNull();

   @Message(id = 229066, value = "Queue name is null")
   IllegalArgumentException queueNameIsNull();

   @Message(id = 229067, value = "Cannot find resource with name {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException cannotFindResource(String resourceName);

   @Message(id = 229068, value = "no getter method for {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noGetterMethod(String resourceName);

   @Message(id = 229069, value = "no operation {0}/{1}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noOperation(String operation, Integer length);

   @Message(id = 229070, value = "match can not be null")
   IllegalArgumentException nullMatch();

   @Message(id = 229071, value = "# can only be at end of match")
   IllegalArgumentException invalidMatch();

   @Message(id = 229072, value = "User cannot be null")
   IllegalArgumentException nullUser();

   @Message(id = 229073, value = "Password cannot be null")
   IllegalArgumentException nullPassword();

   @Message(id = 229074, value = "Error instantiating transformer class {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException errorCreatingTransformerClass(@Cause Exception e, String transformerClassName);

   @Message(id = 229075, value = "method autoEncode doesn''t know how to convert {0} yet", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException autoConvertError(Class<? extends Object> aClass);

   /**
    * Message used on on {@link org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl#destroyConnectionWithSessionMetadata(String, String)}
    */
   @Message(id = 229076, value = "Executing destroyConnection with {0}={1} through management''s request", format = Message.Format.MESSAGE_FORMAT)
   String destroyConnectionWithSessionMetadataHeader(String key, String value);

   /**
    * Message used on on {@link org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl#destroyConnectionWithSessionMetadata(String, String)}
    */
   @Message(id = 229077, value = "Closing connection {0}", format = Message.Format.MESSAGE_FORMAT)
   String destroyConnectionWithSessionMetadataClosingConnection(String serverSessionString);

   /**
    * Exception used on on {@link org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl#destroyConnectionWithSessionMetadata(String, String)}
    */
   @Message(id = 229078, value = "Disconnected per admin''s request on {0}={1}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQDisconnectedException destroyConnectionWithSessionMetadataSendException(String key, String value);

   /**
    * Message used on on {@link org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl#destroyConnectionWithSessionMetadata(String, String)}
    */
   @Message(id = 229079, value = "No session found with {0}={1}", format = Message.Format.MESSAGE_FORMAT)
   String destroyConnectionWithSessionMetadataNoSessionFound(String key, String value);

   @Message(id = 229080, value = "Invalid Page IO, PagingManager was stopped or closed")
   ActiveMQIllegalStateException invalidPageIO();

   @Message(id = 229081, value = "No Discovery Group configuration named {0} found", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQException noDiscoveryGroupFound(DiscoveryGroupConfiguration dg);

   @Message(id = 229082, value = "Queue {0} already exists on another subscription", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInvalidTransientQueueUseException queueSubscriptionBelongsToDifferentAddress(SimpleString queueName);

   @Message(id = 229083, value = "Queue {0} has a different filter than requested", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInvalidTransientQueueUseException queueSubscriptionBelongsToDifferentFilter(SimpleString queueName);

   // this code has to match with version 2.3.x as it's used on integration tests at Wildfly and JBoss EAP
   @Message(id = 229099, value = "Unable to authenticate cluster user: {0}",
      format = Message.Format.MESSAGE_FORMAT)
   ActiveMQClusterSecurityException unableToValidateClusterUser(String user);

   @Message(id = 229100, value = "Trying to move a journal file that refers to a file instead of a directory: {0}",
      format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException journalDirIsFile(File fDir);

   @Message(id = 229101, value = "error trying to backup journal files at directory: {0}",
      format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException couldNotMoveJournal(File dir);

   @Message(id = 229102, value = "Address \"{0}\" is full.", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAddressFullException addressIsFull(String addressName);

   @Message(id = 229103, value = "No Connectors or Discovery Groups configured for Scale Down")
   ActiveMQException noConfigurationFoundForScaleDown();

   @Message(id = 229104, value = "Server is stopping. Message grouping not allowed")
   ActiveMQException groupWhileStopping();

   @Message(id = 229106, value = "Invalid slow consumer policy type {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidSlowConsumerPolicyType(String val);

   @Message(id = 229107, value = "consumer connections for address {0} closed by management", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException consumerConnectionsClosedByManagement(String address);

   @Message(id = 229108, value = "connections for user {0} closed by management", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException connectionsForUserClosedByManagement(String userName);

   @Message(id = 229109, value = "unsupported HA Policy Configuration {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException unsupportedHAPolicyConfiguration(Object o);

   @Message(id = 229110, value = "Too many sessions for user ''{0}''. Sessions allowed: {1}.", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQSessionCreationException sessionLimitReached(String username, int limit);

   @Message(id = 229111, value = "Too many queues created by user ''{0}''. Queues allowed: {1}.", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQSecurityException queueLimitReached(String username, int limit);

   @Message(id = 229112, value = "Cannot set MBeanServer during startup or while started")
   IllegalStateException cannotSetMBeanserver();

   @Message(id = 229113, value = "Invalid message load balancing type {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidMessageLoadBalancingType(String val);

   @Message(id = 229114, value = "Replication synchronization process timed out after waiting {0} milliseconds", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQReplicationTimeooutException replicationSynchronizationTimeout(long timeout);

   @Message(id = 229115, value = "Colocated Policy hasn''t different type live and backup", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException liveBackupMismatch();

   @Message(id = 229116, value = "Netty Acceptor unavailable", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException acceptorUnavailable();

   @Message(id = 229117, value = "Replicator is null. Replication was likely terminated.")
   ActiveMQIllegalStateException replicatorIsNull();

   @Message(id = 229118, value = "Management method not applicable for current server configuration")
   IllegalStateException methodNotApplicable();

   @Message(id = 229119, value = "Free storage space is at {0} of {1} total. Usage rate is {2} which is beyond the configured <max-disk-usage>. System will start blocking producers.", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIOErrorException diskBeyondLimit(String usableSpace, String totalSpace, String usage);

   @Message(id = 229120, value = "connection with ID {0} closed by management", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException connectionWithIDClosedByManagement(String ID);

   @Message(id = 229200, value = "Maximum Consumer Limit Reached on Queue:(address={0},queue={1})", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQQueueMaxConsumerLimitReached maxConsumerLimitReachedForQueue(SimpleString address, SimpleString queueName);

   @Message(id = 229201, value = "Expected Routing Type {1} but found {2} for address {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQUnexpectedRoutingTypeForAddress unexpectedRoutingTypeForAddress(SimpleString address, RoutingType expectedRoutingType, Set<RoutingType> supportedRoutingTypes);

   @Message(id = 229202, value = "Invalid Queue Configuration for Queue {0}, Address {1}.  Expected {2} to be {3} but was {4}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInvalidQueueConfiguration invalidQueueConfiguration(SimpleString address, SimpleString queueName, String queuePropertyName, Object expectedValue, Object actualValue);

   @Message(id = 229203, value = "Address Does Not Exist: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAddressDoesNotExistException addressDoesNotExist(SimpleString address);

   @Message(id = 229204, value = "Address already exists: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAddressExistsException addressAlreadyExists(SimpleString address);

   @Message(id = 229205, value = "Address {0} has bindings", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQDeleteAddressException addressHasBindings(SimpleString address);

   @Message(id = 229206, value = "Queue {0} has invalid max consumer setting: {1}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidMaxConsumers(String queueName, int value);

   @Message(id = 229207, value = "Can not create queue with routing type: {0}, Supported routing types for address: {1} are {2}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidRoutingTypeForAddress(RoutingType routingType,
                                                         String address,
                                                         Set<RoutingType> supportedRoutingTypes);

   @Message(id = 229208, value = "Invalid routing type {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidRoutingType(String val);

   @Message(id = 229209, value = "Can''t remove routing type {0}, queues exists for address: {1}. Please delete queues before removing this routing type.", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException invalidRoutingTypeDelete(RoutingType routingType, String address);

   @Message(id = 229210, value = "Can''t update queue {0} with maxConsumers: {1}. Current consumers are {2}.", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException invalidMaxConsumersUpdate(String queueName, int maxConsumers, int consumers);

   @Message(id = 229211, value = "Can''t update queue {0} with routing type: {1}, Supported routing types for address: {2} are {3}", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException invalidRoutingTypeUpdate(String queueName,
                                                  RoutingType routingType,
                                                  String address,
                                                  Set<RoutingType> supportedRoutingTypes);

   @Message(id = 229212, value = "Invalid deletion policy type {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidDeletionPolicyType(String val);

   @Message(id = 229213, value = "User: {0} does not have permission=''{1}'' for queue {2} on address {3}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQSecurityException userNoPermissionsQueue(String username, CheckType checkType, SimpleString queue, SimpleString address);

   @Message(id = 229214, value = "{0} must be a valid percentage value between 0 and 100 or -1 (actual value: {1})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException notPercentOrMinusOne(String name, Number val);

   @Message(id = 229215, value = "Cannot delete queue {0} on binding {1} - it has {2} messages", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException cannotDeleteQueueWithMessages(SimpleString name, SimpleString queueName, long messageCount);

   @Message(id = 229216, value = "Invalid queue name: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException invalidQueueName(SimpleString queueName);

   @Message(id = 119217, value = "Cannot write to closed file: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIOErrorException cannotWriteToClosedFile(SequentialFile file);

   @Message(id = 229218, value = "Failed to locate broker configuration URL")
   ActiveMQIllegalStateException failedToLocateConfigURL();

   @Message(id = 229219, value = "Failed to load security configuration")
   ActiveMQIllegalStateException failedToLoadSecurityConfig();

   @Message(id = 229220, value = "Failed to load user file: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException failedToLoadUserFile(String path);

   @Message(id = 229221, value = "Failed to load role file: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException failedToLoadRoleFile(String path);

   @Message(id = 229222, value = "Failed to find login module entry {0} from JAAS configuration", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException failedToFindLoginModuleEntry(String entry);

   @Message(id = 229223, value = "User {0} already exists", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException userAlreadyExists(String user);

   @Message(id = 229224, value = "User {0} does not exist", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException userDoesNotExist(String user);

   @Message(id = 229225, value = "Validated User is not set", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException rejectEmptyValidatedUser();

   @Message(id = 229226, value = "{0}  must be greater than 0 and less than or equal to Integer.MAX_VALUE (actual value: {1})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException inRangeOfPositiveInt(String name, Number val);

   @Message(id = 229227, value = "{0}  must be equals to -1 or greater than 0 and less than or equal to Integer.MAX_VALUE (actual value: {1})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException inRangeOfPositiveIntThanMinusOne(String name, Number val);

   @Message(id = 229228, value = "{0} must be less than or equal to 1 (actual value: {1})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException lessThanOrEqualToOne(String name, Number val);

   @Message(id = 229229, value = "Failed to parse JSON queue configuration: {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException failedToParseJson(String json);

   @Message(id = 229230, value = "Failed to bind acceptor {0} to {1}", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException failedToBind(String acceptor, String hostPort, @Cause Exception e);

   @Message(id = 229231, value = "Divert Does Not Exist: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQDivertDoesNotExistException divertDoesNotExist(String divert);

   @Message(id = 229232, value = "Cannot create consumer on {0}. Session is closed.", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException cannotCreateConsumerOnClosedSession(SimpleString queueName);

   @Message(id = 229233, value = "Cannot set ActiveMQSecurityManager during startup or while started")
   IllegalStateException cannotSetSecurityManager();
}
