/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.logs;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import javax.security.auth.Subject;
import java.security.AccessController;
import java.security.Principal;
import java.util.Arrays;

/**
 * Logger Code 60
 *
 * each message id must be 6 digits long starting with 22, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 601000 to 601999
 */
@MessageLogger(projectCode = "AMQ")
public interface AuditLogger extends BasicLogger {

   AuditLogger LOGGER = Logger.getMessageLogger(AuditLogger.class, "org.apache.activemq.audit.base");
   AuditLogger MESSAGE_LOGGER = Logger.getMessageLogger(AuditLogger.class, "org.apache.activemq.audit.message");

   static boolean isEnabled() {
      return LOGGER.isEnabled(Logger.Level.INFO);
   }

   static boolean isMessageEnabled() {
      return MESSAGE_LOGGER.isEnabled(Logger.Level.INFO);
   }

   static String getCaller() {
      Subject subject = Subject.getSubject(AccessController.getContext());
      String caller = "anonymous";
      if (subject != null) {
         caller = "";
         for (Principal principal : subject.getPrincipals()) {
            caller += principal.getName() + "|";
         }
      }
      return caller;
   }

   static String arrayToString(Object value) {
      if (value == null) return "";

      final String prefix = "with parameters: ";

      if (value instanceof long[]) {
         return prefix + Arrays.toString((long[])value);
      } else if (value instanceof int[]) {
         return prefix + Arrays.toString((int[])value);
      } else if (value instanceof char[]) {
         return prefix + Arrays.toString((char[])value);
      } else if (value instanceof byte[]) {
         return prefix + Arrays.toString((byte[])value);
      } else if (value instanceof float[]) {
         return prefix + Arrays.toString((float[])value);
      } else if (value instanceof short[]) {
         return prefix + Arrays.toString((short[])value);
      } else if (value instanceof double[]) {
         return prefix + Arrays.toString((double[])value);
      } else if (value instanceof boolean[]) {
         return prefix + Arrays.toString((boolean[])value);
      } else if (value instanceof Object[]) {
         return prefix + Arrays.toString((Object[])value);
      } else {
         return prefix + value.toString();
      }
   }

   static void getRoutingTypes(Object source) {
      LOGGER.getRoutingTypes(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601000, value = "User {0} is getting routing type property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRoutingTypes(String user, Object source, Object... args);

   static void getRoutingTypesAsJSON(Object source) {
      LOGGER.getRoutingTypesAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601001, value = "User {0} is getting routing type property as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRoutingTypesAsJSON(String user, Object source, Object... args);

   static void getQueueNames(Object source, Object... args) {
      LOGGER.getQueueNames(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601002, value = "User {0} is getting queue names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getQueueNames(String user, Object source, Object... args);

   static void getBindingNames(Object source) {
      LOGGER.getBindingNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601003, value = "User {0} is getting binding names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getBindingNames(String user, Object source, Object... args);

   static void getRoles(Object source, Object... args) {
      LOGGER.getRoles(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601004, value = "User {0} is getting roles on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRoles(String user, Object source, Object... args);

   static void getRolesAsJSON(Object source, Object... args) {
      LOGGER.getRolesAsJSON(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601005, value = "User {0} is getting roles as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRolesAsJSON(String user, Object source, Object... args);

   static void getNumberOfBytesPerPage(Object source) {
      LOGGER.getNumberOfBytesPerPage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601006, value = "User {0} is getting number of bytes per page on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getNumberOfBytesPerPage(String user, Object source, Object... args);

   static void getAddressSize(Object source) {
      LOGGER.getAddressSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601007, value = "User {0} is getting address size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressSize(String user, Object source, Object... args);

   static void getNumberOfMessages(Object source) {
      LOGGER.getNumberOfMessages(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601008, value = "User {0} is getting number of messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getNumberOfMessages(String user, Object source, Object... args);

   static void isPaging(Object source) {
      LOGGER.isPaging(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601009, value = "User {0} is getting isPaging on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isPaging(String user, Object source, Object... args);

   static void getNumberOfPages(Object source) {
      LOGGER.getNumberOfPages(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601010, value = "User {0} is getting number of pages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getNumberOfPages(String user, Object source, Object... args);

   static void getRoutedMessageCount(Object source) {
      LOGGER.getRoutedMessageCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601011, value = "User {0} is getting routed message count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRoutedMessageCount(String user, Object source, Object... args);

   static void getUnRoutedMessageCount(Object source) {
      LOGGER.getUnRoutedMessageCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601012, value = "User {0} is getting unrouted message count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getUnRoutedMessageCount(String user, Object source, Object... args);

   static void sendMessage(Object source, String user, Object... args) {
      LOGGER.sendMessage(user == null ? getCaller() : user, source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601013, value = "User {0} is sending a message on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void sendMessage(String user, Object source, Object... args);

   static void getName(Object source) {
      LOGGER.getName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601014, value = "User {0} is getting name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getName(String user, Object source, Object... args);

   static void getAddress(Object source) {
      LOGGER.getAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601015, value = "User {0} is getting address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddress(String user, Object source, Object... args);

   static void getFilter(Object source) {
      LOGGER.getFilter(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601016, value = "User {0} is getting filter on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFilter(String user, Object source, Object... args);

   static void isDurable(Object source) {
      LOGGER.isDurable(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601017, value = "User {0} is getting durable property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isDurable(String user, Object source, Object... args);

   static void getMessageCount(Object source) {
      LOGGER.getMessageCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601018, value = "User {0} is getting message count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessageCount(String user, Object source, Object... args);

   static void getMBeanInfo(Object source) {
      LOGGER.getMBeanInfo(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601019, value = "User {0} is getting mbean info on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMBeanInfo(String user, Object source, Object... args);

   static void getFactoryClassName(Object source) {
      LOGGER.getFactoryClassName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601020, value = "User {0} is getting factory class name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFactoryClassName(String user, Object source, Object... args);

   static void getParameters(Object source) {
      LOGGER.getParameters(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601021, value = "User {0} is getting parameters on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getParameters(String user, Object source, Object... args);

   static void reload(Object source) {
      LOGGER.reload(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601022, value = "User {0} is doing reload on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void reload(String user, Object source, Object... args);

   static void isStarted(Object source) {
      LOGGER.isStarted(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601023, value = "User {0} is querying isStarted on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isStarted(String user, Object source, Object... args);

   static void startAcceptor(Object source) {
      LOGGER.startAcceptor(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601024, value = "User {0} is starting an acceptor on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void startAcceptor(String user, Object source, Object... args);

   static void stopAcceptor(Object source) {
      LOGGER.stopAcceptor(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601025, value = "User {0} is stopping an acceptor on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void stopAcceptor(String user, Object source, Object... args);

   static void getVersion(Object source) {
      LOGGER.getVersion(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601026, value = "User {0} is getting version on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getVersion(String user, Object source, Object... args);

   static void isBackup(Object source) {
      LOGGER.isBackup(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601027, value = "User {0} is querying isBackup on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isBackup(String user, Object source, Object... args);

   static void isSharedStore(Object source) {
      LOGGER.isSharedStore(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601028, value = "User {0} is querying isSharedStore on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isSharedStore(String user, Object source, Object... args);

   static void getBindingsDirectory(Object source) {
      LOGGER.getBindingsDirectory(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601029, value = "User {0} is getting bindings directory on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getBindingsDirectory(String user, Object source, Object... args);

   static void getIncomingInterceptorClassNames(Object source) {
      LOGGER.getIncomingInterceptorClassNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601030, value = "User {0} is getting incoming interceptor class names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getIncomingInterceptorClassNames(String user, Object source, Object... args);

   static void getOutgoingInterceptorClassNames(Object source) {
      LOGGER.getOutgoingInterceptorClassNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601031, value = "User {0} is getting outgoing interceptor class names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getOutgoingInterceptorClassNames(String user, Object source, Object... args);

   static void getJournalBufferSize(Object source) {
      LOGGER.getJournalBufferSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601032, value = "User {0} is getting journal buffer size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalBufferSize(String user, Object source, Object... args);

   static void getJournalBufferTimeout(Object source) {
      LOGGER.getJournalBufferTimeout(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601033, value = "User {0} is getting journal buffer timeout on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalBufferTimeout(String user, Object source, Object... args);

   static void setFailoverOnServerShutdown(Object source, Object... args) {
      LOGGER.setFailoverOnServerShutdown(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601034, value = "User {0} is setting failover on server shutdown on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void setFailoverOnServerShutdown(String user, Object source, Object... args);

   static void isFailoverOnServerShutdown(Object source) {
      LOGGER.isFailoverOnServerShutdown(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601035, value = "User {0} is querying is-failover-on-server-shutdown on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isFailoverOnServerShutdown(String user, Object source, Object... args);

   static void getJournalMaxIO(Object source) {
      LOGGER.getJournalMaxIO(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601036, value = "User {0} is getting journal's max io on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalMaxIO(String user, Object source, Object... args);

   static void getJournalDirectory(Object source) {
      LOGGER.getJournalDirectory(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601037, value = "User {0} is getting journal directory on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalDirectory(String user, Object source, Object... args);

   static void getJournalFileSize(Object source) {
      LOGGER.getJournalFileSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601038, value = "User {0} is getting journal file size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalFileSize(String user, Object source, Object... args);

   static void getJournalMinFiles(Object source) {
      LOGGER.getJournalMinFiles(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601039, value = "User {0} is getting journal min files on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalMinFiles(String user, Object source, Object... args);

   static void getJournalCompactMinFiles(Object source) {
      LOGGER.getJournalCompactMinFiles(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601040, value = "User {0} is getting journal compact min files on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalCompactMinFiles(String user, Object source, Object... args);

   static void getJournalCompactPercentage(Object source) {
      LOGGER.getJournalCompactPercentage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601041, value = "User {0} is getting journal compact percentage on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalCompactPercentage(String user, Object source, Object... args);

   static void isPersistenceEnabled(Object source) {
      LOGGER.isPersistenceEnabled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601042, value = "User {0} is querying persistence enabled on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isPersistenceEnabled(String user, Object source, Object... args);

   static void getJournalType(Object source) {
      LOGGER.getJournalType(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601043, value = "User {0} is getting journal type on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalType(String user, Object source, Object... args);

   static void getPagingDirectory(Object source) {
      LOGGER.getPagingDirectory(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601044, value = "User {0} is getting paging directory on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getPagingDirectory(String user, Object source, Object... args);

   static void getScheduledThreadPoolMaxSize(Object source) {
      LOGGER.getScheduledThreadPoolMaxSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601045, value = "User {0} is getting scheduled threadpool max size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getScheduledThreadPoolMaxSize(String user, Object source, Object... args);

   static void getThreadPoolMaxSize(Object source) {
      LOGGER.getThreadPoolMaxSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601046, value = "User {0} is getting threadpool max size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getThreadPoolMaxSize(String user, Object source, Object... args);

   static void getSecurityInvalidationInterval(Object source) {
      LOGGER.getSecurityInvalidationInterval(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601047, value = "User {0} is getting security invalidation interval on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getSecurityInvalidationInterval(String user, Object source, Object... args);

   static void isClustered(Object source) {
      LOGGER.isClustered(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601048, value = "User {0} is querying is-clustered on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isClustered(String user, Object source, Object... args);

   static void isCreateBindingsDir(Object source) {
      LOGGER.isCreateBindingsDir(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601049, value = "User {0} is querying is-create-bindings-dir on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isCreateBindingsDir(String user, Object source, Object... args);

   static void isCreateJournalDir(Object source) {
      LOGGER.isCreateJournalDir(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601050, value = "User {0} is querying is-create-journal-dir on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isCreateJournalDir(String user, Object source, Object... args);

   static void isJournalSyncNonTransactional(Object source) {
      LOGGER.isJournalSyncNonTransactional(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601051, value = "User {0} is querying is-journal-sync-non-transactional on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isJournalSyncNonTransactional(String user, Object source, Object... args);

   static void isJournalSyncTransactional(Object source) {
      LOGGER.isJournalSyncTransactional(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601052, value = "User {0} is querying is-journal-sync-transactional on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isJournalSyncTransactional(String user, Object source, Object... args);

   static void isSecurityEnabled(Object source) {
      LOGGER.isSecurityEnabled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601053, value = "User {0} is querying is-security-enabled on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isSecurityEnabled(String user, Object source, Object... args);

   static void isAsyncConnectionExecutionEnabled(Object source) {
      LOGGER.isAsyncConnectionExecutionEnabled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601054, value = "User {0} is query is-async-connection-execution-enabled on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isAsyncConnectionExecutionEnabled(String user, Object source, Object... args);

   static void getDiskScanPeriod(Object source) {
      LOGGER.getDiskScanPeriod(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601055, value = "User {0} is getting disk scan period on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDiskScanPeriod(String user, Object source, Object... args);

   static void getMaxDiskUsage(Object source) {
      LOGGER.getMaxDiskUsage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601056, value = "User {0} is getting max disk usage on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMaxDiskUsage(String user, Object source, Object... args);

   static void getGlobalMaxSize(Object source) {
      LOGGER.getGlobalMaxSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601057, value = "User {0} is getting global max size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getGlobalMaxSize(String user, Object source, Object... args);

   static void getAddressMemoryUsage(Object source) {
      LOGGER.getAddressMemoryUsage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601058, value = "User {0} is getting address memory usage on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressMemoryUsage(String user, Object source, Object... args);

   static void getAddressMemoryUsagePercentage(Object source) {
      LOGGER.getAddressMemoryUsagePercentage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601059, value = "User {0} is getting address memory usage percentage on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressMemoryUsagePercentage(String user, Object source, Object... args);

   static void freezeReplication(Object source) {
      LOGGER.freezeReplication(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601060, value = "User {0} is freezing replication on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void freezeReplication(String user, Object source, Object... args);

   static void createAddress(Object source, Object... args) {
      LOGGER.createAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601061, value = "User {0} is creating an address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createAddress(String user, Object source, Object... args);

   static void updateAddress(Object source, Object... args) {
      LOGGER.updateAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601062, value = "User {0} is updating an address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void updateAddress(String user, Object source, Object... args);

   static void deleteAddress(Object source, Object... args) {
      LOGGER.deleteAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601063, value = "User {0} is deleting an address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void deleteAddress(String user, Object source, Object... args);

   static void deployQueue(Object source, Object... args) {
      LOGGER.deployQueue(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601064, value = "User {0} is creating a queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void deployQueue(String user, Object source, Object... args);

   static void createQueue(Object source, String user, Object... args) {
      LOGGER.createQueue(user == null ? getCaller() : user, source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601065, value = "User {0} is creating a queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createQueue(String user, Object source, Object... args);

   static void updateQueue(Object source, Object... args) {
      LOGGER.updateQueue(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601066, value = "User {0} is updating a queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void updateQueue(String user, Object source, Object... args);

   static void getClusterConnectionNames(Object source) {
      LOGGER.getClusterConnectionNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601067, value = "User {0} is getting cluster connection names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getClusterConnectionNames(String user, Object source, Object... args);

   static void getUptime(Object source) {
      LOGGER.getUptime(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601068, value = "User {0} is getting uptime on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getUptime(String user, Object source, Object... args);

   static void getUptimeMillis(Object source) {
      LOGGER.getUptimeMillis(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601069, value = "User {0} is getting uptime in milliseconds on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getUptimeMillis(String user, Object source, Object... args);

   static void isReplicaSync(Object source) {
      LOGGER.isReplicaSync(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601070, value = "User {0} is querying is-replica-sync on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isReplicaSync(String user, Object source, Object... args);

   static void getAddressNames(Object source) {
      LOGGER.getAddressNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601071, value = "User {0} is getting address names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressNames(String user, Object source, Object... args);

   static void destroyQueue(Object source, String user, Object... args) {
      LOGGER.destroyQueue(user == null ? getCaller() : user, source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601072, value = "User {0} is deleting a queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void destroyQueue(String user, Object source, Object... args);

   static void getAddressInfo(Object source, Object... args) {
      LOGGER.getAddressInfo(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601073, value = "User {0} is getting address info on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressInfo(String user, Object source, Object... args);

   static void listBindingsForAddress(Object source, Object... args) {
      LOGGER.listBindingsForAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601074, value = "User {0} is listing bindings for address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listBindingsForAddress(String user, Object source, Object... args);

   static void listAddresses(Object source, Object... args) {
      LOGGER.listAddresses(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601075, value = "User {0} is listing addresses on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listAddresses(String user, Object source, Object... args);

   static void getConnectionCount(Object source, Object... args) {
      LOGGER.getConnectionCount(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601076, value = "User {0} is getting connection count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectionCount(String user, Object source, Object... args);

   static void getTotalConnectionCount(Object source) {
      LOGGER.getTotalConnectionCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601077, value = "User {0} is getting total connection count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTotalConnectionCount(String user, Object source, Object... args);

   static void getTotalMessageCount(Object source) {
      LOGGER.getTotalMessageCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601078, value = "User {0} is getting total message count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTotalMessageCount(String user, Object source, Object... args);

   static void getTotalMessagesAdded(Object source) {
      LOGGER.getTotalMessagesAdded(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601079, value = "User {0} is getting total messages added on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTotalMessagesAdded(String user, Object source, Object... args);

   static void getTotalMessagesAcknowledged(Object source) {
      LOGGER.getTotalMessagesAcknowledged(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601080, value = "User {0} is getting total messages acknowledged on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTotalMessagesAcknowledged(String user, Object source, Object... args);

   static void getTotalConsumerCount(Object source) {
      LOGGER.getTotalConsumerCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601081, value = "User {0} is getting total consumer count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTotalConsumerCount(String user, Object source, Object... args);

   static void enableMessageCounters(Object source) {
      LOGGER.enableMessageCounters(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601082, value = "User {0} is enabling message counters on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void enableMessageCounters(String user, Object source, Object... args);

   static void disableMessageCounters(Object source) {
      LOGGER.disableMessageCounters(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601083, value = "User {0} is disabling message counters on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void disableMessageCounters(String user, Object source, Object... args);

   static void resetAllMessageCounters(Object source) {
      LOGGER.resetAllMessageCounters(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601084, value = "User {0} is resetting all message counters on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetAllMessageCounters(String user, Object source, Object... args);

   static void resetAllMessageCounterHistories(Object source) {
      LOGGER.resetAllMessageCounterHistories(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601085, value = "User {0} is resetting all message counter histories on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetAllMessageCounterHistories(String user, Object source, Object... args);

   static void isMessageCounterEnabled(Object source) {
      LOGGER.isMessageCounterEnabled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601086, value = "User {0} is querying is-message-counter-enabled on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isMessageCounterEnabled(String user, Object source, Object... args);

   static void getMessageCounterSamplePeriod(Object source) {
      LOGGER.getMessageCounterSamplePeriod(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601087, value = "User {0} is getting message counter sample period on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessageCounterSamplePeriod(String user, Object source, Object... args);

   static void setMessageCounterSamplePeriod(Object source, Object... args) {
      LOGGER.setMessageCounterSamplePeriod(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601088, value = "User {0} is setting message counter sample period on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void setMessageCounterSamplePeriod(String user, Object source, Object... args);

   static void getMessageCounterMaxDayCount(Object source) {
      LOGGER.getMessageCounterMaxDayCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601089, value = "User {0} is getting message counter max day count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessageCounterMaxDayCount(String user, Object source, Object... args);

   static void setMessageCounterMaxDayCount(Object source, Object... args) {
      LOGGER.setMessageCounterMaxDayCount(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601090, value = "User {0} is setting message counter max day count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void setMessageCounterMaxDayCount(String user, Object source, Object... args);

   static void listPreparedTransactions(Object source) {
      LOGGER.listPreparedTransactions(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601091, value = "User {0} is listing prepared transactions on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listPreparedTransactions(String user, Object source, Object... args);

   static void listPreparedTransactionDetailsAsJSON(Object source) {
      LOGGER.listPreparedTransactionDetailsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601092, value = "User {0} is listing prepared transaction details as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listPreparedTransactionDetailsAsJSON(String user, Object source, Object... args);

   static void listPreparedTransactionDetailsAsHTML(Object source, Object... args) {
      LOGGER.listPreparedTransactionDetailsAsHTML(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601093, value = "User {0} is listing prepared transaction details as HTML on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listPreparedTransactionDetailsAsHTML(String user, Object source, Object... args);

   static void listHeuristicCommittedTransactions(Object source) {
      LOGGER.listHeuristicCommittedTransactions(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601094, value = "User {0} is listing heuristic committed transactions on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listHeuristicCommittedTransactions(String user, Object source, Object... args);

   static void listHeuristicRolledBackTransactions(Object source) {
      LOGGER.listHeuristicRolledBackTransactions(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601095, value = "User {0} is listing heuristic rolled back transactions on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listHeuristicRolledBackTransactions(String user, Object source, Object... args);

   static void commitPreparedTransaction(Object source, Object... args) {
      LOGGER.commitPreparedTransaction(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601096, value = "User {0} is commiting prepared transaction on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void commitPreparedTransaction(String user, Object source, Object... args);

   static void rollbackPreparedTransaction(Object source, Object... args) {
      LOGGER.rollbackPreparedTransaction(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601097, value = "User {0} is rolling back prepared transaction on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void rollbackPreparedTransaction(String user, Object source, Object... args);

   static void listRemoteAddresses(Object source, Object... args) {
      LOGGER.listRemoteAddresses(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601098, value = "User {0} is listing remote addresses on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listRemoteAddresses(String user, Object source, Object... args);

   static void closeConnectionsForAddress(Object source, Object... args) {
      LOGGER.closeConnectionsForAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601099, value = "User {0} is closing connections for address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void closeConnectionsForAddress(String user, Object source, Object... args);

   static void closeConsumerConnectionsForAddress(Object source, Object... args) {
      LOGGER.closeConsumerConnectionsForAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601100, value = "User {0} is closing consumer connections for address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void closeConsumerConnectionsForAddress(String user, Object source, Object... args);

   static void closeConnectionsForUser(Object source, Object... args) {
      LOGGER.closeConnectionsForUser(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601101, value = "User {0} is closing connections for user on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void closeConnectionsForUser(String user, Object source, Object... args);

   static void closeConnectionWithID(Object source, Object... args) {
      LOGGER.closeConnectionWithID(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601102, value = "User {0} is closing a connection by ID on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void closeConnectionWithID(String user, Object source, Object... args);

   static void closeSessionWithID(Object source, Object... args) {
      LOGGER.closeSessionWithID(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601103, value = "User {0} is closing session with id on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void closeSessionWithID(String user, Object source, Object... args);

   static void closeConsumerWithID(Object source, Object... args) {
      LOGGER.closeConsumerWithID(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601104, value = "User {0} is closing consumer with id on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void closeConsumerWithID(String user, Object source, Object... args);

   static void listConnectionIDs(Object source) {
      LOGGER.listConnectionIDs(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601105, value = "User {0} is listing connection IDs on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listConnectionIDs(String user, Object source, Object... args);

   static void listSessions(Object source, Object... args) {
      LOGGER.listSessions(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601106, value = "User {0} is listing sessions on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listSessions(String user, Object source, Object... args);

   static void listProducersInfoAsJSON(Object source) {
      LOGGER.listProducersInfoAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601107, value = "User {0} is listing producers info as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listProducersInfoAsJSON(String user, Object source, Object... args);

   static void listConnections(Object source, Object... args) {
      LOGGER.listConnections(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601108, value = "User {0} is listing connections on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listConnections(String user, Object source, Object... args);

   static void listConsumers(Object source, Object... args) {
      LOGGER.listConsumers(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601109, value = "User {0} is listing consumers on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listConsumers(String user, Object source, Object... args);

   static void listQueues(Object source, Object... args) {
      LOGGER.listQueues(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601110, value = "User {0} is listing queues on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listQueues(String user, Object source, Object... args);

   static void listProducers(Object source, Object... args) {
      LOGGER.listProducers(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601111, value = "User {0} is listing producers on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listProducers(String user, Object source, Object... args);

   static void listConnectionsAsJSON(Object source) {
      LOGGER.listConnectionsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601112, value = "User {0} is listing connections as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listConnectionsAsJSON(String user, Object source, Object... args);

   static void listSessionsAsJSON(Object source, Object... args) {
      LOGGER.listSessionsAsJSON(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601113, value = "User {0} is listing sessions as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listSessionsAsJSON(String user, Object source, Object... args);

   static void listAllSessionsAsJSON(Object source) {
      LOGGER.listAllSessionsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601114, value = "User {0} is listing all sessions as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listAllSessionsAsJSON(String user, Object source, Object... args);

   static void listConsumersAsJSON(Object source, Object... args) {
      LOGGER.listConsumersAsJSON(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601115, value = "User {0} is listing consumers as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listConsumersAsJSON(String user, Object source, Object... args);

   static void listAllConsumersAsJSON(Object source) {
      LOGGER.listAllConsumersAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601116, value = "User {0} is listing all consumers as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listAllConsumersAsJSON(String user, Object source, Object... args);

   static void getConnectors(Object source) {
      LOGGER.getConnectors(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601117, value = "User {0} is getting connectors on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectors(String user, Object source, Object... args);

   static void getConnectorsAsJSON(Object source) {
      LOGGER.getConnectorsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601118, value = "User {0} is getting connectors as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectorsAsJSON(String user, Object source, Object... args);

   static void addSecuritySettings(Object source, Object... args) {
      LOGGER.addSecuritySettings(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601119, value = "User {0} is adding security settings on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void addSecuritySettings(String user, Object source, Object... args);

   static void removeSecuritySettings(Object source, Object... args) {
      LOGGER.removeSecuritySettings(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601120, value = "User {0} is removing security settings on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeSecuritySettings(String user, Object source, Object... args);

   static void getAddressSettingsAsJSON(Object source, Object... args) {
      LOGGER.getAddressSettingsAsJSON(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601121, value = "User {0} is getting address settings as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressSettingsAsJSON(String user, Object source, Object... args);

   static void addAddressSettings(Object source, Object... args) {
      LOGGER.addAddressSettings(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601122, value = "User {0} is adding addressSettings on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void addAddressSettings(String user, Object source, Object... args);

   static void removeAddressSettings(Object source, Object... args) {
      LOGGER.removeAddressSettings(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601123, value = "User {0} is removing address settings on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeAddressSettings(String user, Object source, Object... args);

   static void getDivertNames(Object source) {
      LOGGER.getDivertNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601124, value = "User {0} is getting divert names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDivertNames(String user, Object source, Object... args);

   static void createDivert(Object source, Object... args) {
      LOGGER.createDivert(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601125, value = "User {0} is creating a divert on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createDivert(String user, Object source, Object... args);

   static void destroyDivert(Object source, Object... args) {
      LOGGER.destroyDivert(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601126, value = "User {0} is destroying a divert on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void destroyDivert(String user, Object source, Object... args);

   static void getBridgeNames(Object source) {
      LOGGER.getBridgeNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601127, value = "User {0} is getting bridge names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getBridgeNames(String user, Object source, Object... args);

   static void createBridge(Object source, Object... args) {
      LOGGER.createBridge(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601128, value = "User {0} is creating a bridge on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createBridge(String user, Object source, Object... args);

   static void destroyBridge(Object source, Object... args) {
      LOGGER.destroyBridge(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601129, value = "User {0} is destroying a bridge on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void destroyBridge(String user, Object source, Object... args);

   static void createConnectorService(Object source, Object... args) {
      LOGGER.createConnectorService(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601130, value = "User {0} is creating connector service on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createConnectorService(String user, Object source, Object... args);

   static void destroyConnectorService(Object source, Object... args) {
      LOGGER.destroyConnectorService(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601131, value = "User {0} is destroying connector service on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void destroyConnectorService(String user, Object source, Object... args);

   static void getConnectorServices(Object source, Object... args) {
      LOGGER.getConnectorServices(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601132, value = "User {0} is getting connector services on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectorServices(String user, Object source, Object... args);

   static void forceFailover(Object source) {
      LOGGER.forceFailover(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601133, value = "User {0} is forceing a failover on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void forceFailover(String user, Object source, Object... args);

   static void scaleDown(Object source, Object... args) {
      LOGGER.scaleDown(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601134, value = "User {0} is performing scale down on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void scaleDown(String user, Object source, Object... args);

   static void listNetworkTopology(Object source) {
      LOGGER.listNetworkTopology(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601135, value = "User {0} is listing network topology on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listNetworkTopology(String user, Object source, Object... args);

   static void removeNotificationListener(Object source, Object... args) {
      LOGGER.removeNotificationListener(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601136, value = "User {0} is removing notification listener on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeNotificationListener(String user, Object source, Object... args);

   static void addNotificationListener(Object source, Object... args) {
      LOGGER.addNotificationListener(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601137, value = "User {0} is adding notification listener on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void addNotificationListener(String user, Object source, Object... args);

   static void getNotificationInfo(Object source) {
      LOGGER.getNotificationInfo(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601138, value = "User {0} is getting notification info on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getNotificationInfo(String user, Object source, Object... args);

   static void getConnectionTTLOverride(Object source) {
      LOGGER.getConnectionTTLOverride(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601139, value = "User {0} is getting connection ttl override on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectionTTLOverride(String user, Object source, Object... args);

   static void getIDCacheSize(Object source) {
      LOGGER.getIDCacheSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601140, value = "User {0} is getting ID cache size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getIDCacheSize(String user, Object source, Object... args);

   static void getLargeMessagesDirectory(Object source) {
      LOGGER.getLargeMessagesDirectory(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601141, value = "User {0} is getting large message directory on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getLargeMessagesDirectory(String user, Object source, Object... args);

   static void getManagementAddress(Object source) {
      LOGGER.getManagementAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601142, value = "User {0} is getting management address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getManagementAddress(String user, Object source, Object... args);

   static void getNodeID(Object source) {
      LOGGER.getNodeID(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601143, value = "User {0} is getting node ID on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getNodeID(String user, Object source, Object... args);

   static void getManagementNotificationAddress(Object source) {
      LOGGER.getManagementNotificationAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601144, value = "User {0} is getting management notification address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getManagementNotificationAddress(String user, Object source, Object... args);

   static void getMessageExpiryScanPeriod(Object source) {
      LOGGER.getMessageExpiryScanPeriod(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601145, value = "User {0} is getting message expiry scan period on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessageExpiryScanPeriod(String user, Object source, Object... args);

   static void getMessageExpiryThreadPriority(Object source) {
      LOGGER.getMessageExpiryThreadPriority(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601146, value = "User {0} is getting message expiry thread priority on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessageExpiryThreadPriority(String user, Object source, Object... args);

   static void getTransactionTimeout(Object source) {
      LOGGER.getTransactionTimeout(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601147, value = "User {0} is getting transaction timeout on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTransactionTimeout(String user, Object source, Object... args);

   static void getTransactionTimeoutScanPeriod(Object source) {
      LOGGER.getTransactionTimeoutScanPeriod(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601148, value = "User {0} is getting transaction timeout scan period on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTransactionTimeoutScanPeriod(String user, Object source, Object... args);

   static void isPersistDeliveryCountBeforeDelivery(Object source) {
      LOGGER.isPersistDeliveryCountBeforeDelivery(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601149, value = "User {0} is querying is-persist-delivery-before-delivery on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isPersistDeliveryCountBeforeDelivery(String user, Object source, Object... args);

   static void isPersistIDCache(Object source) {
      LOGGER.isPersistIDCache(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601150, value = "User {0} is querying is-persist-id-cache on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isPersistIDCache(String user, Object source, Object... args);

   static void isWildcardRoutingEnabled(Object source) {
      LOGGER.isWildcardRoutingEnabled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601151, value = "User {0} is querying is-wildcard-routing-enabled on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isWildcardRoutingEnabled(String user, Object source, Object... args);

   static void addUser(Object source, Object... args) {
      LOGGER.addUser(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601152, value = "User {0} is adding a user on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void addUser(String user, Object source, Object... args);

   static void listUser(Object source, Object... args) {
      LOGGER.listUser(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601153, value = "User {0} is listing a user on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listUser(String user, Object source, Object... args);

   static void removeUser(Object source, Object... args) {
      LOGGER.removeUser(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601154, value = "User {0} is removing a user on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeUser(String user, Object source, Object... args);

   static void resetUser(Object source, Object... args) {
      LOGGER.resetUser(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601155, value = "User {0} is resetting a user on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetUser(String user, Object source, Object... args);

   static void getUser(Object source) {
      LOGGER.getUser(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601156, value = "User {0} is getting user property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getUser(String user, Object source, Object... args);

   static void getRoutingType(Object source) {
      LOGGER.getRoutingType(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601157, value = "User {0} is getting routing type property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRoutingType(String user, Object source, Object... args);

   static void isTemporary(Object source) {
      LOGGER.isTemporary(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601158, value = "User {0} is getting temporary property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isTemporary(String user, Object source, Object... args);

   static void getPersistentSize(Object source) {
      LOGGER.getPersistentSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601159, value = "User {0} is getting persistent size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getPersistentSize(String user, Object source, Object... args);

   static void getDurableMessageCount(Object source) {
      LOGGER.getDurableMessageCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601160, value = "User {0} is getting durable message count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDurableMessageCount(String user, Object source, Object... args);

   static void getDurablePersistSize(Object source) {
      LOGGER.getDurablePersistSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601161, value = "User {0} is getting durable persist size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDurablePersistSize(String user, Object source, Object... args);

   static void getConsumerCount(Object source) {
      LOGGER.getConsumerCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601162, value = "User {0} is getting consumer count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConsumerCount(String user, Object source, Object... args);

   static void getDeliveringCount(Object source) {
      LOGGER.getDeliveringCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601163, value = "User {0} is getting delivering count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDeliveringCount(String user, Object source, Object... args);

   static void getDeliveringSize(Object source) {
      LOGGER.getDeliveringSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601164, value = "User {0} is getting delivering size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDeliveringSize(String user, Object source, Object... args);

   static void getDurableDeliveringCount(Object source) {
      LOGGER.getDurableDeliveringCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601165, value = "User {0} is getting durable delivering count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDurableDeliveringCount(String user, Object source, Object... args);

   static void getDurableDeliveringSize(Object source) {
      LOGGER.getDurableDeliveringSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601166, value = "User {0} is getting durable delivering size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDurableDeliveringSize(String user, Object source, Object... args);

   static void getMessagesAdded(Object source) {
      LOGGER.getMessagesAdded(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601167, value = "User {0} is getting messages added on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessagesAdded(String user, Object source, Object... args);

   static void getMessagesAcknowledged(Object source) {
      LOGGER.getMessagesAcknowledged(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601168, value = "User {0} is getting messages acknowledged on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessagesAcknowledged(String user, Object source, Object... args);

   static void getMessagesExpired(Object source) {
      LOGGER.getMessagesExpired(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601169, value = "User {0} is getting messages expired on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessagesExpired(String user, Object source, Object... args);

   static void getMessagesKilled(Object source) {
      LOGGER.getMessagesKilled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601170, value = "User {0} is getting messages killed on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessagesKilled(String user, Object source, Object... args);

   static void getID(Object source) {
      LOGGER.getID(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601171, value = "User {0} is getting ID on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getID(String user, Object source, Object... args);

   static void getScheduledCount(Object source) {
      LOGGER.getScheduledCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601172, value = "User {0} is getting scheduled count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getScheduledCount(String user, Object source, Object... args);

   static void getScheduledSize(Object source) {
      LOGGER.getScheduledSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601173, value = "User {0} is getting scheduled size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getScheduledSize(String user, Object source, Object... args);

   static void getDurableScheduledCount(Object source) {
      LOGGER.getDurableScheduledCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601174, value = "User {0} is getting durable scheduled count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDurableScheduledCount(String user, Object source, Object... args);

   static void getDurableScheduledSize(Object source) {
      LOGGER.getDurableScheduledSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601175, value = "User {0} is getting durable scheduled size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDurableScheduledSize(String user, Object source, Object... args);

   static void getDeadLetterAddress(Object source) {
      LOGGER.getDeadLetterAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601176, value = "User {0} is getting dead letter address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDeadLetterAddress(String user, Object source, Object... args);

   static void getExpiryAddress(Object source) {
      LOGGER.getExpiryAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601177, value = "User {0} is getting expiry address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getExpiryAddress(String user, Object source, Object... args);

   static void getMaxConsumers(Object source) {
      LOGGER.getMaxConsumers(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601178, value = "User {0} is getting max consumers on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMaxConsumers(String user, Object source, Object... args);

   static void isPurgeOnNoConsumers(Object source) {
      LOGGER.isPurgeOnNoConsumers(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601179, value = "User {0} is getting purge-on-consumers property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isPurgeOnNoConsumers(String user, Object source, Object... args);

   static void isConfigurationManaged(Object source) {
      LOGGER.isConfigurationManaged(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601180, value = "User {0} is getting configuration-managed property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isConfigurationManaged(String user, Object source, Object... args);

   static void isExclusive(Object source) {
      LOGGER.isExclusive(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601181, value = "User {0} is getting exclusive property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isExclusive(String user, Object source, Object... args);

   static void isLastValue(Object source) {
      LOGGER.isLastValue(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601182, value = "User {0} is getting last-value property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isLastValue(String user, Object source, Object... args);

   static void listScheduledMessages(Object source) {
      LOGGER.listScheduledMessages(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601183, value = "User {0} is listing scheduled messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listScheduledMessages(String user, Object source, Object... args);

   static void listScheduledMessagesAsJSON(Object source) {
      LOGGER.listScheduledMessagesAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601184, value = "User {0} is listing scheduled messages as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listScheduledMessagesAsJSON(String user, Object source, Object... args);

   static void listDeliveringMessages(Object source) {
      LOGGER.listDeliveringMessages(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601185, value = "User {0} is listing delivering messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listDeliveringMessages(String user, Object source, Object... args);

   static void listDeliveringMessagesAsJSON(Object source) {
      LOGGER.listDeliveringMessagesAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601186, value = "User {0} is listing delivering messages as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listDeliveringMessagesAsJSON(String user, Object source, Object... args);

   static void listMessages(Object source, Object... args) {
      LOGGER.listMessages(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601187, value = "User {0} is listing messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listMessages(String user, Object source, Object... args);

   static void listMessagesAsJSON(Object source) {
      LOGGER.listMessagesAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601188, value = "User {0} is listing messages as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listMessagesAsJSON(String user, Object source, Object... args);

   static void getFirstMessage(Object source) {
      LOGGER.getFirstMessage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601189, value = "User {0} is getting first message on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFirstMessage(String user, Object source, Object... args);

   static void getFirstMessageAsJSON(Object source) {
      LOGGER.getFirstMessageAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601190, value = "User {0} is getting first message as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFirstMessageAsJSON(String user, Object source, Object... args);

   static void getFirstMessageTimestamp(Object source) {
      LOGGER.getFirstMessageTimestamp(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601191, value = "User {0} is getting first message's timestamp on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFirstMessageTimestamp(String user, Object source, Object... args);

   static void getFirstMessageAge(Object source) {
      LOGGER.getFirstMessageAge(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601192, value = "User {0} is getting first message's age on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFirstMessageAge(String user, Object source, Object... args);

   static void countMessages(Object source, Object... args) {
      LOGGER.countMessages(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601193, value = "User {0} is counting messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void countMessages(String user, Object source, Object... args);

   static void countDeliveringMessages(Object source, Object... args) {
      LOGGER.countDeliveringMessages(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601194, value = "User {0} is counting delivery messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void countDeliveringMessages(String user, Object source, Object... args);

   static void removeMessage(Object source, Object... args) {
      LOGGER.removeMessage(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601195, value = "User {0} is removing a message on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeMessage(String user, Object source, Object... args);

   static void removeMessages(Object source, Object... args) {
      LOGGER.removeMessages(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601196, value = "User {0} is removing messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeMessages(String user, Object source, Object... args);

   static void expireMessage(Object source, Object... args) {
      LOGGER.expireMessage(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601197, value = "User {0} is expiring messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void expireMessage(String user, Object source, Object... args);

   static void expireMessages(Object source, Object... args) {
      LOGGER.expireMessages(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601198, value = "User {0} is expiring messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void expireMessages(String user, Object source, Object... args);

   static void retryMessage(Object source, Object... args) {
      LOGGER.retryMessage(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601199, value = "User {0} is retry sending message on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void retryMessage(String user, Object source, Object... args);

   static void retryMessages(Object source) {
      LOGGER.retryMessages(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601200, value = "User {0} is retry sending messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void retryMessages(String user, Object source, Object... args);

   static void moveMessage(Object source, Object... args) {
      LOGGER.moveMessage(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601201, value = "User {0} is moving a message to another queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void moveMessage(String user, Object source, Object... args);

   static void moveMessages(Object source, Object... args) {
      LOGGER.moveMessages(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601202, value = "User {0} is moving messages to another queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void moveMessages(String user, Object source, Object... args);

   static void sendMessagesToDeadLetterAddress(Object source, Object... args) {
      LOGGER.sendMessagesToDeadLetterAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601203, value = "User {0} is sending messages to dead letter address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void sendMessagesToDeadLetterAddress(String user, Object source, Object... args);

   static void sendMessageToDeadLetterAddress(Object source, Object... args) {
      LOGGER.sendMessageToDeadLetterAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601204, value = "User {0} is sending messages to dead letter address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void sendMessageToDeadLetterAddress(String user, Object source, Object... args);

   static void changeMessagesPriority(Object source, Object... args) {
      LOGGER.changeMessagesPriority(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601205, value = "User {0} is changing message's priority on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void changeMessagesPriority(String user, Object source, Object... args);

   static void changeMessagePriority(Object source, Object... args) {
      LOGGER.changeMessagePriority(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601206, value = "User {0} is changing a message's priority on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void changeMessagePriority(String user, Object source, Object... args);

   static void listMessageCounter(Object source) {
      LOGGER.listMessageCounter(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601207, value = "User {0} is listing message counter on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listMessageCounter(String user, Object source, Object... args);

   static void resetMessageCounter(Object source) {
      LOGGER.resetMessageCounter(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601208, value = "User {0} is resetting message counter on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetMessageCounter(String user, Object source, Object... args);

   static void listMessageCounterAsHTML(Object source) {
      LOGGER.listMessageCounterAsHTML(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601209, value = "User {0} is listing message counter as HTML on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listMessageCounterAsHTML(String user, Object source, Object... args);

   static void listMessageCounterHistory(Object source) {
      LOGGER.listMessageCounterHistory(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601210, value = "User {0} is listing message counter history on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listMessageCounterHistory(String user, Object source, Object... args);

   static void listMessageCounterHistoryAsHTML(Object source) {
      LOGGER.listMessageCounterHistoryAsHTML(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601211, value = "User {0} is listing message counter history as HTML on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listMessageCounterHistoryAsHTML(String user, Object source, Object... args);

   static void pause(Object source, Object... args) {
      LOGGER.pause(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601212, value = "User {0} is pausing on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void pause(String user, Object source, Object... args);

   static void resume(Object source) {
      LOGGER.resume(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601213, value = "User {0} is resuming on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resume(String user, Object source, Object... args);

   static void isPaused(Object source) {
      LOGGER.isPaused(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601214, value = "User {0} is getting paused property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isPaused(String user, Object source, Object... args);

   static void browse(Object source, Object... args) {
      LOGGER.browse(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601215, value = "User {0} is browsing a queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void browse(String user, Object source, Object... args);

   static void flushExecutor(Object source) {
      LOGGER.flushExecutor(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601216, value = "User {0} is flushing executor on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void flushExecutor(String user, Object source, Object... args);

   static void resetAllGroups(Object source) {
      LOGGER.resetAllGroups(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601217, value = "User {0} is resetting all groups on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetAllGroups(String user, Object source, Object... args);

   static void resetGroup(Object source, Object... args) {
      LOGGER.resetGroup(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601218, value = "User {0} is resetting group on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetGroup(String user, Object source, Object... args);

   static void getGroupCount(Object source, Object... args) {
      LOGGER.getGroupCount(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601219, value = "User {0} is getting group count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getGroupCount(String user, Object source, Object... args);

   static void listGroupsAsJSON(Object source) {
      LOGGER.listGroupsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601220, value = "User {0} is listing groups as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listGroupsAsJSON(String user, Object source, Object... args);

   static void resetMessagesAdded(Object source) {
      LOGGER.resetMessagesAdded(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601221, value = "User {0} is resetting added messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetMessagesAdded(String user, Object source, Object... args);

   static void resetMessagesAcknowledged(Object source) {
      LOGGER.resetMessagesAcknowledged(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601222, value = "User {0} is resetting acknowledged messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetMessagesAcknowledged(String user, Object source, Object... args);

   static void resetMessagesExpired(Object source) {
      LOGGER.resetMessagesExpired(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601223, value = "User {0} is resetting expired messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetMessagesExpired(String user, Object source, Object... args);

   static void resetMessagesKilled(Object source) {
      LOGGER.resetMessagesKilled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601224, value = "User {0} is resetting killed messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetMessagesKilled(String user, Object source, Object... args);

   static void getStaticConnectors(Object source) {
      LOGGER.getStaticConnectors(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601225, value = "User {0} is getting static connectors on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getStaticConnectors(String user, Object source, Object... args);

   static void getForwardingAddress(Object source) {
      LOGGER.getForwardingAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601226, value = "User {0} is getting forwarding address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getForwardingAddress(String user, Object source, Object... args);

   static void getQueueName(Object source) {
      LOGGER.getQueueName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601227, value = "User {0} is getting the queue name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getQueueName(String user, Object source, Object... args);

   static void getDiscoveryGroupName(Object source) {
      LOGGER.getDiscoveryGroupName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601228, value = "User {0} is getting discovery group name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDiscoveryGroupName(String user, Object source, Object... args);

   static void getFilterString(Object source) {
      LOGGER.getFilterString(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601229, value = "User {0} is getting filter string on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFilterString(String user, Object source, Object... args);

   static void getReconnectAttempts(Object source) {
      LOGGER.getReconnectAttempts(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601230, value = "User {0} is getting reconnect attempts on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getReconnectAttempts(String user, Object source, Object... args);

   static void getRetryInterval(Object source) {
      LOGGER.getRetryInterval(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601231, value = "User {0} is getting retry interval on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRetryInterval(String user, Object source, Object... args);

   static void getRetryIntervalMultiplier(Object source) {
      LOGGER.getRetryIntervalMultiplier(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601232, value = "User {0} is getting retry interval multiplier on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRetryIntervalMultiplier(String user, Object source, Object... args);

   static void getTransformerClassName(Object source) {
      LOGGER.getTransformerClassName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601233, value = "User {0} is getting transformer class name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTransformerClassName(String user, Object source, Object... args);

   static void getTransformerPropertiesAsJSON(Object source) {
      LOGGER.getTransformerPropertiesAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601234, value = "User {0} is getting transformer properties as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTransformerPropertiesAsJSON(String user, Object source, Object... args);

   static void getTransformerProperties(Object source) {
      LOGGER.getTransformerProperties(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601235, value = "User {0} is getting transformer properties on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTransformerProperties(String user, Object source, Object... args);

   static void isStartedBridge(Object source) {
      LOGGER.isStartedBridge(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601236, value = "User {0} is checking if bridge started on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isStartedBridge(String user, Object source, Object... args);

   static void isUseDuplicateDetection(Object source) {
      LOGGER.isUseDuplicateDetection(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601237, value = "User {0} is querying use duplicate detection on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isUseDuplicateDetection(String user, Object source, Object... args);

   static void isHA(Object source) {
      LOGGER.isHA(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601238, value = "User {0} is querying isHA on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isHA(String user, Object source, Object... args);

   static void startBridge(Object source) {
      LOGGER.startBridge(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601239, value = "User {0} is starting a bridge on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void startBridge(String user, Object source, Object... args);

   static void stopBridge(Object source) {
      LOGGER.stopBridge(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601240, value = "User {0} is stopping a bridge on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void stopBridge(String user, Object source, Object... args);

   static void getMessagesPendingAcknowledgement(Object source) {
      LOGGER.getMessagesPendingAcknowledgement(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601241, value = "User {0} is getting messages pending acknowledgement on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessagesPendingAcknowledgement(String user, Object source, Object... args);

   static void getMetrics(Object source) {
      LOGGER.getMetrics(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601242, value = "User {0} is getting metrics on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMetrics(String user, Object source, Object... args);

   static void getBroadcastPeriod(Object source) {
      LOGGER.getBroadcastPeriod(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601243, value = "User {0} is getting broadcast period on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getBroadcastPeriod(String user, Object source, Object... args);

   static void getConnectorPairs(Object source) {
      LOGGER.getConnectorPairs(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601244, value = "User {0} is getting connector pairs on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectorPairs(String user, Object source, Object... args);

   static void getConnectorPairsAsJSON(Object source) {
      LOGGER.getConnectorPairsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601245, value = "User {0} is getting connector pairs as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectorPairsAsJSON(String user, Object source, Object... args);

   static void getGroupAddress(Object source) {
      LOGGER.getGroupAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601246, value = "User {0} is getting group address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getGroupAddress(String user, Object source, Object... args);

   static void getGroupPort(Object source) {
      LOGGER.getGroupPort(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601247, value = "User {0} is getting group port on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getGroupPort(String user, Object source, Object... args);

   static void getLocalBindPort(Object source) {
      LOGGER.getLocalBindPort(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601248, value = "User {0} is getting local binding port on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getLocalBindPort(String user, Object source, Object... args);

   static void startBroadcastGroup(Object source) {
      LOGGER.startBroadcastGroup(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601249, value = "User {0} is starting broadcasting group on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void startBroadcastGroup(String user, Object source, Object... args);

   static void stopBroadcastGroup(Object source) {
      LOGGER.stopBroadcastGroup(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601250, value = "User {0} is stopping broadcasting group on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void stopBroadcastGroup(String user, Object source, Object... args);

   static void getMaxHops(Object source) {
      LOGGER.getMaxHops(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601251, value = "User {0} is getting max hops on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMaxHops(String user, Object source, Object... args);

   static void getStaticConnectorsAsJSON(Object source) {
      LOGGER.getStaticConnectorsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601252, value = "User {0} is geting static connectors as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getStaticConnectorsAsJSON(String user, Object source, Object... args);

   static void isDuplicateDetection(Object source) {
      LOGGER.isDuplicateDetection(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601253, value = "User {0} is querying use duplicate detection on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isDuplicateDetection(String user, Object source, Object... args);

   static void getMessageLoadBalancingType(Object source) {
      LOGGER.getMessageLoadBalancingType(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601254, value = "User {0} is getting message loadbalancing type on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessageLoadBalancingType(String user, Object source, Object... args);

   static void getTopology(Object source) {
      LOGGER.getTopology(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601255, value = "User {0} is getting topology on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTopology(String user, Object source, Object... args);

   static void getNodes(Object source) {
      LOGGER.getNodes(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601256, value = "User {0} is getting nodes on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getNodes(String user, Object source, Object... args);

   static void startClusterConnection(Object source) {
      LOGGER.startClusterConnection(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601257, value = "User {0} is start cluster connection on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void startClusterConnection(String user, Object source, Object... args);

   static void stopClusterConnection(Object source) {
      LOGGER.stopClusterConnection(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601258, value = "User {0} is stop cluster connection on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void stopClusterConnection(String user, Object source, Object... args);

   static void getBridgeMetrics(Object source, Object... args) {
      LOGGER.getBridgeMetrics(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601259, value = "User {0} is getting bridge metrics on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getBridgeMetrics(String user, Object source, Object... args);

   static void getRoutingName(Object source) {
      LOGGER.getRoutingName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601260, value = "User {0} is getting routing name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRoutingName(String user, Object source, Object... args);

   static void getUniqueName(Object source) {
      LOGGER.getUniqueName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601261, value = "User {0} is getting unique name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getUniqueName(String user, Object source, Object... args);

   static void serverSessionCreateAddress(Object source, String user, Object... args) {
      LOGGER.serverSessionCreateAddress2(user == null ? getCaller() : user, source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601262, value = "User {0} is creating address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void serverSessionCreateAddress2(String user, Object source, Object... args);

   static void handleManagementMessage(Object source, String user, Object... args) {
      LOGGER.handleManagementMessage2(user == null ? getCaller() : user, source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601263, value = "User {0} is handling a management message on target resource {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void handleManagementMessage2(String user, Object source, Object... args);


   static void securityFailure(Exception cause) {
      LOGGER.securityFailure(getCaller(), cause);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601264, value = "User {0} gets security check failure", format = Message.Format.MESSAGE_FORMAT)
   void securityFailure(String user, @Cause Throwable cause);


   static void createCoreConsumer(Object source, String user, Object... args) {
      LOGGER.createCoreConsumer(user == null ? getCaller() : user, source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601265, value = "User {0} is creating a core consumer on target resource {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createCoreConsumer(String user, Object source, Object... args);

   static void createSharedQueue(Object source, String user, Object... args) {
      LOGGER.createSharedQueue(user == null ? getCaller() : user, source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601266, value = "User {0} is creating a shared queue on target resource {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createSharedQueue(String user, Object source, Object... args);

   static void createCoreSession(Object source, Object... args) {
      LOGGER.createCoreSession(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601267, value = "User {0} is creating a core session on target resource {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createCoreSession(String user, Object source, Object... args);

   static void getProducedRate(Object source) {
      LOGGER.getProducedRate(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601268, value = "User {0} is getting produced message rate on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getProducedRate(String user, Object source, Object... args);

   //hot path log using a different logger
   static void coreSendMessage(Object source, String user, Object... args) {
      MESSAGE_LOGGER.coreSendMessage(user == null ? getCaller() : user, source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601500, value = "User {0} is sending a core message on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void coreSendMessage(String user, Object source, Object... args);


   static void getAcknowledgeAttempts(Object source) {
      LOGGER.getMessagesAcknowledged(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601501, value = "User {0} is getting messages acknowledged attempts on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAcknowledgeAttempts(String user, Object source, Object... args);

   static void getRingSize(Object source, Object... args) {
      LOGGER.getRingSize(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601502, value = "User {0} is getting ring size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRingSize(String user, Object source, Object... args);


   static void isRetroactiveResource(Object source) {
      LOGGER.isRetroactiveResource(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601503, value = "User {0} is getting retroactiveResource property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isRetroactiveResource(String user, Object source, Object... args);

   static void getDiskStoreUsage(Object source) {
      LOGGER.getDiskStoreUsage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601504, value = "User {0} is getting disk store usage on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDiskStoreUsage(String user, Object source, Object... args);

   static void getDiskStoreUsagePercentage(Object source) {
      LOGGER.getDiskStoreUsagePercentage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601505, value = "User {0} is getting disk store usage percentage on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDiskStoreUsagePercentage(String user, Object source, Object... args);

   static void isGroupRebalance(Object source) {
      LOGGER.isGroupRebalance(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601506, value = "User {0} is getting group rebalance property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isGroupRebalance(String user, Object source, Object... args);

   static void getGroupBuckets(Object source) {
      LOGGER.getGroupBuckets(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601507, value = "User {0} is getting group buckets on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getGroupBuckets(String user, Object source, Object... args);

   static void getGroupFirstKey(Object source) {
      LOGGER.getGroupFirstKey(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601508, value = "User {0} is getting group first key on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getGroupFirstKey(String user, Object source, Object... args);
}
