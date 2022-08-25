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

import javax.management.ObjectName;
import javax.security.auth.Subject;
import java.security.AccessController;
import java.security.Principal;
import java.util.Arrays;
import java.util.Set;

/**
 * Logger Code 60
 *
 * each message id must be 6 digits long starting with 60, the 3rd digit donates the level so
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

   AuditLogger BASE_LOGGER = Logger.getMessageLogger(AuditLogger.class, "org.apache.activemq.audit.base");
   AuditLogger RESOURCE_LOGGER = Logger.getMessageLogger(AuditLogger.class, "org.apache.activemq.audit.resource");
   AuditLogger MESSAGE_LOGGER = Logger.getMessageLogger(AuditLogger.class, "org.apache.activemq.audit.message");

   ThreadLocal<String> remoteAddress = new ThreadLocal<>();

   ThreadLocal<Subject> currentCaller = new ThreadLocal<>();

   static boolean isAnyLoggingEnabled() {
      return isBaseLoggingEnabled() || isMessageLoggingEnabled() || isResourceLoggingEnabled();
   }

   static boolean isBaseLoggingEnabled() {
      return BASE_LOGGER.isEnabled(Logger.Level.INFO);
   }

   static boolean isResourceLoggingEnabled() {
      return RESOURCE_LOGGER.isEnabled(Logger.Level.INFO);
   }

   static boolean isMessageLoggingEnabled() {
      return MESSAGE_LOGGER.isEnabled(Logger.Level.INFO);
   }

   /**
    * @return a String representing the "caller" in the format "user(role)@remoteAddress" using ThreadLocal values (if set)
    */
   static String getCaller() {
      Subject subject = Subject.getSubject(AccessController.getContext());
      if (subject == null) {
         subject = currentCaller.get();
      }
      return getCaller(subject, null);
   }

   /**
    * @param  subject       the Subject to be used instead of the corresponding ThreadLocal Subject
    * @param  remoteAddress the remote address to use; if null use the corresponding ThreadLocal remote address (if set)
    * @return               a String representing the "caller" in the format "user(role)@remoteAddress"
    */
   static String getCaller(Subject subject, String remoteAddress) {
      String user = "anonymous";
      String roles = "";
      String url = remoteAddress == null ? (AuditLogger.remoteAddress.get() == null ? "@unknown" : AuditLogger.remoteAddress.get()) : formatRemoteAddress(remoteAddress);
      if (subject != null) {
         Set<Principal> principals = subject.getPrincipals();
         for (Principal principal : principals) {
            if (principal.getClass().getName().endsWith("UserPrincipal")) {
               user = principal.getName();
            } else if (principal.getClass().getName().endsWith("RolePrincipal")) {
               roles = "(" + principal.getName() + ")";
            }
         }
      }
      return user + roles + url;
   }

   static void setCurrentCaller(Subject caller) {
      currentCaller.set(caller);
   }

   static void setRemoteAddress(String remoteAddress) {
      AuditLogger.remoteAddress.set(formatRemoteAddress(remoteAddress));
   }

   static String formatRemoteAddress(String remoteAddress) {
      String actualAddress;
      if (remoteAddress.startsWith("/")) {
         actualAddress = "@" + remoteAddress.substring(1);
      } else {
         actualAddress = "@" + remoteAddress;
      }
      return actualAddress;
   }

   static String getRemoteAddress() {
      return remoteAddress.get();
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
      BASE_LOGGER.getRoutingTypes(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601000, value = "User {0} is getting routing type property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRoutingTypes(String user, Object source, Object... args);

   static void getRoutingTypesAsJSON(Object source) {
      BASE_LOGGER.getRoutingTypesAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601001, value = "User {0} is getting routing type property as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRoutingTypesAsJSON(String user, Object source, Object... args);

   static void getQueueNames(Object source, Object... args) {
      BASE_LOGGER.getQueueNames(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601002, value = "User {0} is getting queue names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getQueueNames(String user, Object source, Object... args);

   static void getBindingNames(Object source) {
      BASE_LOGGER.getBindingNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601003, value = "User {0} is getting binding names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getBindingNames(String user, Object source, Object... args);

   static void getRoles(Object source, Object... args) {
      BASE_LOGGER.getRoles(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601004, value = "User {0} is getting roles on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRoles(String user, Object source, Object... args);

   static void getRolesAsJSON(Object source, Object... args) {
      BASE_LOGGER.getRolesAsJSON(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601005, value = "User {0} is getting roles as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRolesAsJSON(String user, Object source, Object... args);

   static void getNumberOfBytesPerPage(Object source) {
      BASE_LOGGER.getNumberOfBytesPerPage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601006, value = "User {0} is getting number of bytes per page on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getNumberOfBytesPerPage(String user, Object source, Object... args);

   static void getAddressSize(Object source) {
      BASE_LOGGER.getAddressSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601007, value = "User {0} is getting address size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressSize(String user, Object source, Object... args);

   static void getNumberOfMessages(Object source) {
      BASE_LOGGER.getNumberOfMessages(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601008, value = "User {0} is getting number of messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getNumberOfMessages(String user, Object source, Object... args);

   static void isPaging(Object source) {
      BASE_LOGGER.isPaging(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601009, value = "User {0} is getting isPaging on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isPaging(String user, Object source, Object... args);

   static void getNumberOfPages(Object source) {
      BASE_LOGGER.getNumberOfPages(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601010, value = "User {0} is getting number of pages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getNumberOfPages(String user, Object source, Object... args);

   static void getRoutedMessageCount(Object source) {
      BASE_LOGGER.getRoutedMessageCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601011, value = "User {0} is getting routed message count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRoutedMessageCount(String user, Object source, Object... args);

   static void getUnRoutedMessageCount(Object source) {
      BASE_LOGGER.getUnRoutedMessageCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601012, value = "User {0} is getting unrouted message count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getUnRoutedMessageCount(String user, Object source, Object... args);

   static void sendMessageThroughManagement(Object source, Object... args) {
      BASE_LOGGER.sendMessageThroughManagement(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601013, value = "User {0} is sending a message on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void sendMessageThroughManagement(String user, Object source, Object... args);

   static void getName(Object source) {
      BASE_LOGGER.getName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601014, value = "User {0} is getting name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getName(String user, Object source, Object... args);

   static void getAddress(Object source) {
      BASE_LOGGER.getAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601015, value = "User {0} is getting address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddress(String user, Object source, Object... args);

   static void getFilter(Object source) {
      BASE_LOGGER.getFilter(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601016, value = "User {0} is getting filter on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFilter(String user, Object source, Object... args);

   static void isDurable(Object source) {
      BASE_LOGGER.isDurable(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601017, value = "User {0} is getting durable property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isDurable(String user, Object source, Object... args);

   static void getMessageCount(Object source) {
      BASE_LOGGER.getMessageCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601018, value = "User {0} is getting message count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessageCount(String user, Object source, Object... args);

   static void getMBeanInfo(Object source) {
      BASE_LOGGER.getMBeanInfo(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601019, value = "User {0} is getting mbean info on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMBeanInfo(String user, Object source, Object... args);

   static void getFactoryClassName(Object source) {
      BASE_LOGGER.getFactoryClassName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601020, value = "User {0} is getting factory class name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFactoryClassName(String user, Object source, Object... args);

   static void getParameters(Object source) {
      BASE_LOGGER.getParameters(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601021, value = "User {0} is getting parameters on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getParameters(String user, Object source, Object... args);

   static void reload(Object source) {
      BASE_LOGGER.reload(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601022, value = "User {0} is doing reload on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void reload(String user, Object source, Object... args);

   static void isStarted(Object source) {
      BASE_LOGGER.isStarted(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601023, value = "User {0} is querying isStarted on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isStarted(String user, Object source, Object... args);

   static void startAcceptor(Object source) {
      BASE_LOGGER.startAcceptor(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601024, value = "User {0} is starting an acceptor on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void startAcceptor(String user, Object source, Object... args);

   static void stopAcceptor(Object source) {
      BASE_LOGGER.stopAcceptor(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601025, value = "User {0} is stopping an acceptor on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void stopAcceptor(String user, Object source, Object... args);

   static void getVersion(Object source) {
      BASE_LOGGER.getVersion(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601026, value = "User {0} is getting version on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getVersion(String user, Object source, Object... args);

   static void isBackup(Object source) {
      BASE_LOGGER.isBackup(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601027, value = "User {0} is querying isBackup on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isBackup(String user, Object source, Object... args);

   static void isSharedStore(Object source) {
      BASE_LOGGER.isSharedStore(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601028, value = "User {0} is querying isSharedStore on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isSharedStore(String user, Object source, Object... args);

   static void getBindingsDirectory(Object source) {
      BASE_LOGGER.getBindingsDirectory(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601029, value = "User {0} is getting bindings directory on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getBindingsDirectory(String user, Object source, Object... args);

   static void getIncomingInterceptorClassNames(Object source) {
      BASE_LOGGER.getIncomingInterceptorClassNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601030, value = "User {0} is getting incoming interceptor class names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getIncomingInterceptorClassNames(String user, Object source, Object... args);

   static void getOutgoingInterceptorClassNames(Object source) {
      BASE_LOGGER.getOutgoingInterceptorClassNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601031, value = "User {0} is getting outgoing interceptor class names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getOutgoingInterceptorClassNames(String user, Object source, Object... args);

   static void getJournalBufferSize(Object source) {
      BASE_LOGGER.getJournalBufferSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601032, value = "User {0} is getting journal buffer size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalBufferSize(String user, Object source, Object... args);

   static void getJournalBufferTimeout(Object source) {
      BASE_LOGGER.getJournalBufferTimeout(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601033, value = "User {0} is getting journal buffer timeout on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalBufferTimeout(String user, Object source, Object... args);

   static void setFailoverOnServerShutdown(Object source, Object... args) {
      BASE_LOGGER.setFailoverOnServerShutdown(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601034, value = "User {0} is setting failover on server shutdown on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void setFailoverOnServerShutdown(String user, Object source, Object... args);

   static void isFailoverOnServerShutdown(Object source) {
      BASE_LOGGER.isFailoverOnServerShutdown(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601035, value = "User {0} is querying is-failover-on-server-shutdown on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isFailoverOnServerShutdown(String user, Object source, Object... args);

   static void getJournalMaxIO(Object source) {
      BASE_LOGGER.getJournalMaxIO(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601036, value = "User {0} is getting journal's max io on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalMaxIO(String user, Object source, Object... args);

   static void getJournalDirectory(Object source) {
      BASE_LOGGER.getJournalDirectory(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601037, value = "User {0} is getting journal directory on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalDirectory(String user, Object source, Object... args);

   static void getJournalFileSize(Object source) {
      BASE_LOGGER.getJournalFileSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601038, value = "User {0} is getting journal file size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalFileSize(String user, Object source, Object... args);

   static void getJournalMinFiles(Object source) {
      BASE_LOGGER.getJournalMinFiles(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601039, value = "User {0} is getting journal min files on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalMinFiles(String user, Object source, Object... args);

   static void getJournalCompactMinFiles(Object source) {
      BASE_LOGGER.getJournalCompactMinFiles(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601040, value = "User {0} is getting journal compact min files on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalCompactMinFiles(String user, Object source, Object... args);

   static void getJournalCompactPercentage(Object source) {
      BASE_LOGGER.getJournalCompactPercentage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601041, value = "User {0} is getting journal compact percentage on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalCompactPercentage(String user, Object source, Object... args);

   static void isPersistenceEnabled(Object source) {
      BASE_LOGGER.isPersistenceEnabled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601042, value = "User {0} is querying persistence enabled on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isPersistenceEnabled(String user, Object source, Object... args);

   static void getJournalType(Object source) {
      BASE_LOGGER.getJournalType(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601043, value = "User {0} is getting journal type on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getJournalType(String user, Object source, Object... args);

   static void getPagingDirectory(Object source) {
      BASE_LOGGER.getPagingDirectory(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601044, value = "User {0} is getting paging directory on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getPagingDirectory(String user, Object source, Object... args);

   static void getScheduledThreadPoolMaxSize(Object source) {
      BASE_LOGGER.getScheduledThreadPoolMaxSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601045, value = "User {0} is getting scheduled threadpool max size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getScheduledThreadPoolMaxSize(String user, Object source, Object... args);

   static void getThreadPoolMaxSize(Object source) {
      BASE_LOGGER.getThreadPoolMaxSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601046, value = "User {0} is getting threadpool max size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getThreadPoolMaxSize(String user, Object source, Object... args);

   static void getSecurityInvalidationInterval(Object source) {
      BASE_LOGGER.getSecurityInvalidationInterval(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601047, value = "User {0} is getting security invalidation interval on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getSecurityInvalidationInterval(String user, Object source, Object... args);

   static void isClustered(Object source) {
      BASE_LOGGER.isClustered(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601048, value = "User {0} is querying is-clustered on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isClustered(String user, Object source, Object... args);

   static void isCreateBindingsDir(Object source) {
      BASE_LOGGER.isCreateBindingsDir(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601049, value = "User {0} is querying is-create-bindings-dir on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isCreateBindingsDir(String user, Object source, Object... args);

   static void isCreateJournalDir(Object source) {
      BASE_LOGGER.isCreateJournalDir(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601050, value = "User {0} is querying is-create-journal-dir on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isCreateJournalDir(String user, Object source, Object... args);

   static void isJournalSyncNonTransactional(Object source) {
      BASE_LOGGER.isJournalSyncNonTransactional(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601051, value = "User {0} is querying is-journal-sync-non-transactional on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isJournalSyncNonTransactional(String user, Object source, Object... args);

   static void isJournalSyncTransactional(Object source) {
      BASE_LOGGER.isJournalSyncTransactional(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601052, value = "User {0} is querying is-journal-sync-transactional on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isJournalSyncTransactional(String user, Object source, Object... args);

   static void isSecurityEnabled(Object source) {
      BASE_LOGGER.isSecurityEnabled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601053, value = "User {0} is querying is-security-enabled on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isSecurityEnabled(String user, Object source, Object... args);

   static void isAsyncConnectionExecutionEnabled(Object source) {
      BASE_LOGGER.isAsyncConnectionExecutionEnabled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601054, value = "User {0} is query is-async-connection-execution-enabled on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isAsyncConnectionExecutionEnabled(String user, Object source, Object... args);

   static void getDiskScanPeriod(Object source) {
      BASE_LOGGER.getDiskScanPeriod(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601055, value = "User {0} is getting disk scan period on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDiskScanPeriod(String user, Object source, Object... args);

   static void getMaxDiskUsage(Object source) {
      BASE_LOGGER.getMaxDiskUsage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601056, value = "User {0} is getting max disk usage on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMaxDiskUsage(String user, Object source, Object... args);

   static void getGlobalMaxSize(Object source) {
      BASE_LOGGER.getGlobalMaxSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601057, value = "User {0} is getting global max size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getGlobalMaxSize(String user, Object source, Object... args);

   static void getAddressMemoryUsage(Object source) {
      BASE_LOGGER.getAddressMemoryUsage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601058, value = "User {0} is getting address memory usage on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressMemoryUsage(String user, Object source, Object... args);

   static void getAddressMemoryUsagePercentage(Object source) {
      BASE_LOGGER.getAddressMemoryUsagePercentage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601059, value = "User {0} is getting address memory usage percentage on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressMemoryUsagePercentage(String user, Object source, Object... args);

   static void freezeReplication(Object source) {
      BASE_LOGGER.freezeReplication(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601060, value = "User {0} is freezing replication on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void freezeReplication(String user, Object source, Object... args);

   static void createAddress(Object source, Object... args) {
      BASE_LOGGER.createAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601061, value = "User {0} is creating an address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createAddress(String user, Object source, Object... args);

   static void updateAddress(Object source, Object... args) {
      BASE_LOGGER.updateAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601062, value = "User {0} is updating an address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void updateAddress(String user, Object source, Object... args);

   static void deleteAddress(Object source, Object... args) {
      BASE_LOGGER.deleteAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601063, value = "User {0} is deleting an address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void deleteAddress(String user, Object source, Object... args);

   static void deployQueue(Object source, Object... args) {
      BASE_LOGGER.deployQueue(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601064, value = "User {0} is creating a queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void deployQueue(String user, Object source, Object... args);

   static void createQueue(Object source, Subject user, String remoteAddress, Object... args) {
      RESOURCE_LOGGER.createQueue(getCaller(user, remoteAddress), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601065, value = "User {0} is creating a queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createQueue(String user, Object source, Object... args);

   static void updateQueue(Object source, Object... args) {
      BASE_LOGGER.updateQueue(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601066, value = "User {0} is updating a queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void updateQueue(String user, Object source, Object... args);

   static void getClusterConnectionNames(Object source) {
      BASE_LOGGER.getClusterConnectionNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601067, value = "User {0} is getting cluster connection names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getClusterConnectionNames(String user, Object source, Object... args);

   static void getUptime(Object source) {
      BASE_LOGGER.getUptime(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601068, value = "User {0} is getting uptime on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getUptime(String user, Object source, Object... args);

   static void getUptimeMillis(Object source) {
      BASE_LOGGER.getUptimeMillis(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601069, value = "User {0} is getting uptime in milliseconds on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getUptimeMillis(String user, Object source, Object... args);

   static void isReplicaSync(Object source) {
      BASE_LOGGER.isReplicaSync(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601070, value = "User {0} is querying is-replica-sync on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isReplicaSync(String user, Object source, Object... args);

   static void getAddressNames(Object source) {
      BASE_LOGGER.getAddressNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601071, value = "User {0} is getting address names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressNames(String user, Object source, Object... args);

   static void destroyQueue(Object source, Subject user, String remoteAddress, Object... args) {
      BASE_LOGGER.destroyQueue(getCaller(user, remoteAddress), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601072, value = "User {0} is deleting a queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void destroyQueue(String user, Object source, Object... args);

   static void getAddressInfo(Object source, Object... args) {
      BASE_LOGGER.getAddressInfo(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601073, value = "User {0} is getting address info on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressInfo(String user, Object source, Object... args);

   static void listBindingsForAddress(Object source, Object... args) {
      BASE_LOGGER.listBindingsForAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601074, value = "User {0} is listing bindings for address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listBindingsForAddress(String user, Object source, Object... args);

   static void listAddresses(Object source, Object... args) {
      BASE_LOGGER.listAddresses(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601075, value = "User {0} is listing addresses on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listAddresses(String user, Object source, Object... args);

   static void getConnectionCount(Object source, Object... args) {
      BASE_LOGGER.getConnectionCount(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601076, value = "User {0} is getting connection count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectionCount(String user, Object source, Object... args);

   static void getTotalConnectionCount(Object source) {
      BASE_LOGGER.getTotalConnectionCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601077, value = "User {0} is getting total connection count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTotalConnectionCount(String user, Object source, Object... args);

   static void getTotalMessageCount(Object source) {
      BASE_LOGGER.getTotalMessageCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601078, value = "User {0} is getting total message count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTotalMessageCount(String user, Object source, Object... args);

   static void getTotalMessagesAdded(Object source) {
      BASE_LOGGER.getTotalMessagesAdded(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601079, value = "User {0} is getting total messages added on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTotalMessagesAdded(String user, Object source, Object... args);

   static void getTotalMessagesAcknowledged(Object source) {
      BASE_LOGGER.getTotalMessagesAcknowledged(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601080, value = "User {0} is getting total messages acknowledged on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTotalMessagesAcknowledged(String user, Object source, Object... args);

   static void getTotalConsumerCount(Object source) {
      BASE_LOGGER.getTotalConsumerCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601081, value = "User {0} is getting total consumer count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTotalConsumerCount(String user, Object source, Object... args);

   static void enableMessageCounters(Object source) {
      BASE_LOGGER.enableMessageCounters(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601082, value = "User {0} is enabling message counters on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void enableMessageCounters(String user, Object source, Object... args);

   static void disableMessageCounters(Object source) {
      BASE_LOGGER.disableMessageCounters(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601083, value = "User {0} is disabling message counters on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void disableMessageCounters(String user, Object source, Object... args);

   static void resetAllMessageCounters(Object source) {
      BASE_LOGGER.resetAllMessageCounters(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601084, value = "User {0} is resetting all message counters on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetAllMessageCounters(String user, Object source, Object... args);

   static void resetAllMessageCounterHistories(Object source) {
      BASE_LOGGER.resetAllMessageCounterHistories(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601085, value = "User {0} is resetting all message counter histories on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetAllMessageCounterHistories(String user, Object source, Object... args);

   static void isMessageCounterEnabled(Object source) {
      BASE_LOGGER.isMessageCounterEnabled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601086, value = "User {0} is querying is-message-counter-enabled on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isMessageCounterEnabled(String user, Object source, Object... args);

   static void getMessageCounterSamplePeriod(Object source) {
      BASE_LOGGER.getMessageCounterSamplePeriod(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601087, value = "User {0} is getting message counter sample period on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessageCounterSamplePeriod(String user, Object source, Object... args);

   static void setMessageCounterSamplePeriod(Object source, Object... args) {
      BASE_LOGGER.setMessageCounterSamplePeriod(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601088, value = "User {0} is setting message counter sample period on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void setMessageCounterSamplePeriod(String user, Object source, Object... args);

   static void getMessageCounterMaxDayCount(Object source) {
      BASE_LOGGER.getMessageCounterMaxDayCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601089, value = "User {0} is getting message counter max day count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessageCounterMaxDayCount(String user, Object source, Object... args);

   static void setMessageCounterMaxDayCount(Object source, Object... args) {
      BASE_LOGGER.setMessageCounterMaxDayCount(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601090, value = "User {0} is setting message counter max day count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void setMessageCounterMaxDayCount(String user, Object source, Object... args);

   static void listPreparedTransactions(Object source) {
      BASE_LOGGER.listPreparedTransactions(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601091, value = "User {0} is listing prepared transactions on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listPreparedTransactions(String user, Object source, Object... args);

   static void listPreparedTransactionDetailsAsJSON(Object source) {
      BASE_LOGGER.listPreparedTransactionDetailsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601092, value = "User {0} is listing prepared transaction details as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listPreparedTransactionDetailsAsJSON(String user, Object source, Object... args);

   static void listPreparedTransactionDetailsAsHTML(Object source, Object... args) {
      BASE_LOGGER.listPreparedTransactionDetailsAsHTML(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601093, value = "User {0} is listing prepared transaction details as HTML on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listPreparedTransactionDetailsAsHTML(String user, Object source, Object... args);

   static void listHeuristicCommittedTransactions(Object source) {
      BASE_LOGGER.listHeuristicCommittedTransactions(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601094, value = "User {0} is listing heuristic committed transactions on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listHeuristicCommittedTransactions(String user, Object source, Object... args);

   static void listHeuristicRolledBackTransactions(Object source) {
      BASE_LOGGER.listHeuristicRolledBackTransactions(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601095, value = "User {0} is listing heuristic rolled back transactions on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listHeuristicRolledBackTransactions(String user, Object source, Object... args);

   static void commitPreparedTransaction(Object source, Object... args) {
      BASE_LOGGER.commitPreparedTransaction(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601096, value = "User {0} is commiting prepared transaction on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void commitPreparedTransaction(String user, Object source, Object... args);

   static void rollbackPreparedTransaction(Object source, Object... args) {
      BASE_LOGGER.rollbackPreparedTransaction(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601097, value = "User {0} is rolling back prepared transaction on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void rollbackPreparedTransaction(String user, Object source, Object... args);

   static void listRemoteAddresses(Object source, Object... args) {
      BASE_LOGGER.listRemoteAddresses(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601098, value = "User {0} is listing remote addresses on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listRemoteAddresses(String user, Object source, Object... args);

   static void closeConnectionsForAddress(Object source, Object... args) {
      BASE_LOGGER.closeConnectionsForAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601099, value = "User {0} is closing connections for address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void closeConnectionsForAddress(String user, Object source, Object... args);

   static void closeConsumerConnectionsForAddress(Object source, Object... args) {
      BASE_LOGGER.closeConsumerConnectionsForAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601100, value = "User {0} is closing consumer connections for address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void closeConsumerConnectionsForAddress(String user, Object source, Object... args);

   static void closeConnectionsForUser(Object source, Object... args) {
      BASE_LOGGER.closeConnectionsForUser(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601101, value = "User {0} is closing connections for user on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void closeConnectionsForUser(String user, Object source, Object... args);

   static void closeConnectionWithID(Object source, Object... args) {
      BASE_LOGGER.closeConnectionWithID(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601102, value = "User {0} is closing a connection by ID on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void closeConnectionWithID(String user, Object source, Object... args);

   static void closeSessionWithID(Object source, Object... args) {
      BASE_LOGGER.closeSessionWithID(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601103, value = "User {0} is closing session with id on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void closeSessionWithID(String user, Object source, Object... args);

   static void closeConsumerWithID(Object source, Object... args) {
      BASE_LOGGER.closeConsumerWithID(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601104, value = "User {0} is closing consumer with id on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void closeConsumerWithID(String user, Object source, Object... args);

   static void listConnectionIDs(Object source) {
      BASE_LOGGER.listConnectionIDs(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601105, value = "User {0} is listing connection IDs on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listConnectionIDs(String user, Object source, Object... args);

   static void listSessions(Object source, Object... args) {
      BASE_LOGGER.listSessions(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601106, value = "User {0} is listing sessions on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listSessions(String user, Object source, Object... args);

   static void listProducersInfoAsJSON(Object source) {
      BASE_LOGGER.listProducersInfoAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601107, value = "User {0} is listing producers info as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listProducersInfoAsJSON(String user, Object source, Object... args);

   static void listConnections(Object source, Object... args) {
      BASE_LOGGER.listConnections(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601108, value = "User {0} is listing connections on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listConnections(String user, Object source, Object... args);

   static void listConsumers(Object source, Object... args) {
      BASE_LOGGER.listConsumers(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601109, value = "User {0} is listing consumers on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listConsumers(String user, Object source, Object... args);

   static void listQueues(Object source, Object... args) {
      BASE_LOGGER.listQueues(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601110, value = "User {0} is listing queues on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listQueues(String user, Object source, Object... args);

   static void listProducers(Object source, Object... args) {
      BASE_LOGGER.listProducers(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601111, value = "User {0} is listing producers on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listProducers(String user, Object source, Object... args);

   static void listConnectionsAsJSON(Object source) {
      BASE_LOGGER.listConnectionsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601112, value = "User {0} is listing connections as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listConnectionsAsJSON(String user, Object source, Object... args);

   static void listSessionsAsJSON(Object source, Object... args) {
      BASE_LOGGER.listSessionsAsJSON(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601113, value = "User {0} is listing sessions as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listSessionsAsJSON(String user, Object source, Object... args);

   static void listAllSessionsAsJSON(Object source) {
      BASE_LOGGER.listAllSessionsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601114, value = "User {0} is listing all sessions as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listAllSessionsAsJSON(String user, Object source, Object... args);

   static void listConsumersAsJSON(Object source, Object... args) {
      BASE_LOGGER.listConsumersAsJSON(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601115, value = "User {0} is listing consumers as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listConsumersAsJSON(String user, Object source, Object... args);

   static void listAllConsumersAsJSON(Object source) {
      BASE_LOGGER.listAllConsumersAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601116, value = "User {0} is listing all consumers as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listAllConsumersAsJSON(String user, Object source, Object... args);

   static void getConnectors(Object source) {
      BASE_LOGGER.getConnectors(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601117, value = "User {0} is getting connectors on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectors(String user, Object source, Object... args);

   static void getConnectorsAsJSON(Object source) {
      BASE_LOGGER.getConnectorsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601118, value = "User {0} is getting connectors as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectorsAsJSON(String user, Object source, Object... args);

   static void addSecuritySettings(Object source, Object... args) {
      BASE_LOGGER.addSecuritySettings(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601119, value = "User {0} is adding security settings on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void addSecuritySettings(String user, Object source, Object... args);

   static void removeSecuritySettings(Object source, Object... args) {
      BASE_LOGGER.removeSecuritySettings(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601120, value = "User {0} is removing security settings on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeSecuritySettings(String user, Object source, Object... args);

   static void getAddressSettingsAsJSON(Object source, Object... args) {
      BASE_LOGGER.getAddressSettingsAsJSON(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601121, value = "User {0} is getting address settings as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressSettingsAsJSON(String user, Object source, Object... args);

   static void addAddressSettings(Object source, Object... args) {
      BASE_LOGGER.addAddressSettings(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601122, value = "User {0} is adding addressSettings on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void addAddressSettings(String user, Object source, Object... args);

   static void removeAddressSettings(Object source, Object... args) {
      BASE_LOGGER.removeAddressSettings(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601123, value = "User {0} is removing address settings on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeAddressSettings(String user, Object source, Object... args);

   static void getDivertNames(Object source) {
      BASE_LOGGER.getDivertNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601124, value = "User {0} is getting divert names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDivertNames(String user, Object source, Object... args);

   static void createDivert(Object source, Object... args) {
      BASE_LOGGER.createDivert(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601125, value = "User {0} is creating a divert on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createDivert(String user, Object source, Object... args);

   static void destroyDivert(Object source, Object... args) {
      BASE_LOGGER.destroyDivert(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601126, value = "User {0} is destroying a divert on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void destroyDivert(String user, Object source, Object... args);

   static void getBridgeNames(Object source) {
      BASE_LOGGER.getBridgeNames(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601127, value = "User {0} is getting bridge names on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getBridgeNames(String user, Object source, Object... args);

   static void createBridge(Object source, Object... args) {
      BASE_LOGGER.createBridge(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601128, value = "User {0} is creating a bridge on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createBridge(String user, Object source, Object... args);

   static void destroyBridge(Object source, Object... args) {
      BASE_LOGGER.destroyBridge(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601129, value = "User {0} is destroying a bridge on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void destroyBridge(String user, Object source, Object... args);

   static void createConnectorService(Object source, Object... args) {
      BASE_LOGGER.createConnectorService(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601130, value = "User {0} is creating connector service on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createConnectorService(String user, Object source, Object... args);

   static void destroyConnectorService(Object source, Object... args) {
      BASE_LOGGER.destroyConnectorService(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601131, value = "User {0} is destroying connector service on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void destroyConnectorService(String user, Object source, Object... args);

   static void getConnectorServices(Object source, Object... args) {
      BASE_LOGGER.getConnectorServices(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601132, value = "User {0} is getting connector services on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectorServices(String user, Object source, Object... args);

   static void forceFailover(Object source) {
      BASE_LOGGER.forceFailover(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601133, value = "User {0} is forceing a failover on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void forceFailover(String user, Object source, Object... args);

   static void scaleDown(Object source, Object... args) {
      BASE_LOGGER.scaleDown(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601134, value = "User {0} is performing scale down on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void scaleDown(String user, Object source, Object... args);

   static void listNetworkTopology(Object source) {
      BASE_LOGGER.listNetworkTopology(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601135, value = "User {0} is listing network topology on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listNetworkTopology(String user, Object source, Object... args);

   static void removeNotificationListener(Object source, Object... args) {
      BASE_LOGGER.removeNotificationListener(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601136, value = "User {0} is removing notification listener on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeNotificationListener(String user, Object source, Object... args);

   static void addNotificationListener(Object source, Object... args) {
      BASE_LOGGER.addNotificationListener(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601137, value = "User {0} is adding notification listener on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void addNotificationListener(String user, Object source, Object... args);

   static void getNotificationInfo(Object source) {
      BASE_LOGGER.getNotificationInfo(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601138, value = "User {0} is getting notification info on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getNotificationInfo(String user, Object source, Object... args);

   static void getConnectionTTLOverride(Object source) {
      BASE_LOGGER.getConnectionTTLOverride(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601139, value = "User {0} is getting connection ttl override on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectionTTLOverride(String user, Object source, Object... args);

   static void getIDCacheSize(Object source) {
      BASE_LOGGER.getIDCacheSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601140, value = "User {0} is getting ID cache size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getIDCacheSize(String user, Object source, Object... args);

   static void getLargeMessagesDirectory(Object source) {
      BASE_LOGGER.getLargeMessagesDirectory(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601141, value = "User {0} is getting large message directory on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getLargeMessagesDirectory(String user, Object source, Object... args);

   static void getManagementAddress(Object source) {
      BASE_LOGGER.getManagementAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601142, value = "User {0} is getting management address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getManagementAddress(String user, Object source, Object... args);

   static void getNodeID(Object source) {
      BASE_LOGGER.getNodeID(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601143, value = "User {0} is getting node ID on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getNodeID(String user, Object source, Object... args);

   static void getManagementNotificationAddress(Object source) {
      BASE_LOGGER.getManagementNotificationAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601144, value = "User {0} is getting management notification address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getManagementNotificationAddress(String user, Object source, Object... args);

   static void getMessageExpiryScanPeriod(Object source) {
      BASE_LOGGER.getMessageExpiryScanPeriod(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601145, value = "User {0} is getting message expiry scan period on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessageExpiryScanPeriod(String user, Object source, Object... args);

   static void getMessageExpiryThreadPriority(Object source) {
      BASE_LOGGER.getMessageExpiryThreadPriority(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601146, value = "User {0} is getting message expiry thread priority on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessageExpiryThreadPriority(String user, Object source, Object... args);

   static void getTransactionTimeout(Object source) {
      BASE_LOGGER.getTransactionTimeout(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601147, value = "User {0} is getting transaction timeout on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTransactionTimeout(String user, Object source, Object... args);

   static void getTransactionTimeoutScanPeriod(Object source) {
      BASE_LOGGER.getTransactionTimeoutScanPeriod(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601148, value = "User {0} is getting transaction timeout scan period on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTransactionTimeoutScanPeriod(String user, Object source, Object... args);

   static void isPersistDeliveryCountBeforeDelivery(Object source) {
      BASE_LOGGER.isPersistDeliveryCountBeforeDelivery(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601149, value = "User {0} is querying is-persist-delivery-before-delivery on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isPersistDeliveryCountBeforeDelivery(String user, Object source, Object... args);

   static void isPersistIDCache(Object source) {
      BASE_LOGGER.isPersistIDCache(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601150, value = "User {0} is querying is-persist-id-cache on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isPersistIDCache(String user, Object source, Object... args);

   static void isWildcardRoutingEnabled(Object source) {
      BASE_LOGGER.isWildcardRoutingEnabled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601151, value = "User {0} is querying is-wildcard-routing-enabled on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isWildcardRoutingEnabled(String user, Object source, Object... args);

   static void addUser(Object source, Object... args) {
      BASE_LOGGER.addUser(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601152, value = "User {0} is adding a user on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void addUser(String user, Object source, Object... args);

   static void listUser(Object source, Object... args) {
      BASE_LOGGER.listUser(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601153, value = "User {0} is listing a user on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listUser(String user, Object source, Object... args);

   static void removeUser(Object source, Object... args) {
      BASE_LOGGER.removeUser(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601154, value = "User {0} is removing a user on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeUser(String user, Object source, Object... args);

   static void resetUser(Object source, Object... args) {
      BASE_LOGGER.resetUser(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601155, value = "User {0} is resetting a user on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetUser(String user, Object source, Object... args);

   static void getUser(Object source) {
      BASE_LOGGER.getUser(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601156, value = "User {0} is getting user property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getUser(String user, Object source, Object... args);

   static void getRoutingType(Object source) {
      BASE_LOGGER.getRoutingType(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601157, value = "User {0} is getting routing type property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRoutingType(String user, Object source, Object... args);

   static void isTemporary(Object source) {
      BASE_LOGGER.isTemporary(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601158, value = "User {0} is getting temporary property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isTemporary(String user, Object source, Object... args);

   static void getPersistentSize(Object source) {
      BASE_LOGGER.getPersistentSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601159, value = "User {0} is getting persistent size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getPersistentSize(String user, Object source, Object... args);

   static void getDurableMessageCount(Object source) {
      BASE_LOGGER.getDurableMessageCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601160, value = "User {0} is getting durable message count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDurableMessageCount(String user, Object source, Object... args);

   static void getDurablePersistSize(Object source) {
      BASE_LOGGER.getDurablePersistSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601161, value = "User {0} is getting durable persist size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDurablePersistSize(String user, Object source, Object... args);

   static void getConsumerCount(Object source) {
      BASE_LOGGER.getConsumerCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601162, value = "User {0} is getting consumer count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConsumerCount(String user, Object source, Object... args);

   static void getDeliveringCount(Object source) {
      BASE_LOGGER.getDeliveringCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601163, value = "User {0} is getting delivering count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDeliveringCount(String user, Object source, Object... args);

   static void getDeliveringSize(Object source) {
      BASE_LOGGER.getDeliveringSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601164, value = "User {0} is getting delivering size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDeliveringSize(String user, Object source, Object... args);

   static void getDurableDeliveringCount(Object source) {
      BASE_LOGGER.getDurableDeliveringCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601165, value = "User {0} is getting durable delivering count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDurableDeliveringCount(String user, Object source, Object... args);

   static void getDurableDeliveringSize(Object source) {
      BASE_LOGGER.getDurableDeliveringSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601166, value = "User {0} is getting durable delivering size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDurableDeliveringSize(String user, Object source, Object... args);

   static void getMessagesAdded(Object source) {
      BASE_LOGGER.getMessagesAdded(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601167, value = "User {0} is getting messages added on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessagesAdded(String user, Object source, Object... args);

   static void getMessagesAcknowledged(Object source) {
      BASE_LOGGER.getMessagesAcknowledged(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601168, value = "User {0} is getting messages acknowledged on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessagesAcknowledged(String user, Object source, Object... args);

   static void getMessagesExpired(Object source) {
      BASE_LOGGER.getMessagesExpired(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601169, value = "User {0} is getting messages expired on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessagesExpired(String user, Object source, Object... args);

   static void getMessagesKilled(Object source) {
      BASE_LOGGER.getMessagesKilled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601170, value = "User {0} is getting messages killed on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessagesKilled(String user, Object source, Object... args);

   static void getID(Object source) {
      BASE_LOGGER.getID(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601171, value = "User {0} is getting ID on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getID(String user, Object source, Object... args);

   static void getScheduledCount(Object source) {
      BASE_LOGGER.getScheduledCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601172, value = "User {0} is getting scheduled count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getScheduledCount(String user, Object source, Object... args);

   static void getScheduledSize(Object source) {
      BASE_LOGGER.getScheduledSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601173, value = "User {0} is getting scheduled size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getScheduledSize(String user, Object source, Object... args);

   static void getDurableScheduledCount(Object source) {
      BASE_LOGGER.getDurableScheduledCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601174, value = "User {0} is getting durable scheduled count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDurableScheduledCount(String user, Object source, Object... args);

   static void getDurableScheduledSize(Object source) {
      BASE_LOGGER.getDurableScheduledSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601175, value = "User {0} is getting durable scheduled size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDurableScheduledSize(String user, Object source, Object... args);

   static void getDeadLetterAddress(Object source) {
      BASE_LOGGER.getDeadLetterAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601176, value = "User {0} is getting dead letter address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDeadLetterAddress(String user, Object source, Object... args);

   static void getExpiryAddress(Object source) {
      BASE_LOGGER.getExpiryAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601177, value = "User {0} is getting expiry address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getExpiryAddress(String user, Object source, Object... args);

   static void getMaxConsumers(Object source) {
      BASE_LOGGER.getMaxConsumers(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601178, value = "User {0} is getting max consumers on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMaxConsumers(String user, Object source, Object... args);

   static void isPurgeOnNoConsumers(Object source) {
      BASE_LOGGER.isPurgeOnNoConsumers(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601179, value = "User {0} is getting purge-on-consumers property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isPurgeOnNoConsumers(String user, Object source, Object... args);

   static void isConfigurationManaged(Object source) {
      BASE_LOGGER.isConfigurationManaged(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601180, value = "User {0} is getting configuration-managed property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isConfigurationManaged(String user, Object source, Object... args);

   static void isExclusive(Object source) {
      BASE_LOGGER.isExclusive(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601181, value = "User {0} is getting exclusive property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isExclusive(String user, Object source, Object... args);

   static void isLastValue(Object source) {
      BASE_LOGGER.isLastValue(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601182, value = "User {0} is getting last-value property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isLastValue(String user, Object source, Object... args);

   static void listScheduledMessages(Object source) {
      BASE_LOGGER.listScheduledMessages(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601183, value = "User {0} is listing scheduled messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listScheduledMessages(String user, Object source, Object... args);

   static void listScheduledMessagesAsJSON(Object source) {
      BASE_LOGGER.listScheduledMessagesAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601184, value = "User {0} is listing scheduled messages as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listScheduledMessagesAsJSON(String user, Object source, Object... args);

   static void listDeliveringMessages(Object source) {
      BASE_LOGGER.listDeliveringMessages(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601185, value = "User {0} is listing delivering messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listDeliveringMessages(String user, Object source, Object... args);

   static void listDeliveringMessagesAsJSON(Object source) {
      BASE_LOGGER.listDeliveringMessagesAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601186, value = "User {0} is listing delivering messages as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listDeliveringMessagesAsJSON(String user, Object source, Object... args);

   static void listMessages(Object source, Object... args) {
      BASE_LOGGER.listMessages(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601187, value = "User {0} is listing messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listMessages(String user, Object source, Object... args);

   static void listMessagesAsJSON(Object source) {
      BASE_LOGGER.listMessagesAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601188, value = "User {0} is listing messages as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listMessagesAsJSON(String user, Object source, Object... args);

   static void getFirstMessage(Object source) {
      BASE_LOGGER.getFirstMessage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601189, value = "User {0} is getting first message on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFirstMessage(String user, Object source, Object... args);

   static void getFirstMessageAsJSON(Object source) {
      BASE_LOGGER.getFirstMessageAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601190, value = "User {0} is getting first message as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFirstMessageAsJSON(String user, Object source, Object... args);

   static void getFirstMessageTimestamp(Object source) {
      BASE_LOGGER.getFirstMessageTimestamp(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601191, value = "User {0} is getting first message's timestamp on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFirstMessageTimestamp(String user, Object source, Object... args);

   static void getFirstMessageAge(Object source) {
      BASE_LOGGER.getFirstMessageAge(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601192, value = "User {0} is getting first message's age on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFirstMessageAge(String user, Object source, Object... args);

   static void countMessages(Object source, Object... args) {
      BASE_LOGGER.countMessages(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601193, value = "User {0} is counting messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void countMessages(String user, Object source, Object... args);

   static void countDeliveringMessages(Object source, Object... args) {
      BASE_LOGGER.countDeliveringMessages(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601194, value = "User {0} is counting delivery messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void countDeliveringMessages(String user, Object source, Object... args);

   static void removeMessage(Object source, Object... args) {
      BASE_LOGGER.removeMessage(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601195, value = "User {0} is removing a message on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeMessage(String user, Object source, Object... args);

   static void removeMessages(Object source, Object... args) {
      BASE_LOGGER.removeMessages(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601196, value = "User {0} is removing messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeMessages(String user, Object source, Object... args);

   static void expireMessage(Object source, Object... args) {
      BASE_LOGGER.expireMessage(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601197, value = "User {0} is expiring messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void expireMessage(String user, Object source, Object... args);

   static void expireMessages(Object source, Object... args) {
      BASE_LOGGER.expireMessages(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601198, value = "User {0} is expiring messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void expireMessages(String user, Object source, Object... args);

   static void retryMessage(Object source, Object... args) {
      BASE_LOGGER.retryMessage(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601199, value = "User {0} is retry sending message on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void retryMessage(String user, Object source, Object... args);

   static void retryMessages(Object source) {
      BASE_LOGGER.retryMessages(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601200, value = "User {0} is retry sending messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void retryMessages(String user, Object source, Object... args);

   static void moveMessage(Object source, Object... args) {
      BASE_LOGGER.moveMessage(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601201, value = "User {0} is moving a message to another queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void moveMessage(String user, Object source, Object... args);

   static void moveMessages(Object source, Object... args) {
      BASE_LOGGER.moveMessages(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601202, value = "User {0} is moving messages to another queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void moveMessages(String user, Object source, Object... args);

   static void sendMessagesToDeadLetterAddress(Object source, Object... args) {
      BASE_LOGGER.sendMessagesToDeadLetterAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601203, value = "User {0} is sending messages to dead letter address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void sendMessagesToDeadLetterAddress(String user, Object source, Object... args);

   static void sendMessageToDeadLetterAddress(Object source, Object... args) {
      BASE_LOGGER.sendMessageToDeadLetterAddress(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601204, value = "User {0} is sending messages to dead letter address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void sendMessageToDeadLetterAddress(String user, Object source, Object... args);

   static void changeMessagesPriority(Object source, Object... args) {
      BASE_LOGGER.changeMessagesPriority(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601205, value = "User {0} is changing message's priority on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void changeMessagesPriority(String user, Object source, Object... args);

   static void changeMessagePriority(Object source, Object... args) {
      BASE_LOGGER.changeMessagePriority(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601206, value = "User {0} is changing a message's priority on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void changeMessagePriority(String user, Object source, Object... args);

   static void listMessageCounter(Object source) {
      BASE_LOGGER.listMessageCounter(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601207, value = "User {0} is listing message counter on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listMessageCounter(String user, Object source, Object... args);

   static void resetMessageCounter(Object source) {
      BASE_LOGGER.resetMessageCounter(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601208, value = "User {0} is resetting message counter on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetMessageCounter(String user, Object source, Object... args);

   static void listMessageCounterAsHTML(Object source) {
      BASE_LOGGER.listMessageCounterAsHTML(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601209, value = "User {0} is listing message counter as HTML on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listMessageCounterAsHTML(String user, Object source, Object... args);

   static void listMessageCounterHistory(Object source) {
      BASE_LOGGER.listMessageCounterHistory(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601210, value = "User {0} is listing message counter history on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listMessageCounterHistory(String user, Object source, Object... args);

   static void listMessageCounterHistoryAsHTML(Object source) {
      BASE_LOGGER.listMessageCounterHistoryAsHTML(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601211, value = "User {0} is listing message counter history as HTML on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listMessageCounterHistoryAsHTML(String user, Object source, Object... args);

   static void pause(Object source, Object... args) {
      BASE_LOGGER.pause(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601212, value = "User {0} is pausing on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void pause(String user, Object source, Object... args);

   static void resume(Object source) {
      BASE_LOGGER.resume(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601213, value = "User {0} is resuming on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resume(String user, Object source, Object... args);

   static void isPaused(Object source) {
      BASE_LOGGER.isPaused(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601214, value = "User {0} is getting paused property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isPaused(String user, Object source, Object... args);

   static void browse(Object source, Object... args) {
      BASE_LOGGER.browse(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601215, value = "User {0} is browsing a queue on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void browse(String user, Object source, Object... args);

   static void flushExecutor(Object source) {
      BASE_LOGGER.flushExecutor(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601216, value = "User {0} is flushing executor on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void flushExecutor(String user, Object source, Object... args);

   static void resetAllGroups(Object source) {
      BASE_LOGGER.resetAllGroups(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601217, value = "User {0} is resetting all groups on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetAllGroups(String user, Object source, Object... args);

   static void resetGroup(Object source, Object... args) {
      BASE_LOGGER.resetGroup(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601218, value = "User {0} is resetting group on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetGroup(String user, Object source, Object... args);

   static void getGroupCount(Object source, Object... args) {
      BASE_LOGGER.getGroupCount(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601219, value = "User {0} is getting group count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getGroupCount(String user, Object source, Object... args);

   static void listGroupsAsJSON(Object source) {
      BASE_LOGGER.listGroupsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601220, value = "User {0} is listing groups as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void listGroupsAsJSON(String user, Object source, Object... args);

   static void resetMessagesAdded(Object source) {
      BASE_LOGGER.resetMessagesAdded(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601221, value = "User {0} is resetting added messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetMessagesAdded(String user, Object source, Object... args);

   static void resetMessagesAcknowledged(Object source) {
      BASE_LOGGER.resetMessagesAcknowledged(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601222, value = "User {0} is resetting acknowledged messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetMessagesAcknowledged(String user, Object source, Object... args);

   static void resetMessagesExpired(Object source) {
      BASE_LOGGER.resetMessagesExpired(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601223, value = "User {0} is resetting expired messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetMessagesExpired(String user, Object source, Object... args);

   static void resetMessagesKilled(Object source) {
      BASE_LOGGER.resetMessagesKilled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601224, value = "User {0} is resetting killed messages on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void resetMessagesKilled(String user, Object source, Object... args);

   static void getStaticConnectors(Object source) {
      BASE_LOGGER.getStaticConnectors(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601225, value = "User {0} is getting static connectors on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getStaticConnectors(String user, Object source, Object... args);

   static void getForwardingAddress(Object source) {
      BASE_LOGGER.getForwardingAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601226, value = "User {0} is getting forwarding address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getForwardingAddress(String user, Object source, Object... args);

   static void getQueueName(Object source) {
      BASE_LOGGER.getQueueName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601227, value = "User {0} is getting the queue name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getQueueName(String user, Object source, Object... args);

   static void getDiscoveryGroupName(Object source) {
      BASE_LOGGER.getDiscoveryGroupName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601228, value = "User {0} is getting discovery group name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDiscoveryGroupName(String user, Object source, Object... args);

   static void getFilterString(Object source) {
      BASE_LOGGER.getFilterString(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601229, value = "User {0} is getting filter string on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFilterString(String user, Object source, Object... args);

   static void getReconnectAttempts(Object source) {
      BASE_LOGGER.getReconnectAttempts(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601230, value = "User {0} is getting reconnect attempts on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getReconnectAttempts(String user, Object source, Object... args);

   static void getRetryInterval(Object source) {
      BASE_LOGGER.getRetryInterval(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601231, value = "User {0} is getting retry interval on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRetryInterval(String user, Object source, Object... args);

   static void getRetryIntervalMultiplier(Object source) {
      BASE_LOGGER.getRetryIntervalMultiplier(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601232, value = "User {0} is getting retry interval multiplier on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRetryIntervalMultiplier(String user, Object source, Object... args);

   static void getTransformerClassName(Object source) {
      BASE_LOGGER.getTransformerClassName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601233, value = "User {0} is getting transformer class name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTransformerClassName(String user, Object source, Object... args);

   static void getTransformerPropertiesAsJSON(Object source) {
      BASE_LOGGER.getTransformerPropertiesAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601234, value = "User {0} is getting transformer properties as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTransformerPropertiesAsJSON(String user, Object source, Object... args);

   static void getTransformerProperties(Object source) {
      BASE_LOGGER.getTransformerProperties(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601235, value = "User {0} is getting transformer properties on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTransformerProperties(String user, Object source, Object... args);

   static void isStartedBridge(Object source) {
      BASE_LOGGER.isStartedBridge(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601236, value = "User {0} is checking if bridge started on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isStartedBridge(String user, Object source, Object... args);

   static void isUseDuplicateDetection(Object source) {
      BASE_LOGGER.isUseDuplicateDetection(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601237, value = "User {0} is querying use duplicate detection on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isUseDuplicateDetection(String user, Object source, Object... args);

   static void isHA(Object source) {
      BASE_LOGGER.isHA(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601238, value = "User {0} is querying isHA on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isHA(String user, Object source, Object... args);

   static void startBridge(Object source) {
      BASE_LOGGER.startBridge(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601239, value = "User {0} is starting a bridge on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void startBridge(String user, Object source, Object... args);

   static void stopBridge(Object source) {
      BASE_LOGGER.stopBridge(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601240, value = "User {0} is stopping a bridge on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void stopBridge(String user, Object source, Object... args);

   static void getMessagesPendingAcknowledgement(Object source) {
      BASE_LOGGER.getMessagesPendingAcknowledgement(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601241, value = "User {0} is getting messages pending acknowledgement on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessagesPendingAcknowledgement(String user, Object source, Object... args);

   static void getMetrics(Object source) {
      BASE_LOGGER.getMetrics(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601242, value = "User {0} is getting metrics on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMetrics(String user, Object source, Object... args);

   static void getBroadcastPeriod(Object source) {
      BASE_LOGGER.getBroadcastPeriod(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601243, value = "User {0} is getting broadcast period on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getBroadcastPeriod(String user, Object source, Object... args);

   static void getConnectorPairs(Object source) {
      BASE_LOGGER.getConnectorPairs(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601244, value = "User {0} is getting connector pairs on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectorPairs(String user, Object source, Object... args);

   static void getConnectorPairsAsJSON(Object source) {
      BASE_LOGGER.getConnectorPairsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601245, value = "User {0} is getting connector pairs as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getConnectorPairsAsJSON(String user, Object source, Object... args);

   static void getGroupAddress(Object source) {
      BASE_LOGGER.getGroupAddress(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601246, value = "User {0} is getting group address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getGroupAddress(String user, Object source, Object... args);

   static void getGroupPort(Object source) {
      BASE_LOGGER.getGroupPort(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601247, value = "User {0} is getting group port on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getGroupPort(String user, Object source, Object... args);

   static void getLocalBindPort(Object source) {
      BASE_LOGGER.getLocalBindPort(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601248, value = "User {0} is getting local binding port on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getLocalBindPort(String user, Object source, Object... args);

   static void startBroadcastGroup(Object source) {
      BASE_LOGGER.startBroadcastGroup(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601249, value = "User {0} is starting broadcasting group on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void startBroadcastGroup(String user, Object source, Object... args);

   static void stopBroadcastGroup(Object source) {
      BASE_LOGGER.stopBroadcastGroup(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601250, value = "User {0} is stopping broadcasting group on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void stopBroadcastGroup(String user, Object source, Object... args);

   static void getMaxHops(Object source) {
      BASE_LOGGER.getMaxHops(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601251, value = "User {0} is getting max hops on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMaxHops(String user, Object source, Object... args);

   static void getStaticConnectorsAsJSON(Object source) {
      BASE_LOGGER.getStaticConnectorsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601252, value = "User {0} is geting static connectors as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getStaticConnectorsAsJSON(String user, Object source, Object... args);

   static void isDuplicateDetection(Object source) {
      BASE_LOGGER.isDuplicateDetection(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601253, value = "User {0} is querying use duplicate detection on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isDuplicateDetection(String user, Object source, Object... args);

   static void getMessageLoadBalancingType(Object source) {
      BASE_LOGGER.getMessageLoadBalancingType(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601254, value = "User {0} is getting message loadbalancing type on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMessageLoadBalancingType(String user, Object source, Object... args);

   static void getTopology(Object source) {
      BASE_LOGGER.getTopology(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601255, value = "User {0} is getting topology on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getTopology(String user, Object source, Object... args);

   static void getNodes(Object source) {
      BASE_LOGGER.getNodes(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601256, value = "User {0} is getting nodes on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getNodes(String user, Object source, Object... args);

   static void startClusterConnection(Object source) {
      BASE_LOGGER.startClusterConnection(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601257, value = "User {0} is start cluster connection on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void startClusterConnection(String user, Object source, Object... args);

   static void stopClusterConnection(Object source) {
      BASE_LOGGER.stopClusterConnection(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601258, value = "User {0} is stop cluster connection on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void stopClusterConnection(String user, Object source, Object... args);

   static void getBridgeMetrics(Object source, Object... args) {
      BASE_LOGGER.getBridgeMetrics(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601259, value = "User {0} is getting bridge metrics on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getBridgeMetrics(String user, Object source, Object... args);

   static void getRoutingName(Object source) {
      BASE_LOGGER.getRoutingName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601260, value = "User {0} is getting routing name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRoutingName(String user, Object source, Object... args);

   static void getUniqueName(Object source) {
      BASE_LOGGER.getUniqueName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601261, value = "User {0} is getting unique name on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getUniqueName(String user, Object source, Object... args);

   static void serverSessionCreateAddress(Object source, Subject user, String remoteAddress, Object... args) {
      BASE_LOGGER.serverSessionCreateAddress2(getCaller(user, remoteAddress), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601262, value = "User {0} is creating address on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void serverSessionCreateAddress2(String user, Object source, Object... args);

   static void handleManagementMessage(Object source, Subject user, String remoteAddress, Object... args) {
      BASE_LOGGER.handleManagementMessage2(getCaller(user, remoteAddress), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601263, value = "User {0} is handling a management message on target resource {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void handleManagementMessage2(String user, Object source, Object... args);


   static void securityFailure(Exception cause) {
      BASE_LOGGER.securityFailure(getCaller(), cause);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601264, value = "User {0} gets security check failure", format = Message.Format.MESSAGE_FORMAT)
   void securityFailure(String user, @Cause Throwable cause);


   static void createCoreConsumer(Object source, Subject user, String remoteAddress, Object... args) {
      BASE_LOGGER.createCoreConsumer(getCaller(user, remoteAddress), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601265, value = "User {0} is creating a core consumer on target resource {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createCoreConsumer(String user, Object source, Object... args);

   static void createSharedQueue(Object source, Subject user, String remoteAddress, Object... args) {
      BASE_LOGGER.createSharedQueue(getCaller(user, remoteAddress), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601266, value = "User {0} is creating a shared queue on target resource {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createSharedQueue(String user, Object source, Object... args);

   static void createCoreSession(Object source, Subject user, String remoteAddress, Object... args) {
      BASE_LOGGER.createCoreSession(getCaller(user, remoteAddress), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601267, value = "User {0} is creating a core session on target resource {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void createCoreSession(String user, Object source, Object... args);

   static void getAcknowledgeAttempts(Object source) {
      BASE_LOGGER.getMessagesAcknowledged(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601269, value = "User {0} is getting messages acknowledged attempts on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAcknowledgeAttempts(String user, Object source, Object... args);

   static void getRingSize(Object source, Object... args) {
      BASE_LOGGER.getRingSize(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601270, value = "User {0} is getting ring size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getRingSize(String user, Object source, Object... args);


   static void isRetroactiveResource(Object source) {
      BASE_LOGGER.isRetroactiveResource(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601271, value = "User {0} is getting retroactiveResource property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isRetroactiveResource(String user, Object source, Object... args);

   static void getDiskStoreUsage(Object source) {
      BASE_LOGGER.getDiskStoreUsage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601272, value = "User {0} is getting disk store usage on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDiskStoreUsage(String user, Object source, Object... args);

   static void getDiskStoreUsagePercentage(Object source) {
      BASE_LOGGER.getDiskStoreUsagePercentage(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601273, value = "User {0} is getting disk store usage percentage on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getDiskStoreUsagePercentage(String user, Object source, Object... args);

   static void isGroupRebalance(Object source) {
      BASE_LOGGER.isGroupRebalance(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601274, value = "User {0} is getting group rebalance property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isGroupRebalance(String user, Object source, Object... args);

   static void getGroupBuckets(Object source) {
      BASE_LOGGER.getGroupBuckets(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601275, value = "User {0} is getting group buckets on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getGroupBuckets(String user, Object source, Object... args);

   static void getGroupFirstKey(Object source) {
      BASE_LOGGER.getGroupFirstKey(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601276, value = "User {0} is getting group first key on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getGroupFirstKey(String user, Object source, Object... args);

   static void getCurrentDuplicateIdCacheSize(Object source) {
      BASE_LOGGER.getCurrentDuplicateIdCacheSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601509, value = "User {0} is getting currentDuplicateIdCacheSize property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getCurrentDuplicateIdCacheSize(String user, Object source, Object... args);


   static void clearDuplicateIdCache(Object source) {
      BASE_LOGGER.clearDuplicateIdCache(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601510, value = "User {0} is clearing duplicate ID cache on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void clearDuplicateIdCache(String user, Object source, Object... args);


   static void getChannelName(Object source) {
      BASE_LOGGER.getChannelName(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601511, value = "User {0} is getting channelName property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getChannelName(String user, Object source, Object... args);

   static void getFileContents(Object source) {
      BASE_LOGGER.getFileContents(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601512, value = "User {0} is getting fileContents property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFileContents(String user, Object source, Object... args);

   static void getFile(Object source) {
      BASE_LOGGER.getFile(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601513, value = "User {0} is getting file property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getFile(String user, Object source, Object... args);

   static void getPreparedTransactionMessageCount(Object source) {
      BASE_LOGGER.getPreparedTransactionMessageCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601514, value = "User {0} is getting preparedTransactionMessageCount property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getPreparedTransactionMessageCount(String user, Object source, Object... args);

   /*
    * This logger is for message production and consumption and is on the hot path so enabled independently
    *
    * */
   //hot path log using a different logger
   static void coreSendMessage(Subject user, String remoteAddress, String messageToString, Object context, String tx) {
      MESSAGE_LOGGER.coreSendMessage(getCaller(user, remoteAddress), messageToString, context, tx);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601500, value = "User {0} sent a message {1}, context: {2}, transaction: {3}", format = Message.Format.MESSAGE_FORMAT)
   void coreSendMessage(String user, String messageToString, Object context, String tx);

   //hot path log using a different logger
   static void coreConsumeMessage(Subject user, String remoteAddress, String queue, String message) {
      MESSAGE_LOGGER.consumeMessage(getCaller(user, remoteAddress), queue, message);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601501, value = "User {0} is consuming a message from {1}: {2}", format = Message.Format.MESSAGE_FORMAT)
   void consumeMessage(String user, String address, String message);

   //hot path log using a different logger
   static void coreAcknowledgeMessage(Subject user, String remoteAddress, String queue, String message, String tx) {
      MESSAGE_LOGGER.coreAcknowledgeMessage(getCaller(user, remoteAddress), queue, message, tx);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601502, value = "User {0} acknowledged message from {1}: {2}, transaction: {3}", format = Message.Format.MESSAGE_FORMAT)
   void coreAcknowledgeMessage(String user, String queue, String message, String tx);

   /*
    * This logger is focused on user interaction from the console or thru resource specific functions in the management layer/JMX
    * */

   static void createAddressSuccess(String name, String routingTypes) {
      RESOURCE_LOGGER.createAddressSuccess(getCaller(), name, routingTypes);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601701, value = "User {0} successfully created address: {1} with routing types {2}", format = Message.Format.MESSAGE_FORMAT)
   void createAddressSuccess(String user, String name, String routingTypes);

   static void createAddressFailure(String name, String routingTypes) {
      RESOURCE_LOGGER.createAddressFailure(getCaller(), name, routingTypes);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601702, value = "User {0} failed to created address: {1} with routing types {2}", format = Message.Format.MESSAGE_FORMAT)
   void createAddressFailure(String user, String name, String routingTypes);

   static void updateAddressSuccess(String name, String routingTypes) {
      RESOURCE_LOGGER.updateAddressSuccess(getCaller(), name, routingTypes);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601703, value = "User {0} successfully updated address: {1} with routing types {2}", format = Message.Format.MESSAGE_FORMAT)
   void updateAddressSuccess(String user, String name, String routingTypes);

   static void updateAddressFailure(String name, String routingTypes) {
      RESOURCE_LOGGER.updateAddressFailure(getCaller(), name, routingTypes);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601704, value = "User {0} successfully updated address: {1} with routing types {2}", format = Message.Format.MESSAGE_FORMAT)
   void updateAddressFailure(String user, String name, String routingTypes);

   static void deleteAddressSuccess(String name) {
      RESOURCE_LOGGER.deleteAddressSuccess(getCaller(), name);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601705, value = "User {0} successfully deleted address: {1}", format = Message.Format.MESSAGE_FORMAT)
   void deleteAddressSuccess(String user, String name);


   static void deleteAddressFailure(String name) {
      RESOURCE_LOGGER.deleteAddressFailure(getCaller(), name);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601706, value = "User {0} failed to deleted address: {1}", format = Message.Format.MESSAGE_FORMAT)
   void deleteAddressFailure(String user, String name);

   static void createQueueSuccess(String name, String address, String routingType) {
      RESOURCE_LOGGER.createQueueSuccess(getCaller(), name, address, routingType);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601707, value = "User {0} successfully created queue: {1} on address: {2} with routing type {3}", format = Message.Format.MESSAGE_FORMAT)
   void createQueueSuccess(String user, String name, String address, String routingType);

   static void createQueueFailure(String name, String address, String routingType) {
      RESOURCE_LOGGER.createQueueFailure(getCaller(), name, address, routingType);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601708, value = "User {0} failed to create queue: {1} on address: {2} with routing type {3}", format = Message.Format.MESSAGE_FORMAT)
   void createQueueFailure(String user, String name, String address, String routingType);

   static void updateQueueSuccess(String name, String routingType) {
      RESOURCE_LOGGER.updateQueueSuccess(getCaller(), name, routingType);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601709, value = "User {0} successfully updated queue: {1} with routing type {2}", format = Message.Format.MESSAGE_FORMAT)
   void updateQueueSuccess(String user, String name, String routingType);

   static void updateQueueFailure(String name, String routingType) {
      RESOURCE_LOGGER.updateQueueFailure(getCaller(), name, routingType);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601710, value = "User {0} failed to update queue: {1} with routing type {2}", format = Message.Format.MESSAGE_FORMAT)
   void updateQueueFailure(String user, String name, String routingType);


   static void destroyQueueSuccess(String name) {
      RESOURCE_LOGGER.destroyQueueSuccess(getCaller(), name);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601711, value = "User {0} successfully deleted queue: {1}", format = Message.Format.MESSAGE_FORMAT)
   void destroyQueueSuccess(String user, String name);

   static void destroyQueueFailure(String name) {
      RESOURCE_LOGGER.destroyQueueFailure(getCaller(), name);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601712, value = "User {0} failed to delete queue: {1}", format = Message.Format.MESSAGE_FORMAT)
   void destroyQueueFailure(String user, String name);

   static void removeMessagesSuccess(int removed, String queue) {
      RESOURCE_LOGGER.removeMessagesSuccess(getCaller(), removed, queue);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601713, value = "User {0} has removed {1} messages from queue: {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeMessagesSuccess(String user, int removed, String queue);

   static void removeMessagesFailure(String queue) {
      RESOURCE_LOGGER.removeMessagesFailure(getCaller(), queue);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601714, value = "User {0} failed to remove messages from queue: {1}", format = Message.Format.MESSAGE_FORMAT)
   void removeMessagesFailure(String user, String queue);

   static void userSuccesfullyAuthenticatedInAudit(Subject subject, String remoteAddress) {
      RESOURCE_LOGGER.userSuccesfullyAuthenticated(getCaller(subject, remoteAddress));
   }

   static void userSuccesfullyAuthenticatedInAudit(Subject subject) {
      userSuccesfullyAuthenticatedInAudit(subject, null);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601715, value = "User {0} successfully authenticated", format = Message.Format.MESSAGE_FORMAT)
   void userSuccesfullyAuthenticated(String caller);


   static void userFailedAuthenticationInAudit(String reason) {
      RESOURCE_LOGGER.userFailedAuthentication(getCaller(), reason);
   }

   static void userFailedAuthenticationInAudit(Subject subject, String reason) {
      RESOURCE_LOGGER.userFailedAuthentication(getCaller(subject, null), reason);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601716, value = "User {0} failed authentication, reason: {1}", format = Message.Format.MESSAGE_FORMAT)
   void userFailedAuthentication(String user, String reason);

   static void objectInvokedSuccessfully(ObjectName objectName, String operationName) {
      RESOURCE_LOGGER.objectInvokedSuccessfully(getCaller(), objectName, operationName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601717, value = "User {0} accessed {2} on management object {1}", format = Message.Format.MESSAGE_FORMAT)
   void objectInvokedSuccessfully(String caller, ObjectName objectName, String operationName);


   static void objectInvokedFailure(ObjectName objectName, String operationName) {
      RESOURCE_LOGGER.objectInvokedFailure(getCaller(), objectName, operationName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601718, value = "User {0} does not have correct role to access {2} on management object {1}", format = Message.Format.MESSAGE_FORMAT)
   void objectInvokedFailure(String caller, ObjectName objectName, String operationName);

   static void pauseQueueSuccess(String queueName) {
      RESOURCE_LOGGER.pauseQueueSuccess(getCaller(), queueName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601719, value = "User {0} has paused queue {1}", format = Message.Format.MESSAGE_FORMAT)
   void pauseQueueSuccess(String user, String queueName);


   static void pauseQueueFailure(String queueName) {
      RESOURCE_LOGGER.pauseQueueFailure(getCaller(), queueName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601720, value = "User {0} failed to pause queue {1}", format = Message.Format.MESSAGE_FORMAT)
   void pauseQueueFailure(String user, String queueName);


   static void resumeQueueSuccess(String queueName) {
      RESOURCE_LOGGER.resumeQueueSuccess(getCaller(), queueName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601721, value = "User {0} has resumed queue {1}", format = Message.Format.MESSAGE_FORMAT)
   void resumeQueueSuccess(String user, String queueName);


   static void resumeQueueFailure(String queueName) {
      RESOURCE_LOGGER.pauseQueueFailure(getCaller(), queueName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601722, value = "User {0} failed to resume queue {1}", format = Message.Format.MESSAGE_FORMAT)
   void resumeQueueFailure(String user, String queueName);

   static void sendMessageSuccess(String queueName, String user) {
      RESOURCE_LOGGER.sendMessageSuccess(getCaller(), queueName, user);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601723, value = "User {0} sent message to {1} as user {2}", format = Message.Format.MESSAGE_FORMAT)
   void sendMessageSuccess(String user, String queueName, String sendUser);

   static void sendMessageFailure(String queueName, String user) {
      RESOURCE_LOGGER.sendMessageFailure(getCaller(), queueName, user);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601724, value = "User {0} failed to send message to {1} as user {2}", format = Message.Format.MESSAGE_FORMAT)
   void sendMessageFailure(String user, String queueName, String sendUser);

   static void browseMessagesSuccess(String queueName, int numMessages) {
      RESOURCE_LOGGER.browseMessagesSuccess(getCaller(), queueName, numMessages);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601725, value = "User {0} browsed {2} messages from queue {1}", format = Message.Format.MESSAGE_FORMAT)
   void browseMessagesSuccess(String user, String queueName, int numMessages);

   static void browseMessagesFailure(String queueName) {
      RESOURCE_LOGGER.browseMessagesFailure(getCaller(), queueName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601726, value = "User {0} failed to browse messages from queue {1}", format = Message.Format.MESSAGE_FORMAT)
   void browseMessagesFailure(String user, String queueName);

   static void updateDivert(Object source, Object... args) {
      BASE_LOGGER.updateDivert(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601727, value = "User {0} is updating a divert on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void updateDivert(String user, Object source, Object... args);

   static void isEnabled(Object source) {
      BASE_LOGGER.isEnabled(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601728, value = "User {0} is getting enabled property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isEnabled(String user, Object source, Object... args);

   static void disable(Object source, Object... args) {
      BASE_LOGGER.disable(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601729, value = "User {0} is disabling on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void disable(String user, Object source, Object... args);

   static void enable(Object source) {
      BASE_LOGGER.resume(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601730, value = "User {0} is enabling on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void enable(String user, Object source, Object... args);

   static void pauseAddressSuccess(String queueName) {
      RESOURCE_LOGGER.pauseAddressSuccess(getCaller(), queueName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601731, value = "User {0} has paused address {1}", format = Message.Format.MESSAGE_FORMAT)
   void pauseAddressSuccess(String user, String queueName);


   static void pauseAddressFailure(String queueName) {
      RESOURCE_LOGGER.pauseAddressFailure(getCaller(), queueName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601732, value = "User {0} failed to pause address {1}", format = Message.Format.MESSAGE_FORMAT)
   void pauseAddressFailure(String user, String queueName);


   static void resumeAddressSuccess(String queueName) {
      RESOURCE_LOGGER.resumeAddressSuccess(getCaller(), queueName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601733, value = "User {0} has resumed address {1}", format = Message.Format.MESSAGE_FORMAT)
   void resumeAddressSuccess(String user, String queueName);


   static void resumeAddressFailure(String queueName) {
      RESOURCE_LOGGER.resumeAddressFailure(getCaller(), queueName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601734, value = "User {0} failed to resume address {1}", format = Message.Format.MESSAGE_FORMAT)
   void resumeAddressFailure(String user, String queueName);

   static void isGroupRebalancePauseDispatch(Object source) {
      BASE_LOGGER.isGroupRebalancePauseDispatch(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601735, value = "User {0} is getting group rebalance pause dispatch property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isGroupRebalancePauseDispatch(String user, Object source, Object... args);

   static void getAuthenticationCacheSize(Object source) {
      BASE_LOGGER.getAuthenticationCacheSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601736, value = "User {0} is getting authentication cache size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAuthenticationCacheSize(String user, Object source, Object... args);

   static void getAuthorizationCacheSize(Object source) {
      BASE_LOGGER.getAuthorizationCacheSize(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601737, value = "User {0} is getting authorization cache size on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAuthorizationCacheSize(String user, Object source, Object... args);

   static void listBrokerConnections() {
      BASE_LOGGER.listBrokerConnections(getCaller());
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601738, value = "User {0} is requesting a list of broker connections", format = Message.Format.MESSAGE_FORMAT)
   void listBrokerConnections(String user);

   static void stopBrokerConnection(String name) {
      BASE_LOGGER.stopBrokerConnection(getCaller(), name);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601739, value = "User {0} is requesting to stop broker connection {1}", format = Message.Format.MESSAGE_FORMAT)
   void stopBrokerConnection(String user, String name);

   static void startBrokerConnection(String name) {
      BASE_LOGGER.startBrokerConnection(getCaller(), name);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601740, value = "User {0} is requesting to start broker connection {1}", format = Message.Format.MESSAGE_FORMAT)
   void startBrokerConnection(String user, String name);

   static void getAddressCount(Object source) {
      BASE_LOGGER.getAddressCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601741, value = "User {0} is getting address count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressCount(String user, Object source, Object... args);

   static void getQueueCount(Object source) {
      BASE_LOGGER.getQueueCount(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601742, value = "User {0} is getting the queue count on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getQueueCount(String user, Object source, Object... args);

   static void lastValueKey(Object source) {
      BASE_LOGGER.lastValueKey(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601743, value = "User {0} is getting last-value-key property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void lastValueKey(String user, Object source, Object... args);

   static void consumersBeforeDispatch(Object source) {
      BASE_LOGGER.consumersBeforeDispatch(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601744, value = "User {0} is getting consumers-before-dispatch property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void consumersBeforeDispatch(String user, Object source, Object... args);

   static void delayBeforeDispatch(Object source) {
      BASE_LOGGER.delayBeforeDispatch(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601745, value = "User {0} is getting delay-before-dispatch property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void delayBeforeDispatch(String user, Object source, Object... args);

   static void isInternal(Object source) {
      BASE_LOGGER.isInternal(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601746, value = "User {0} is getting internal property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isInternal(String user, Object source, Object... args);

   static void isAutoCreated(Object source) {
      BASE_LOGGER.isAutoCreated(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601747, value = "User {0} is getting auto-created property on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void isAutoCreated(String user, Object source, Object... args);

   static void getMaxRetryInterval(Object source) {
      BASE_LOGGER.getMaxRetryInterval(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601748, value = "User {0} is getting max retry interval on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getMaxRetryInterval(String user, Object source, Object... args);

   static void getActivationSequence(Object source) {
      BASE_LOGGER.getActivationSequence(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601749, value = "User {0} is getting activation sequence on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getActivationSequence(String user, Object source, Object... args);

   static void purge(Object source) {
      RESOURCE_LOGGER.purge(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601750, value = "User {0} is purging target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void purge(String user, Object source, Object... args);


   static void purgeAddressSuccess(String queueName) {
      RESOURCE_LOGGER.purgeAddressSuccess(getCaller(), queueName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601751, value = "User {0} has purged address {1}", format = Message.Format.MESSAGE_FORMAT)
   void purgeAddressSuccess(String user, String queueName);


   static void purgeAddressFailure(String queueName) {
      RESOURCE_LOGGER.purgeAddressFailure(getCaller(), queueName);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601752, value = "User {0} failed to purge address {1}", format = Message.Format.MESSAGE_FORMAT)
   void purgeAddressFailure(String user, String queueName);

   static void getAddressLimitPercent(Object source) {
      BASE_LOGGER.getAddressLimitPercent(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601753, value = "User {0} is getting address limit %  on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAddressLimitPercent(String user, Object source, Object... args);

   static void block(Object source) {
      BASE_LOGGER.block(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601754, value = "User {0} is blocking target resource: {1}", format = Message.Format.MESSAGE_FORMAT)
   void block(String user, Object source);

   static void unBlock(Object source) {
      BASE_LOGGER.unBlock(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601755, value = "User {0} is unblocking target resource: {1}", format = Message.Format.MESSAGE_FORMAT)
   void unBlock(String user, Object source);

   static void getAcceptors(Object source) {
      BASE_LOGGER.getAcceptors(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601756, value = "User {0} is getting acceptors on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAcceptors(String user, Object source, Object... args);

   static void getAcceptorsAsJSON(Object source) {
      BASE_LOGGER.getAcceptorsAsJSON(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601757, value = "User {0} is getting acceptors as json on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void getAcceptorsAsJSON(String user, Object source, Object... args);

   static void schedulePageCleanup(Object source) {
      BASE_LOGGER.schedulePageCleanup(getCaller(), source);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601758, value = "User {0} is calling schedulePageCleanup on address: {1}", format = Message.Format.MESSAGE_FORMAT)
   void schedulePageCleanup(String user, Object address);

   //hot path log using a different logger
   static void addAckToTransaction(Subject user, String remoteAddress, String queue, String message, String tx) {
      MESSAGE_LOGGER.addAckToTransaction(getCaller(user, remoteAddress), queue, message, tx);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601759, value = "User {0} added acknowledgement of a message from {1}: {2} to transaction: {3}", format = Message.Format.MESSAGE_FORMAT)
   void addAckToTransaction(String user, String queue, String message, String tx);

   //hot path log using a different logger
   static void addSendToTransaction(Subject user, String remoteAddress, String messageToString, String tx) {
      MESSAGE_LOGGER.addSendToTransaction(getCaller(user, remoteAddress), messageToString, tx);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601760, value = "User {0} added a message send for: {1} to transaction: {2}", format = Message.Format.MESSAGE_FORMAT)
   void addSendToTransaction(String user, String messageToString, String tx);

   //hot path log using a different logger
   static void rolledBackTransaction(Subject user, String remoteAddress, String tx, String resource) {
      MESSAGE_LOGGER.rolledBackTransaction(getCaller(user, remoteAddress), tx, resource);
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601761, value = "User {0} rolled back transaction {1} involving {2}", format = Message.Format.MESSAGE_FORMAT)
   void rolledBackTransaction(String user, String tx, String resource);

   static void addConnector(Object source, Object... args) {
      BASE_LOGGER.addConnector(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601762, value = "User {0} is adding a connector on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void addConnector(String user, Object source, Object... args);

   static void removeConnector(Object source, Object... args) {
      BASE_LOGGER.removeConnector(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601763, value = "User {0} is removing a connector on target resource: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void removeConnector(String user, Object source, Object... args);

   static void deliverScheduledMessage(Object source, Object... args) {
      BASE_LOGGER.deliverScheduledMessage(getCaller(), source, arrayToString(args));
   }

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 601764, value = "User {0} is calling deliverScheduledMessage on queue: {1} {2}", format = Message.Format.MESSAGE_FORMAT)
   void deliverScheduledMessage(String user, Object source, Object... args);
}
