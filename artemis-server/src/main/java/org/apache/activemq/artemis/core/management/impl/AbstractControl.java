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
package org.apache.activemq.artemis.core.management.impl;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.DummyOperationContext;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.logs.AuditLogger;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.RunnableEx;
import org.apache.activemq.artemis.utils.UUIDGenerator;

public abstract class AbstractControl extends StandardMBean {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected final StorageManager storageManager;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AbstractControl(final Class<?> clazz, final StorageManager storageManager) throws NotCompliantMBeanException {
      super(clazz);
      this.storageManager = storageManager;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void clearIO() {
      // the storage manager could be null on the backup on certain components
      if (storageManager != null) {
         storageManager.clearContext();
      }
   }

   protected void blockOnIO() {
      // the storage manager could be null on the backup on certain components
      if (storageManager != null) {
         try {
            storageManager.waitOnOperations();
            storageManager.clearContext();
         } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
         }
      }

   }

   protected abstract MBeanOperationInfo[] fillMBeanOperationInfo();

   protected abstract MBeanAttributeInfo[] fillMBeanAttributeInfo();

   protected Object tcclCall(ClassLoader loader, Callable<Object> callable) throws Exception {
      ClassLoader originalTCCL = Thread.currentThread().getContextClassLoader();
      try {
         Thread.currentThread().setContextClassLoader(loader);
         return callable.call();
      } finally {
         Thread.currentThread().setContextClassLoader(originalTCCL);
      }
   }

   protected void tcclInvoke(ClassLoader loader, RunnableEx runnableEx) throws Exception {
      ClassLoader originalTCCL = Thread.currentThread().getContextClassLoader();
      try {
         Thread.currentThread().setContextClassLoader(loader);
         runnableEx.run();
      } finally {
         Thread.currentThread().setContextClassLoader(originalTCCL);
      }
   }

   @Override
   public MBeanInfo getMBeanInfo() {
      if (AuditLogger.isEnabled()) {
         AuditLogger.getMBeanInfo(this);
      }
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(), info.getDescription(), fillMBeanAttributeInfo(), info.getConstructors(), fillMBeanOperationInfo(), info.getNotifications());
   }

   protected String sendMessage(SimpleString address,
                                ActiveMQServer server,
                                Map<String, String> headers,
                                int type,
                                String body,
                                boolean durable,
                                String user,
                                String password,
                                Long...queueID) throws Exception {
      ManagementRemotingConnection fakeConnection = new ManagementRemotingConnection();
      ServerSession serverSession = server.createSession("management::" + UUIDGenerator.getInstance().generateStringUUID(), user, password,
                                                         Integer.MAX_VALUE, fakeConnection,
                                                         true, true, false,
                                                         false, address.toString(), fakeConnection.callback,
                                                         false, new DummyOperationContext(), Collections.emptyMap(), null);
      try {
         CoreMessage message = new CoreMessage(storageManager.generateID(), 50);
         if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
               message.putStringProperty(header.getKey(), header.getValue());
            }
         }
         message.setType((byte) type);
         message.setDurable(durable);
         message.setTimestamp(System.currentTimeMillis());
         if (body != null) {
            if (type == Message.TEXT_TYPE) {
               message.getBodyBuffer().writeNullableSimpleString(new SimpleString(body));
            } else {
               message.getBodyBuffer().writeBytes(Base64.decode(body));
            }
         }

         message.setAddress(address);

         // if a queueID is used, we set the routeToIDs property
         // to one or many specific queues
         if (queueID != null && queueID.length > 0) {
            ByteBuffer buffer = ByteBuffer.allocate(8 * queueID.length);
            for (Long q : queueID) {
               buffer.putLong(q);
            }
            message.putBytesProperty(Message.HDR_ROUTE_TO_IDS, buffer.array());
         }

         // There's no point on direct delivery using the management thread, use false here
         serverSession.send(message, false);
         return "" + message.getMessageID();
      } finally {
         try {
            serverSession.close(false);
         } catch (Exception ignored) {
         }
      }
   }
   // Inner classes------------------------------------------------

}
