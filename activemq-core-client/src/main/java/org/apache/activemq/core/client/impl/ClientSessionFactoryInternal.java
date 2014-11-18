/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.core.client.impl;

import java.util.concurrent.locks.Lock;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.SessionFailureListener;
import org.apache.activemq.utils.ConfirmationWindowWarning;

/**
 * A ClientSessionFactoryInternal
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 *
 */
public interface ClientSessionFactoryInternal extends ClientSessionFactory
{
   void causeExit();

   void addFailureListener(SessionFailureListener listener);

   boolean removeFailureListener(SessionFailureListener listener);

   void disableFinalizeCheck();

   String getLiveNodeId();

   // for testing

   int numConnections();

   int numSessions();

   void removeSession(final ClientSessionInternal session, boolean failingOver);

   void connect(int reconnectAttempts, boolean failoverOnInitialConnection) throws ActiveMQException;

   void setBackupConnector(TransportConfiguration live, TransportConfiguration backUp);

   Object getConnector();

   Object getBackupConnector();

   void setReconnectAttempts(int i);

   ConfirmationWindowWarning getConfirmationWindowWarning();

   Lock lockFailover();
}
