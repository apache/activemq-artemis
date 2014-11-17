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

package org.proton.plug.handler;

import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;

/**
 * EventHandler
 * <p/>
 *
 * @author rhs
 */

public interface EventHandler
{

   void onSASLInit(ProtonHandler handler, Connection connection);

   void onInit(Connection connection) throws Exception;

   void onLocalOpen(Connection connection) throws Exception;

   void onRemoteOpen(Connection connection) throws Exception;

   void onLocalClose(Connection connection) throws Exception;

   void onRemoteClose(Connection connection) throws Exception;

   void onFinal(Connection connection) throws Exception;

   void onInit(Session session) throws Exception;

   void onLocalOpen(Session session) throws Exception;

   void onRemoteOpen(Session session) throws Exception;

   void onLocalClose(Session session) throws Exception;

   void onRemoteClose(Session session) throws Exception;

   void onFinal(Session session) throws Exception;

   void onInit(Link link) throws Exception;

   void onLocalOpen(Link link) throws Exception;

   void onRemoteOpen(Link link) throws Exception;

   void onLocalClose(Link link) throws Exception;

   void onRemoteClose(Link link) throws Exception;

   void onFlow(Link link) throws Exception;

   void onFinal(Link link) throws Exception;

   void onRemoteDetach(Link link) throws Exception;

   void onDetach(Link link) throws Exception;

   void onDelivery(Delivery delivery) throws Exception;

   void onTransport(Transport transport) throws Exception;

}
