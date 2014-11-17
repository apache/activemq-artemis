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

/**
 * To be called right after the ConnectionFactory created a connection.
 * This listener is not part of the API and shouldn't be used by users.
 * (if you do so we can't guarantee any API compatibility on this class)
 *
 * @author clebertsuconic
 *
 *
 */
public interface AfterConnectInternalListener
{
   void onConnection(ClientSessionFactoryInternal sf);
}
