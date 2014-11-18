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
package org.apache.activemq.rest.queue.push;

import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.rest.queue.push.xml.PushRegistration;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public interface PushStrategy
{
   /**
    * Return false if unable to connect. Push consumer may be disabled if configured to do so when
    * unable to connect. Throw an exception if the message sent was unaccepted by the receiver.
    * Hornetq's retry and dead letter logic will take over from there.
    *
    * @param message
    * @return {@code false} if unable to connect
    */
   boolean push(ClientMessage message);

   void setRegistration(PushRegistration reg);

   void start() throws Exception;

   void stop() throws Exception;
}
