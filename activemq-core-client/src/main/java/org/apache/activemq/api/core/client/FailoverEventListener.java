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
package org.apache.activemq.api.core.client;

/**
 * A FailoverEvent notifies the client the state if the connection changes occurred on the session.
 *
 * @author <a href="mailto:flemming.harms@gmail.com">Flemming Harms</a>
 */
public interface FailoverEventListener
{
   /**
    * Notifies that a connection state has changed according the specified event type. <br>
    * This method is called when failover is detected, if it fails and when it's completed
    *
    * @param eventType The type of event
    */
   void failoverEvent(FailoverEventType eventType);

}
