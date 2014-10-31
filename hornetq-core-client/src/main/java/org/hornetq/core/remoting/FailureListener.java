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
package org.hornetq.core.remoting;

import org.hornetq.api.core.HornetQException;

/**
 * A FailureListener notifies the user when a connection failure occurred.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public interface FailureListener
{
   /**
    * Notifies that a connection has failed due to the specified exception.
    *
    * @param exception exception which has caused the connection to fail
    * @param failedOver
    */
   void connectionFailed(HornetQException exception, boolean failedOver);

   /**
    * Notifies that a connection has failed due to the specified exception.
    *
    * @param exception exception which has caused the connection to fail
    * @param failedOver
    * @param scaleDownTargetNodeID the ID of the node to which messages are scaling down
    */
   void connectionFailed(HornetQException exception, boolean failedOver, String scaleDownTargetNodeID);
}
