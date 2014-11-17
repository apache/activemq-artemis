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
package org.apache.activemq6.rest.queue;

import org.apache.activemq6.api.core.client.ClientMessage;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class Acknowledgement
{
   private final String ackToken;
   private final ClientMessage message;
   private boolean wasSet;
   private boolean acknowledged;

   public Acknowledgement(String ackToken, ClientMessage message)
   {
      this.ackToken = ackToken;
      this.message = message;
   }

   public String getAckToken()
   {
      return ackToken;
   }

   public ClientMessage getMessage()
   {
      return message;
   }

   public boolean wasSet()
   {
      return wasSet;
   }

   public void acknowledge()
   {
      if (wasSet) throw new RuntimeException("Ack state is immutable");
      wasSet = true;
      acknowledged = true;
   }

   public void unacknowledge()
   {
      if (wasSet) throw new RuntimeException("Ack state is immutable");
      wasSet = true;
   }

   public boolean isAcknowledged()
   {
      return acknowledged;
   }
}