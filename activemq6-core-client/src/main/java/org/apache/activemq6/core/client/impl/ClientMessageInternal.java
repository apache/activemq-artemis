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

package org.apache.activemq6.core.client.impl;

import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.api.core.client.ClientMessage;
import org.apache.activemq6.utils.TypedProperties;

/**
 * A ClientMessageInternal
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface ClientMessageInternal extends ClientMessage
{

   TypedProperties getProperties();

   /** Size used for FlowControl */
   int getFlowControlSize();

   /** Size used for FlowControl */
   void setFlowControlSize(int flowControlSize);

   void setAddressTransient(SimpleString address);

   void onReceipt(ClientConsumerInternal consumer);

   /**
    * Discard unused packets (used on large-message)
    */
   void discardBody();

   boolean isCompressed();
}
