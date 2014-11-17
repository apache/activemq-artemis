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
 * A MessageHandler is used to receive message <em>asynchronously</em>.
 * <p>
 * To receive messages asynchronously, a MessageHandler is set on a ClientConsumer. Every time the
 * consumer will receive a message, it will call the handler's {@code onMessage()} method.
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @see ClientConsumer#setMessageHandler(MessageHandler)
 */
public interface MessageHandler
{
   /**
    * Notifies the MessageHandler that a message has been received.
    *
    * @param message a message
    */
   void onMessage(ClientMessage message);
}
