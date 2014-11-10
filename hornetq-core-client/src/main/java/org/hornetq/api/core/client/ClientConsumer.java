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
package org.hornetq.api.core.client;

import org.hornetq.api.core.HornetQException;
import org.hornetq.spi.core.remoting.ConsumerContext;

/**
 * A ClientConsumer receives messages from HornetQ queues.
 * <br>
 * Messages can be consumed synchronously by using the <code>receive()</code> methods
 * which will block until a message is received (or a timeout expires) or asynchronously
 * by setting a {@link MessageHandler}.
 * <br>
 * These 2 types of consumption are exclusive: a ClientConsumer with a MessageHandler set will
 * throw HornetQException if its <code>receive()</code> methods are called.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 * @see ClientSession#createConsumer(String)
 */
public interface ClientConsumer extends AutoCloseable
{

   /**
    * The server's ID associated with this consumer.
    * HornetQ implements this as a long but this could be protocol dependent.
    * @return
    */
   ConsumerContext getConsumerContext();

   /**
    * Receives a message from a queue.
    * <p>
    * This call will block indefinitely until a message is received.
    * <p>
    * Calling this method on a closed consumer will throw a HornetQException.
    * @return a ClientMessage
    * @throws HornetQException if an exception occurs while waiting to receive a message
    */
   ClientMessage receive() throws HornetQException;

   /**
    * Receives a message from a queue.
    * <p>
    * This call will block until a message is received or the given timeout expires.
    * <p>
    * Calling this method on a closed consumer will throw a HornetQException.
    * @param timeout time (in milliseconds) to wait to receive a message
    * @return a message or {@code null} if the time out expired
    * @throws HornetQException if an exception occurs while waiting to receive a message
    */
   ClientMessage receive(long timeout) throws HornetQException;

   /**
    * Receives a message from a queue. This call will force a network trip to HornetQ server to
    * ensure that there are no messages in the queue which can be delivered to this consumer.
    * <p>
    * This call will never wait indefinitely for a message, it will return {@code null} if no
    * messages are available for this consumer.
    * <p>
    * Note however that there is a performance cost as an additional network trip to the server may
    * required to check the queue status.
    * <p>
    * Calling this method on a closed consumer will throw a HornetQException.
    * @return a message or {@code null} if there are no messages in the queue for this consumer
    * @throws HornetQException if an exception occurs while waiting to receive a message
    */
   ClientMessage receiveImmediate() throws HornetQException;

   /**
    * Returns the MessageHandler associated to this consumer.
    * <p>
    * Calling this method on a closed consumer will throw a HornetQException.
    * @return the MessageHandler associated to this consumer or {@code null}
    * @throws HornetQException if an exception occurs while getting the MessageHandler
    */
   MessageHandler getMessageHandler() throws HornetQException;

   /**
    * Sets the MessageHandler for this consumer to consume messages asynchronously.
    * <p>
    * Calling this method on a closed consumer will throw a HornetQException.
    * @param handler a MessageHandler
    * @throws HornetQException if an exception occurs while setting the MessageHandler
    */
   ClientConsumer setMessageHandler(MessageHandler handler) throws HornetQException;

   /**
    * Closes the consumer.
    * <p>
    * Once this consumer is closed, it can not receive messages, whether synchronously or
    * asynchronously.
    * @throws HornetQException
    */
   void close() throws HornetQException;

   /**
    * Returns whether the consumer is closed or not.
    *
    * @return <code>true</code> if this consumer is closed, <code>false</code> else
    */
   boolean isClosed();

   /**
    * Returns the last exception thrown by a call to this consumer's MessageHandler.
    *
    * @return the last exception thrown by a call to this consumer's MessageHandler or {@code null}
    */
   Exception getLastException();
}
