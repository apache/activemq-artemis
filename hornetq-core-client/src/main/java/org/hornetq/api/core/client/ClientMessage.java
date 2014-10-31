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

import java.io.InputStream;
import java.io.OutputStream;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;

/**
 *
 * A ClientMessage represents a message sent and/or received by HornetQ.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface ClientMessage extends Message
{
   /**
    * Returns the number of times this message was delivered.
    */
   int getDeliveryCount();

   /**
    * Sets the delivery count for this message.
    * <p>
    * This method is not meant to be called by HornetQ clients.
    * @param deliveryCount message delivery count
    */
   void setDeliveryCount(int deliveryCount);

   /**
    * Acknowledges reception of this message.
    * <p>
    * If the session responsible to acknowledge this message has {@code autoCommitAcks} set to
    * {@code true}, the transaction will automatically commit the current transaction. Otherwise,
    * this acknowledgement will not be committed until the client commits the session transaction.
    * @throws HornetQException if an error occurred while acknowledging the message.
    * @see ClientSession#isAutoCommitAcks()
    */
   void acknowledge() throws HornetQException;

   /**
    * Acknowledges reception of a single message.
    * <p>
    * If the session responsible to acknowledge this message has {@code autoCommitAcks} set to
    * {@code true}, the transaction will automatically commit the current transaction. Otherwise,
    * this acknowledgement will not be committed until the client commits the session transaction.
    * @throws HornetQException if an error occurred while acknowledging the message.
    * @see ClientSession#isAutoCommitAcks()
    */
   void individualAcknowledge() throws HornetQException;

   /**
    * This can be optionally used to verify if the entire message has been received.
    * It won't have any effect on regular messages but it may be helpful on large messages.
    * The use case for this is to make sure there won't be an exception while getting the buffer.
    * Using getBodyBuffer directly would have the same effect but you could get a Runtime non checked Exception
    * instead
    * @throws HornetQException
    */
   void checkCompletion() throws HornetQException;

   /**
    * Returns the size (in bytes) of this message's body
    */
   int getBodySize();

   /**
    * Sets the OutputStream that will receive the content of a message received in a non blocking way.
    * <br>
    * This method is used when consuming large messages
    *
    * @throws HornetQException
    */
   void setOutputStream(OutputStream out) throws HornetQException;

   /**
    * Saves the content of the message to the OutputStream.
    * It will block until the entire content is transferred to the OutputStream.
    * <br>
    *
    * @throws HornetQException
    */
   void saveToOutputStream(OutputStream out) throws HornetQException;

   /**
    * Wait the outputStream completion of the message.
    *
    * This method is used when consuming large messages
    *
    * @param timeMilliseconds - 0 means wait forever
    * @return true if it reached the end
    * @throws HornetQException
    */
   boolean waitOutputStreamCompletion(long timeMilliseconds) throws HornetQException;

   /**
    * Sets the body's IntputStream.
    * <br>
    * This method is used when sending large messages
    */
   void setBodyInputStream(InputStream bodyInputStream);

}
