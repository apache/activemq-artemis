/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.ra;

import org.apache.activemq.artemis.ra.inflow.ActiveMQActivationSpec;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import javax.jms.IllegalStateException;

/**
 * Logger Code 15
 * <br>
 * each message id must be 6 digits long starting with 15, the 3rd digit donates
 * the level so
 * <br>
 * <pre>
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 * </pre>
 * <br>
 * so an INFO message would be 151000 to 151999
 */
@MessageLogger(projectCode = "AMQ")
public interface ActiveMQRALogger extends BasicLogger {

   /**
    * The default logger.
    */
   ActiveMQRALogger LOGGER = Logger.getMessageLogger(ActiveMQRALogger.class, ActiveMQRALogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151000, value = "awaiting topic/queue creation {0}", format = Message.Format.MESSAGE_FORMAT)
   void awaitingTopicQueueCreation(String destination);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151001, value = "Attempting to reconnect {0}", format = Message.Format.MESSAGE_FORMAT)
   void attemptingReconnect(ActiveMQActivationSpec spec);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151002, value = "Reconnected with broker", format = Message.Format.MESSAGE_FORMAT)
   void reconnected();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151003, value = "resource adaptor stopped", format = Message.Format.MESSAGE_FORMAT)
   void raStopped();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151004, value = "Instantiating {0} \"{1}\" directly since UseJNDI=false.", format = Message.Format.MESSAGE_FORMAT)
   void instantiatingDestination(String destinationType, String destination);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151005, value = "awaiting server availability", format = Message.Format.MESSAGE_FORMAT)
   void awaitingJMSServerCreation();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151006, value = "Cluster topology change detected. Re-balancing connections on even {0}.", format = Message.Format.MESSAGE_FORMAT)
   void rebalancingConnections(String event);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 151007, value = "Resource adaptor started", format = Message.Format.MESSAGE_FORMAT)
   void resourceAdaptorStarted();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152001, value = "problem resetting xa session after failure", format = Message.Format.MESSAGE_FORMAT)
   void problemResettingXASession(@Cause Throwable t);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152002, value = "Unable to roll local transaction back", format = Message.Format.MESSAGE_FORMAT)
   void unableToRollbackTX();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152003, value = "unable to reset session after failure, we will place the MDB Inflow now in setup mode for activation={0}", format = Message.Format.MESSAGE_FORMAT)
   void unableToResetSession(String spec, @Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152004, value = "Handling JMS exception failure", format = Message.Format.MESSAGE_FORMAT)
   void handlingJMSFailure(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152005, value = "Failure in broker activation {0}", format = Message.Format.MESSAGE_FORMAT)
   void failureInActivation(@Cause Throwable t, ActiveMQActivationSpec spec);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152006, value = "Unable to call after delivery", format = Message.Format.MESSAGE_FORMAT)
   void unableToCallAfterDelivery(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152007, value = "Thread {0} could not be finished", format = Message.Format.MESSAGE_FORMAT)
   void threadCouldNotFinish(String thread);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152008, value = "Error interrupting handler on endpoint {0} handler = {1}", format = Message.Format.MESSAGE_FORMAT)
   void errorInterruptingHandler(String endpoint, String handler, @Cause Throwable cause);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152009, value = "Unable to validate properties", format = Message.Format.MESSAGE_FORMAT)
   void unableToValidateProperties(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152010, value = "Unable to clear the transaction", format = Message.Format.MESSAGE_FORMAT)
   void unableToClearTheTransaction(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 152011, value = "Unable to close the factory", format = Message.Format.MESSAGE_FORMAT)
   void unableToCloseFactory(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 154000, value = "Error while creating object Reference.", format = Message.Format.MESSAGE_FORMAT)
   void errorCreatingReference(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 154001, value = "Unable to stop resource adapter.", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingRA(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 154003, value = "Unable to reconnect {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorReconnecting(@Cause Throwable t, ActiveMQActivationSpec spec);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 154004, value = "Failed to deliver message", format = Message.Format.MESSAGE_FORMAT)
   void errorDeliveringMessage(@Cause Throwable t);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 153001, value = "using different ActiveMQRAConnectionFactory", format = Message.Format.MESSAGE_FORMAT)
   void warnDifferentConnectionfactory();

   @Message(id = 153002, value = "Cannot create a subscriber on the durable subscription since it already has subscriber(s)")
   IllegalStateException canNotCreatedNonSharedSubscriber();
}
