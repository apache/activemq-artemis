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

import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.resource.NotSupportedException;

import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Logger Code 15
 *
 * each message id must be 6 digits long starting with 15, the 3rd digit should be 9
 *
 * so 159000 to 159999
 */
@MessageBundle(projectCode = "AMQ")
public interface ActiveMQRABundle {

   /**
    * Error message for strict behaviour
    */
   String ISE = "This method is not applicable inside the application server. See the JEE spec, e.g. JEE 7 Section 6.7";

   ActiveMQRABundle BUNDLE = Messages.getBundle(ActiveMQRABundle.class);

   @Message(id = 159000, value = "Error decoding password using codec instance")
   ActiveMQIllegalStateException errorDecodingPassword(@Cause Exception e);

   @Message(id = 159001, value = "MDB cannot be deployed as it has no Activation Spec. Please provide an Activation!")
   NotSupportedException noActivationSpec();

   @Message(id = 159002, value = "Please provide a destination for the MDB")
   IllegalArgumentException noDestinationName();

   @Message(id = 159003, value = ISE)
   JMSRuntimeException illegalJEEMethod();

   @Message(id = 159004, value = "Invalid Session Mode SESSION_TRANSACTED")
   JMSRuntimeException invalidSessionTransactedModeRuntime();

   @Message(id = 159005, value = "Invalid Session Mode CLIENT_ACKNOWLEDGE")
   JMSRuntimeException invalidClientAcknowledgeModeRuntime();

   @Message(id = 159006, value = "Invalid Session Mode {0}", format = Message.Format.MESSAGE_FORMAT)
   JMSRuntimeException invalidAcknowledgeMode(int sessionMode);

   @Message(id = 159007, value = "Invalid Session Mode SESSION_TRANSACTED, to enable Local Transacted Sessions you can " +
         "set the allowLocalTransactions (allow-local-transactions) on the resource adapter")
   JMSException invalidSessionTransactedModeRuntimeAllowLocal();
}
