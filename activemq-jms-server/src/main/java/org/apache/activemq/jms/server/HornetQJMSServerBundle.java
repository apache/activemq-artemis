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
package org.apache.activemq.jms.server;


import org.apache.activemq.api.core.ActiveMQAddressExistsException;
import org.apache.activemq.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.api.core.ActiveMQInternalErrorException;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;
import org.jboss.logging.Messages;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/12/12
 *
 * Logger Code 12
 *
 * each message id must be 6 digits long starting with 10, the 3rd digit should be 9
 *
 * so 129000 to 129999
 */
@MessageBundle(projectCode = "HQ")
public interface HornetQJMSServerBundle
{
   HornetQJMSServerBundle BUNDLE = Messages.getBundle(HornetQJMSServerBundle.class);

   @Message(id = 129000, value =  "Connection Factory {0} does not exist" , format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException cfDoesntExist(String name);

   @Message(id = 129001, value =  "Invalid signature {0} parsing Connection Factory" , format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException invalidSignatureParsingCF(String sig);

   @Message(id = 129002, value = "Invalid node {0} parsing Connection Factory", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQInternalErrorException invalidNodeParsingCF(String name);

   @Message(id = 129003, value = "Discovery Group ''{0}'' does not exist on main config", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException discoveryGroupDoesntExist(String name);

   @Message(id = 129004, value = "No Connector name configured on create ConnectionFactory", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException noConnectorNameOnCF();

   @Message(id = 129005, value = "Connector ''{0}'' not found on the main configuration file" , format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException noConnectorNameConfiguredOnCF(String name);

   @Message(id = 129006, value =  "JNDI {0} is already being used by another connection factory", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQAddressExistsException cfJndiExists(String name);

   @Message(id = 129007, value = "Error decoding password using codec instance", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException errorDecodingPassword(@Cause Exception e);
}
