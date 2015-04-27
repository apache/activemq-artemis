/**
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
package org.apache.activemq.artemis.integration.aerogear;

import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;
import org.jboss.logging.Messages;

/**
 *         Logger Code 23
 *         <p/>
 *         each message id must be 6 digits long starting with 10, the 3rd digit should be 9
 *         <p/>
 *         so 239000 to 239999
 */
@MessageBundle(projectCode = "AMQ")
public interface ActiveMQAeroGearBundle
{
   ActiveMQAeroGearBundle BUNDLE = Messages.getBundle(ActiveMQAeroGearBundle.class);

   @Message(id = 239000, value = "endpoint can not be null")
   ActiveMQIllegalStateException endpointNull();

   @Message(id = 239001, value = "application-id can not be null")
   ActiveMQIllegalStateException applicationIdNull();

   @Message(id = 239002, value = "master-secret can not be null")
   ActiveMQIllegalStateException masterSecretNull();

   @Message(id = 239003, value = "{0}: queue {1} not found", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException noQueue(String connectorName, String queueName);
}
