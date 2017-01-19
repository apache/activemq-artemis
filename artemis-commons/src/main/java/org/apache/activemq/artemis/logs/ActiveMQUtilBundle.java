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
package org.apache.activemq.artemis.logs;

import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Logger Code 20
 *
 * each message id must be 6 digits long starting with 20, the 3rd digit should be 9
 *
 * so 209000 to 209999
 */
@MessageBundle(projectCode = "AMQ")
public interface ActiveMQUtilBundle {

   ActiveMQUtilBundle BUNDLE = Messages.getBundle(ActiveMQUtilBundle.class);

   @Message(id = 209000, value = "invalid property: {0}", format = Message.Format.MESSAGE_FORMAT)
   ActiveMQIllegalStateException invalidProperty(String part);

   @Message(id = 209001, value = "Invalid type: {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException invalidType(Byte type);

   @Message(id = 209002, value = "the specified string is too long ({0})", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException stringTooLong(Integer length);

   @Message(id = 209003, value = "Error instantiating codec {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException errorCreatingCodec(@Cause Exception e, String codecClassName);

   @Message(id = 209004, value = "Failed to parse long value from {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException failedToParseLong(String value);
}
