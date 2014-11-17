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
package org.apache.activemq6.utils;


import org.apache.activemq6.api.core.HornetQIllegalStateException;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;
import org.jboss.logging.Messages;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/12/12
 *
 * Logger Code 20
 *
 * each message id must be 6 digits long starting with 20, the 3rd digit should be 9
 *
 * so 209000 to 209999
 */
@MessageBundle(projectCode = "HQ")
public interface HornetQUtilBundle
{
   HornetQUtilBundle BUNDLE = Messages.getBundle(HornetQUtilBundle.class);

   @Message(id = 209000, value = "invalid property: {0}" , format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException invalidProperty(String part);

   @Message(id = 209001, value = "Invalid type: {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException invalidType(Byte type);

   @Message(id = 209002, value = "the specified string is too long ({0})", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException stringTooLong(Integer length);

   @Message(id = 209003, value = "Error instantiating codec {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException errorCreatingCodec(@Cause Exception e, String codecClassName);
}
