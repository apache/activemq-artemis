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
package org.apache.activemq.integration.jboss;

import javax.security.auth.Subject;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/15/12
 *
 * Logger Code 13
 *
 * each message id must be 6 digits long starting with 13, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 131000 to 131999
 */
@MessageLogger(projectCode = "AMQ")
public interface ActiveMQJBossLogger extends BasicLogger
{
   /**
    * The jboss integration logger.
    */
   ActiveMQJBossLogger LOGGER = Logger.getMessageLogger(ActiveMQJBossLogger.class, ActiveMQJBossLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 131001, value = "Security Context Setting Subject = {0}",
            format = Message.Format.MESSAGE_FORMAT)
   void settingSecuritySubject(Subject subject);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 132001, value = "An error happened while setting the context",
            format = Message.Format.MESSAGE_FORMAT)
   void errorSettingSecurityContext(@Cause Throwable Throwable);
}
