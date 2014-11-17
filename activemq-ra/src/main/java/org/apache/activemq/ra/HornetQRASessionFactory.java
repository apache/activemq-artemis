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
package org.apache.activemq6.ra;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.XAQueueConnection;
import javax.jms.XATopicConnection;

/**
 * A joint interface for all connection types
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: 71554 $
 */
public interface HornetQRASessionFactory extends XATopicConnection, XAQueueConnection
{
   /** Error message for strict behaviour */
   String ISE = "This method is not applicable inside the application server. See the J2EE spec, e.g. J2EE1.4 Section 6.6";

   /**
    * Add a temporary queue
    * @param temp The temporary queue
    */
   void addTemporaryQueue(TemporaryQueue temp);

   /**
    * Add a temporary topic
    * @param temp The temporary topic
    */
   void addTemporaryTopic(TemporaryTopic temp);

   /**
    * Notification that a session is closed
    * @param session The session
    * @throws JMSException for any error
    */
   void closeSession(HornetQRASession session) throws JMSException;
}
