/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq;

import junit.framework.TestCase;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;

public class MessageTransformationTest extends TestCase {

   /**
    * Sets up the resources of the unit test.
    *
    * @throws Exception
    */
   @Override
   protected void setUp() throws Exception {
   }

   /**
    * Clears up the resources used in the unit test.
    */
   @Override
   protected void tearDown() throws Exception {
   }

   /**
    * Tests transforming destinations into ActiveMQ's destination
    * implementation.
    */
   public void testTransformDestination() throws Exception {
      assertTrue("Transforming a TempQueue destination to an ActiveMQTempQueue", ActiveMQMessageTransformation.transformDestination(new ActiveMQTempQueue()) instanceof ActiveMQTempQueue);

      assertTrue("Transforming a TempTopic destination to an ActiveMQTempTopic", ActiveMQMessageTransformation.transformDestination(new ActiveMQTempTopic()) instanceof ActiveMQTempTopic);

      assertTrue("Transforming a Queue destination to an ActiveMQQueue", ActiveMQMessageTransformation.transformDestination(new ActiveMQQueue()) instanceof ActiveMQQueue);

      assertTrue("Transforming a Topic destination to an ActiveMQTopic", ActiveMQMessageTransformation.transformDestination(new ActiveMQTopic()) instanceof ActiveMQTopic);

      assertTrue("Transforming a Destination to an ActiveMQDestination", ActiveMQMessageTransformation.transformDestination(new ActiveMQTopic()) instanceof ActiveMQDestination);
   }

   /**
    * Tests transforming messages into ActiveMQ's message implementation.
    */
   public void testTransformMessage() throws Exception {
      assertTrue("Transforming a BytesMessage message into an ActiveMQBytesMessage", ActiveMQMessageTransformation.transformMessage(new ActiveMQBytesMessage(), null) instanceof ActiveMQBytesMessage);

      assertTrue("Transforming a MapMessage message to an ActiveMQMapMessage", ActiveMQMessageTransformation.transformMessage(new ActiveMQMapMessage(), null) instanceof ActiveMQMapMessage);

      assertTrue("Transforming an ObjectMessage message to an ActiveMQObjectMessage", ActiveMQMessageTransformation.transformMessage(new ActiveMQObjectMessage(), null) instanceof ActiveMQObjectMessage);

      assertTrue("Transforming a StreamMessage message to an ActiveMQStreamMessage", ActiveMQMessageTransformation.transformMessage(new ActiveMQStreamMessage(), null) instanceof ActiveMQStreamMessage);

      assertTrue("Transforming a TextMessage message to an ActiveMQTextMessage", ActiveMQMessageTransformation.transformMessage(new ActiveMQTextMessage(), null) instanceof ActiveMQTextMessage);

      assertTrue("Transforming an ActiveMQMessage message to an ActiveMQMessage", ActiveMQMessageTransformation.transformMessage(new ActiveMQMessage(), null) instanceof ActiveMQMessage);
   }
}
