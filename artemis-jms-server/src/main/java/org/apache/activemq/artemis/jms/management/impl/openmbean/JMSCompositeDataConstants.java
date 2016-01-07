/**
 *
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
package org.apache.activemq.artemis.jms.management.impl.openmbean;

public interface JMSCompositeDataConstants {
   String JMS_DESTINATION = "JMSDestination";
   String JMS_MESSAGE_ID = "JMSMessageID";
   String JMS_TYPE = "JMSType";
   String JMS_DELIVERY_MODE = "JMSDeliveryMode";
   String JMS_EXPIRATION = "JMSExpiration";
   String JMS_PRIORITY = "JMSPriority";
   String JMS_REDELIVERED = "JMSRedelivered";
   String JMS_TIMESTAMP = "JMSTimestamp";
   String JMSXGROUP_SEQ = "JMSXGroupSeq";
   String JMSXGROUP_ID = "JMSXGroupID";
   String JMSXUSER_ID = "JMSXUserID";
   String JMS_CORRELATION_ID = "JMSCorrelationID";
   String ORIGINAL_DESTINATION = "OriginalDestination";
   String JMS_REPLY_TO = "JMSReplyTo";

   String JMS_DESTINATION_DESCRIPTION = "The message destination";
   String JMS_MESSAGE_ID_DESCRIPTION = "The message ID";
   String JMS_TYPE_DESCRIPTION = "The message type";
   String JMS_DELIVERY_MODE_DESCRIPTION = "The message delivery mode";
   String JMS_EXPIRATION_DESCRIPTION = "The message expiration";
   String JMS_PRIORITY_DESCRIPTION = "The message priority";
   String JMS_REDELIVERED_DESCRIPTION = "Is the message redelivered";
   String JMS_TIMESTAMP_DESCRIPTION = "The message timestamp";
   String JMSXGROUP_SEQ_DESCRIPTION = "The message group sequence number";
   String JMSXGROUP_ID_DESCRIPTION = "The message group ID";
   String JMSXUSER_ID_DESCRIPTION = "The user that sent the message";
   String JMS_CORRELATION_ID_DESCRIPTION = "The message correlation ID";
   String ORIGINAL_DESTINATION_DESCRIPTION = "Original Destination Before Senting To DLQ";
   String JMS_REPLY_TO_DESCRIPTION = "The reply to address";

   String BODY_LENGTH = "BodyLength";
   String BODY_PREVIEW = "BodyPreview";
   String CONTENT_MAP = "ContentMap";
   String MESSAGE_TEXT = "Text";
   String MESSAGE_URL = "Url";


}
