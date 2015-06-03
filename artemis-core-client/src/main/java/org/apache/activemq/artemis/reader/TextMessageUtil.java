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
package org.apache.activemq.artemis.reader;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;

public class TextMessageUtil extends MessageUtil
{

   /**
    * Utility method to set the Text message on a message body
    */
   public static void writeBodyText(Message message, SimpleString text)
   {
      ActiveMQBuffer buff = getBodyBuffer(message);
      buff.clear();
      buff.writeNullableSimpleString(text);
   }

   /**
    * Utility method to set the Text message on a message body
    */
   public static SimpleString readBodyText(Message message)
   {
      ActiveMQBuffer buff = getBodyBuffer(message);
      buff.resetReaderIndex();
      return buff.readNullableSimpleString();
   }

}
