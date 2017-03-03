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
package org.apache.activemq.artemis.tests.integration.cluster.bridge;


import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.cluster.Transformer;

public class SimpleTransformer implements Transformer {

   @Override
   public Message transform(final Message message) {

      // TODO-now: fix this test!!!

      throw new RuntimeException(("Fix me"));
//      SimpleString oldProp = (SimpleString) message.getObjectProperty(new SimpleString("wibble"));
//
//      if (!oldProp.equals(new SimpleString("bing"))) {
//         throw new IllegalStateException("Wrong property value!!");
//      }
//
//      // Change a property
//      message.putStringProperty(new SimpleString("wibble"), new SimpleString("bong"));
//
//      // Change the body
//      ActiveMQBuffer buffer = message.getBodyBuffer();
//
//      buffer.readerIndex(0);
//
//      String str = buffer.readString();
//
//      if (!str.equals("doo be doo be doo be doo")) {
//         throw new IllegalStateException("Wrong body!!");
//      }
//
//      buffer.clear();
//
//      buffer.writeString("dee be dee be dee be dee");
//
//      return message;
   }

}
