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
package org.apache.activemq.artemis.jms.example;

import org.apache.activemq.artemis.api.core.SimpleString;

import org.apache.activemq.artemis.core.server.cluster.Transformer;

public class HatColourChangeTransformer implements Transformer {

   @Override
   public ServerMessage transform(final ServerMessage message) {
      SimpleString propName = new SimpleString("hat");

      SimpleString oldProp = message.getSimpleStringProperty(propName);

      // System.out.println("Old hat colour is " + oldProp);

      // Change the colour
      message.putStringProperty(propName, new SimpleString("blue"));

      return message;
   }

}
