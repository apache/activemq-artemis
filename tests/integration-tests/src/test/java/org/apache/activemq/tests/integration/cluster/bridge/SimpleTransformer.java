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
package org.apache.activemq6.tests.integration.cluster.bridge;

import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.core.server.ServerMessage;
import org.apache.activemq6.core.server.cluster.Transformer;

/**
 * A SimpleTransformer
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 21 Nov 2008 11:44:37
 *
 *
 */
public class SimpleTransformer implements Transformer
{
   public ServerMessage transform(final ServerMessage message)
   {
      SimpleString oldProp = (SimpleString)message.getObjectProperty(new SimpleString("wibble"));

      if (!oldProp.equals(new SimpleString("bing")))
      {
         throw new IllegalStateException("Wrong property value!!");
      }

      // Change a property
      message.putStringProperty(new SimpleString("wibble"), new SimpleString("bong"));

      // Change the body
      HornetQBuffer buffer = message.getBodyBuffer();

      buffer.readerIndex(0);

      String str = buffer.readString();

      if (!str.equals("doo be doo be doo be doo"))
      {
         throw new IllegalStateException("Wrong body!!");
      }

      buffer.clear();

      buffer.writeString("dee be dee be dee be dee");

      return message;
   }

}
