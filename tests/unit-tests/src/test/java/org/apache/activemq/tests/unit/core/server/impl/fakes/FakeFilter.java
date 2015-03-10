/**
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
package org.apache.activemq.tests.unit.core.server.impl.fakes;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.filter.Filter;
import org.apache.activemq.core.server.ServerMessage;

/**
 * A FakeFilter
 */
public class FakeFilter implements Filter
{
   private String headerName;

   private Object headerValue;

   public FakeFilter(final String headerName, final Object headerValue)
   {
      this.headerName = headerName;

      this.headerValue = headerValue;
   }

   public FakeFilter()
   {
   }

   public boolean match(final ServerMessage message)
   {
      if (headerName != null)
      {
         Object value = message.getObjectProperty(new SimpleString(headerName));

         if (value instanceof SimpleString)
         {
            value = ((SimpleString)value).toString();
         }

         if (value != null && headerValue.equals(value))
         {
            return true;
         }

         return false;
      }

      return true;
   }

   public SimpleString getFilterString()
   {
      return null;
   }
}
