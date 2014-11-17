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
package org.apache.activemq6.rest.queue;

import org.apache.activemq6.api.core.client.ClientMessage;

import javax.ws.rs.core.Response;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class ConsumedHttpMessage extends ConsumedMessage
{
   private byte[] data;

   public ConsumedHttpMessage(ClientMessage message)
   {
      super(message);
   }

   @Override
   public void build(Response.ResponseBuilder builder)
   {
      buildHeaders(builder);
      if (data == null)
      {
         int size = message.getBodySize();
         if (size > 0)
         {
            data = new byte[size];
            message.getBodyBuffer().readBytes(data);
         }
         else
         {
            data = new byte[0];
         }
      }
      builder.entity(data);
   }
}
