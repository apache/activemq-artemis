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
package org.apache.activemq.rest.test;

import org.apache.activemq.rest.util.Constants;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.spi.Link;
import org.junit.Assert;

public final class Util
{
   private Util()
   {
      // Utility class
   }

   static ClientResponse head(ClientRequest request) throws Exception
   {
      ClientResponse<?> response = request.head();
      response.releaseConnection();
      Assert.assertEquals(200, response.getStatus());
      return response;
   }

   static String getUrlPath(String queueName)
   {
      return Constants.PATH_FOR_QUEUES + "/" + queueName;
   }

   static ClientResponse setAutoAck(Link link, boolean ack) throws Exception
   {
      ClientResponse response;
      response = link.request().formParameter("autoAck", Boolean.toString(ack)).post();
      response.releaseConnection();
      Assert.assertEquals(201, response.getStatus());
      return response;
   }
}
