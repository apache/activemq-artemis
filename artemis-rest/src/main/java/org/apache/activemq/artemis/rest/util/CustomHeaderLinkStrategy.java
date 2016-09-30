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
package org.apache.activemq.artemis.rest.util;

import javax.ws.rs.core.Response;

public class CustomHeaderLinkStrategy implements LinkStrategy {

   @Override
   public void setLinkHeader(Response.ResponseBuilder builder, String title, String rel, String href, String type) {
      String headerName = null;
      if (title != null) {
         headerName = title;
      } else if (rel != null) {
         headerName = rel;
      } else {
         throw new RuntimeException("Cannot figure out header name");
      }
      headerName = "msg-" + headerName;
      builder.header(headerName, href);
      if (type != null) {
         builder.header(headerName + "-type", type);
      }
   }
}
