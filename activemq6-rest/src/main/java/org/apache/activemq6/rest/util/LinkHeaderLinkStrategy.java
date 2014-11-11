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
package org.apache.activemq6.rest.util;

import org.jboss.resteasy.spi.Link;

import javax.ws.rs.core.Response;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class LinkHeaderLinkStrategy implements LinkStrategy
{
   /**
    * @param builder
    * @param title   user friendly name
    * @param rel     could be a link
    * @param href
    * @param type
    */
   public void setLinkHeader(Response.ResponseBuilder builder, String title, String rel, String href, String type)
   {
      Link link = new Link(title, rel, href, type, null);
      setLinkHeader(builder, link);
   }

   public void setLinkHeader(Response.ResponseBuilder builder, Link link)
   {
      builder.header("Link", link);
   }

   /* proprietary, keep this around just in case

   public static void setLinkHeader(Response.ResponseBuilder builder, String rel, String href, String type)
   {
      String link = "Link-Href-" + rel;
      builder.header(link, href);
      if (type != null)
      {
         String typeHeader = "Link-Type-" + rel;
         builder.header(typeHeader, type);
      }
   }
    */
}
