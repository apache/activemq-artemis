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
package org.apache.activemq6.rest.integration;

import org.apache.activemq6.jms.server.embedded.EmbeddedJMS;
import org.apache.activemq6.spi.core.naming.BindingRegistry;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class EmbeddedRestHornetQJMS extends EmbeddedRestHornetQ
{
   @Override
   protected void initEmbeddedHornetQ()
   {
      embeddedHornetQ = new EmbeddedJMS();
   }

   public BindingRegistry getRegistry()
   {
      if (embeddedHornetQ == null) return null;
      return ((EmbeddedJMS)embeddedHornetQ).getRegistry();
   }

}
