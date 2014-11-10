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
package org.hornetq.factory;

import org.hornetq.cli.ConfigurationException;
import org.hornetq.dto.BrokerDTO;
import org.hornetq.dto.XmlUtil;

import java.io.File;
import java.net.URI;

public class XmlBrokerFactoryHandler implements BrokerFactoryHandler
{
   @Override
   public BrokerDTO createBroker(URI brokerURI) throws Exception
   {
      File file = new File(brokerURI.getSchemeSpecificPart());
      if (!file.exists())
      {
         throw new ConfigurationException("Invalid configuration URI, can't find file: " + file.getName());
      }
      return XmlUtil.decode(BrokerDTO.class, file);
   }
}
