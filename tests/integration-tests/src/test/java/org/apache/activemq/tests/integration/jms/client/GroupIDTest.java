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
package org.apache.activemq.tests.integration.jms.client;

import org.junit.Test;

import javax.jms.ConnectionFactory;
import javax.jms.Message;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.jms.HornetQJMSClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.jms.client.HornetQJMSConnectionFactory;

/**
 * A GroupIDTest
 *
 * @author Tim Fox
 *
 *
 */
public class GroupIDTest extends GroupingTest
{

   @Override
   protected ConnectionFactory getCF() throws Exception
   {
      HornetQJMSConnectionFactory cf = (HornetQJMSConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      cf.setGroupID("wibble");

      return cf;
   }


   @Test
   public void testManyGroups()
   {
      // this test does not make sense here
   }

   @Override
   protected void setProperty(Message message)
   {
   }
}
