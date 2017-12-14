package clients
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

// Create a client connection factory

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory
import org.apache.activemq.artemis.tests.compatibility.GroovyRun;

println("serverType " + serverArg[0]);

if (serverArg[0].startsWith("HORNETQ")) {
    cf = new ActiveMQConnectionFactory("tcp://localhost:61616?protocolManagerFactoryStr=org.apache.activemq.artemis.core.protocol.hornetq.client.HornetQClientProtocolManagerFactory&confirmationWindowSize=1048576&blockOnDurableSend=false");
} else {
    cf = new ActiveMQConnectionFactory("tcp://localhost:61616?confirmationWindowSize=1048576&blockOnDurableSend=false");
}


GroovyRun.assertTrue(!cf.getServerLocator().isBlockOnDurableSend());
GroovyRun.assertEquals(1048576, cf.getServerLocator().getConfirmationWindowSize());

