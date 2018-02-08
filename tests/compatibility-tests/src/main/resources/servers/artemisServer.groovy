package servers
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

// starts an artemis server

import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.JournalType
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS
import org.apache.activemq.artemis.tests.compatibility.GroovyRun;


String folder = arg[0];
String id = arg[1];
String type = arg[2];
String producer = arg[3];
String consumer = arg[4];
String globalMaxSize = arg[5];

println("type = " + type);
println("globalMaxSize = " + globalMaxSize);

configuration = new ConfigurationImpl();
configuration.setJournalType(JournalType.NIO);
System.out.println("folder:: " + folder);
configuration.setBrokerInstance(new File(folder + "/" + id));
configuration.addAcceptorConfiguration("artemis", "tcp://0.0.0.0:61616?anycastPrefix=jms.queue.&multicastPrefix=jms.topic.");
configuration.setSecurityEnabled(false);
configuration.setPersistenceEnabled(persistent);
try {
    if (!type.startsWith("ARTEMIS-1")) {
        configuration.addAddressesSetting("#", new AddressSettings().setAutoCreateAddresses(true));
        if (globalMaxSize != null) {
            configuration.getAddressesSettings().get("#").setPageSizeBytes(globalMaxSize).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE)
            configuration.setGlobalMaxSize(Long.parseLong(globalMaxSize));
        }
    }
} catch (Throwable e) {
    // need to ignore this for 1.4
    e.printStackTrace();
}

jmsConfiguration = new JMSConfigurationImpl();

server = new EmbeddedJMS();
server.setConfiguration(configuration);
server.setJmsConfiguration(jmsConfiguration);
server.start();

server.getJMSServerManager().createTopic(true, "topic");
server.getJMSServerManager().createQueue(true, "queue", null, true);
