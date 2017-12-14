package servers
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

// starts an artemis server

import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS
import org.apache.activemq.artemis.tests.compatibility.GroovyRun;


String folder = arg[0];
String id = arg[1];
String type = arg[2];
String producer = arg[3];
String consumer = arg[4];

println("type = " + type);

configuration = new ConfigurationImpl();
configuration.setJournalType(JournalType.NIO);
System.out.println("folder:: " + folder);
configuration.setBrokerInstance(new File(folder + "/" + id));
configuration.addAcceptorConfiguration("artemis", "tcp://0.0.0.0:61616");
configuration.setSecurityEnabled(false);
configuration.setPersistenceEnabled(false);
try {
    if (!type.equals("ARTEMIS-140")) {
        configuration.addAddressesSetting("#", new AddressSettings().setAutoCreateAddresses(true));
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

// uncomment this line to validate https://issues.apache.org/jira/browse/ARTEMIS-1561
// this api exists on both 1.4 and 2.x... so, this one was preferred for this
if (producer.toString().startsWith("HORNETQ")) {
    // hornetq servers won't auto-create
    server.getJMSServerManager().createQueue(true, "queue", null, true);
}
