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

package ReplicationTest

import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl

evaluate(new File('ReplicationTest/testConfiguration.groovy'))

final clusterConnectionConfiguration = new ClusterConnectionConfiguration()
clusterConnectionConfiguration.name = 'test'
clusterConnectionConfiguration.connectorName = 'slave'
clusterConnectionConfiguration.staticConnectors = ['master']

final haConfiguration = new ReplicaPolicyConfiguration()
haConfiguration.clusterName = 'test'
haConfiguration.allowFailBack = true

final configuration = new ConfigurationImpl()
configuration.name = 'slave'
configuration.brokerInstance = new File(workingDir, 'slave')
configuration.addAcceptorConfiguration('artemis', "tcp://${slaveBindAddress}:${slaveBindPort}?protocols=CORE")
configuration.createJournalDir = true
configuration.securityEnabled = false
configuration.persistenceEnabled = true
configuration.addConnectorConfiguration('master', "tcp://${masterBindAddress}:${masterBindPort}")
configuration.addConnectorConfiguration('slave', "tcp://${slaveBindAddress}:${slaveBindPort}")
configuration.setHAPolicyConfiguration(haConfiguration)
configuration.addClusterConfiguration(clusterConnectionConfiguration)

server = new ActiveMQServerImpl(configuration)
println "Created \"${configuration.name}\" server of version ${server.version.fullVersion}."

evaluate(new File('servers/startServer.groovy'))