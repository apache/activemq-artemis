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
/**
 * This package is used to locate resources and connectors along the cluster set
 * I - JCA Connection Factories or InBound MDBs will call ActiveMQRegistryBase::register(XARecoveryConfig)
 * II - For each XARecoveryConfig the RegistryBase will instantiate a ResourceDiscoveryUnit which will
 *      connect using that configuration and inform the Registry of any topology members
 * III - For each topology member found on the DiscoveryUnits, the RegistryBase will registry a ActiveMQResourceRecovery
 *       that will exist per server
  */
package org.apache.activemq.jms.server.recovery;

