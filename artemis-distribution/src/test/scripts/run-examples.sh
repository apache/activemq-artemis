#!/usr/bin/env sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script will run all the examples on the distribution

rm -rf target
mkdir target

# Setting the script to fail if anything goes wrong
set -e

export TEST_TARGET="./target"

. ./installHome.sh

cd $ARTEMIS_HOME/examples/features/standard/

cd auto-closeable; mvn verify; cd ..
cd broker-plugin; mvn verify; cd ..
cd browser; mvn verify; cd ..
cd cdi; mvn verify; cd ..
cd client-kickoff; mvn verify; cd ..
cd completion-listener; mvn verify; cd ..
cd consumer-rate-limit; mvn verify; cd ..
cd context; mvn verify; cd ..
cd core-bridge; mvn verify; cd ..
cd database; mvn verify; cd ..
cd dead-letter; mvn verify; cd ..
cd delayed-redelivery; mvn verify; cd ..
cd divert; mvn verify; cd ..
cd durable-subscription; mvn verify; cd ..
cd embedded; mvn verify; cd ..
cd embedded-simple; mvn verify; cd ..
cd exclusive-queue; mvn verify; cd ..
cd expiry; mvn verify; cd ..
cd http-transport; mvn verify; cd ..
cd instantiate-connection-factory; mvn verify; cd ..
cd interceptor; mvn verify; cd ..
cd interceptor-amqp; mvn verify; cd ..
cd interceptor-client; mvn verify; cd ..
cd interceptor-mqtt; mvn verify; cd ..
cd jms-bridge; mvn verify; cd ..
cd jmx; mvn verify; cd ..
cd jmx-ssl; mvn verify; cd ..

# too big for most CI machines
#cd large-message; mvn verify; cd ..

cd last-value-queue; mvn verify; cd ..
cd management; mvn verify; cd ..
cd management-notifications; mvn verify; cd ..
cd message-counters; mvn verify; cd ..
cd message-group; mvn verify; cd ..
cd message-group2; mvn verify; cd ..
cd message-priority; mvn verify; cd ..
cd no-consumer-buffering; mvn verify; cd ..
cd paging; mvn verify; cd ..
cd pre-acknowledge; mvn verify; cd ..
cd producer-rate-limit; mvn verify; cd ..
cd queue; mvn verify; cd ..
cd queue-requestor; mvn verify; cd ..
cd queue-selector; mvn verify; cd ..
cd reattach-node; mvn verify; cd ..
cd request-reply; mvn verify; cd ..
cd rest; mvn verify; cd ..
cd scheduled-message; mvn verify; cd ..
cd security; mvn verify; cd ..
cd security-ldap; mvn verify; cd ..
cd security-manager; mvn verify; cd ..
cd send-acknowledgements; mvn verify; cd ..
cd shared-consumer; mvn verify; cd ..
cd slow-consumer; mvn verify; cd ..
cd spring-integration; mvn verify; cd ..
cd ssl-enabled; mvn verify; cd ..
cd ssl-enabled-crl-mqtt; mvn verify; cd ..
cd ssl-enabled-dual-authentication; mvn verify; cd ..
cd static-selector; mvn verify; cd ..
cd temp-queue; mvn verify; cd ..
cd topic; mvn verify; cd ..
cd topic-hierarchies; mvn verify; cd ..
cd topic-selector1; mvn verify; cd ..
cd topic-selector2; mvn verify; cd ..
cd transactional; mvn verify; cd ..
cd xa-heuristic; mvn verify; cd ..
cd xa-receive; mvn verify; cd ..
cd xa-send; mvn verify; cd ..


cd $ARTEMIS_HOME/examples/features/clustered/


cd client-side-load-balancing; mvn verify; cd ..
cd clustered-durable-subscription; mvn verify; cd ..
cd clustered-grouping; mvn verify; cd ..
cd clustered-jgroups; mvn verify; cd ..
cd clustered-queue; mvn verify; cd ..
cd clustered-static-oneway; mvn verify; cd ..
cd clustered-static-discovery; mvn verify; cd ..
cd clustered-static-discovery-uri; mvn verify; cd ..
cd clustered-topic; mvn verify; cd ..
cd clustered-topic-uri; mvn verify; cd ..
cd queue-message-redistribution; mvn verify; cd ..
cd symmetric-cluster; mvn verify; cd ..
cd shared-storage-static-cluster; mvn verify; cd ..


# TODO: these will hung eventually when ran in series

cd $ARTEMIS_HOME/examples/features/ha/

cd application-layer-failover; mvn verify; cd ..
cd client-side-failoverlistener; mvn verify; cd ..
cd colocated-failover; mvn verify; cd ..
cd colocated-failover-scale-down; mvn verify; cd ..
cd ha-policy-autobackup; mvn verify; cd ..
cd multiple-failover; mvn verify; cd ..
cd multiple-failover-failback; mvn verify; cd ..
cd non-transaction-failover; mvn verify; cd ..
cd replicated-failback; mvn verify; cd ..
cd replicated-failback-static; mvn verify; cd ..

cd replicated-multiple-failover; mvn verify; cd ..

cd replicated-transaction-failover; mvn verify; cd ..
cd scale-down; mvn verify; cd ..
cd transaction-failover; mvn verify; cd ..


cd $ARTEMIS_HOME/examples/protocols/amqp/


cd queue; mvn verify; cd ..


cd $ARTEMIS_HOME/examples/protocols/mqtt/


cd clustered-queue-mqtt; mvn verify; cd ..
cd publish-subscribe; mvn verify; cd ..


cd $ARTEMIS_HOME/examples/protocols/openwire/


cd queue; mvn verify; cd ..
cd message-listener; mvn verify; cd ..
cd message-recovery; mvn verify; cd ..
cd virtual-topic-mapping; mvn verify; cd ..


cd $ARTEMIS_HOME/examples/protocols/stomp/


cd stomp; mvn verify; cd ..
cd stomp1.1; mvn verify; cd ..
cd stomp1.2; mvn verify; cd ..
cd stomp-dual-authentication; mvn verify; cd ..
cd stomp-embedded-interceptor; mvn verify; cd ..
cd stomp-jms; mvn verify; cd ..


cd $CURRENT_DIR
rm -rf target
