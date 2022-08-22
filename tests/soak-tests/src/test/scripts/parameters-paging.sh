#!/bin/sh
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

# this script contains a suggest set of variables to run the HorizontalPagingTest in a medium environment and hit some issues we used to have with paging

## Generic variable:
# It is possible to save the producer's time. If you set this variable the test will reuse previously sent data by zip and unzipping the data folder
#export TEST_ZIP_LOCATION=~/zipTest/

#HorizontalPagingTest

export TEST_HORIZONTAL_TEST_ENABLED=true
export TEST_HORIZONTAL_SERVER_START_TIMEOUT=300000
export TEST_HORIZONTAL_TIMEOUT_MINUTES=120
export TEST_HORIZONTAL_PROTOCOL_LIST=OPENWIRE,CORE,AMQP

export TEST_HORIZONTAL_CORE_DESTINATIONS=200
export TEST_HORIZONTAL_CORE_MESSAGES=1000
export TEST_HORIZONTAL_CORE_COMMIT_INTERVAL=100
export TEST_HORIZONTAL_CORE_RECEIVE_COMMIT_INTERVAL=0
export TEST_HORIZONTAL_CORE_MESSAGE_SIZE=20000
export TEST_HORIZONTAL_CORE_PARALLEL_SENDS=10

export TEST_HORIZONTAL_AMQP_DESTINATIONS=200
export TEST_HORIZONTAL_AMQP_MESSAGES=1000
export TEST_HORIZONTAL_AMQP_COMMIT_INTERVAL=100
export TEST_HORIZONTAL_AMQP_RECEIVE_COMMIT_INTERVAL=0
export TEST_HORIZONTAL_AMQP_MESSAGE_SIZE=20000
export TEST_HORIZONTAL_AMQP_PARALLEL_SENDS=10

export TEST_HORIZONTAL_OPENWIRE_DESTINATIONS=200
export TEST_HORIZONTAL_OPENWIRE_MESSAGES=1000
export TEST_HORIZONTAL_OPENWIRE_COMMIT_INTERVAL=100
export TEST_HORIZONTAL_OPENWIRE_RECEIVE_COMMIT_INTERVAL=0
export TEST_HORIZONTAL_OPENWIRE_MESSAGE_SIZE=20000
export TEST_HORIZONTAL_OPENWIRE_PARALLEL_SENDS=10

export TEST_FLOW_SERVER_START_TIMEOUT=300000
export TEST_FLOW_TIMEOUT_MINUTES=120


# FlowControlPagingTest
export TEST_FLOW_PROTOCOL_LIST=CORE,AMQP,OPENWIRE
export TEST_FLOW_PRINT_INTERVAL=100

export TEST_FLOW_OPENWIRE_MESSAGES=10000
export TEST_FLOW_OPENWIRE_COMMIT_INTERVAL=1000
export TEST_FLOW_OPENWIRE_RECEIVE_COMMIT_INTERVAL=10
export TEST_FLOW_OPENWIRE_MESSAGE_SIZE=60000

export TEST_FLOW_CORE_MESSAGES=10000
export TEST_FLOW_CORE_COMMIT_INTERVAL=1000
export TEST_FLOW_CORE_RECEIVE_COMMIT_INTERVAL=10
export TEST_FLOW_CORE_MESSAGE_SIZE=30000

export TEST_FLOW_AMQP_MESSAGES=10000
export TEST_FLOW_AMQP_COMMIT_INTERVAL=1000
export TEST_FLOW_AMQP_RECEIVE_COMMIT_INTERVAL=10
export TEST_FLOW_AMQP_MESSAGE_SIZE=30000


# SubscriptionPagingTest
export TEST_SUBSCRIPTION_PROTOCOL_LIST=CORE

export TEST_SUBSCRIPTION_SERVER_START_TIMEOUT=300000
export TEST_SUBSCRIPTION_TIMEOUT_MINUTES=120
export TEST_SUBSCRIPTION_PRINT_INTERVAL=100
export TEST_SUBSCRIPTION_SLOW_SUBSCRIPTIONS=5


export TEST_SUBSCRIPTION_CORE_MESSAGES=10000
export TEST_SUBSCRIPTION_CORE_COMMIT_INTERVAL=1000
export TEST_SUBSCRIPTION_CORE_RECEIVE_COMMIT_INTERVAL=0
export TEST_SUBSCRIPTION_CORE_MESSAGE_SIZE=30000
export TEST_SUBSCRIPTION_SLEEP_SLOW=1000