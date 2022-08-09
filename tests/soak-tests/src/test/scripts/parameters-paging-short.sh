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

# Setting the script to fail if anything goes wrong
set -e

# this script contains a suggest set of variables to run the HorizontalPagingTest in a medium environment and hit some issues we used to have with paging

# It is possible to save the producer's time. If you set this variable the test will reuse previously sent data by zip and unzipping the data folder
export TEST_HORIZONTAL_ZIP_LOCATION=/tmp

export TEST_HORIZONTAL_SERVER_START_TIMEOUT=300000
export TEST_HORIZONTAL_TIMEOUT_MINUTES=120
export TEST_HORIZONTAL_PROTOCOL_LIST=OPENWIRE,CORE,AMQP

export TEST_HORIZONTAL_CORE_DESTINATIONS=2
export TEST_HORIZONTAL_CORE_MESSAGES=1000
export TEST_HORIZONTAL_CORE_COMMIT_INTERVAL=100
export TEST_HORIZONTAL_CORE_RECEIVE_COMMIT_INTERVAL=0
export TEST_HORIZONTAL_CORE_MESSAGE_SIZE=20000
export TEST_HORIZONTAL_CORE_PARALLEL_SENDS=10

export TEST_HORIZONTAL_AMQP_DESTINATIONS=2
export TEST_HORIZONTAL_AMQP_MESSAGES=1000
export TEST_HORIZONTAL_AMQP_COMMIT_INTERVAL=100
export TEST_HORIZONTAL_AMQP_RECEIVE_COMMIT_INTERVAL=0
export TEST_HORIZONTAL_AMQP_MESSAGE_SIZE=20000
export TEST_HORIZONTAL_AMQP_PARALLEL_SENDS=10

export TEST_HORIZONTAL_OPENWIRE_DESTINATIONS=2
export TEST_HORIZONTAL_OPENWIRE_MESSAGES=1000
export TEST_HORIZONTAL_OPENWIRE_COMMIT_INTERVAL=100
export TEST_HORIZONTAL_OPENWIRE_RECEIVE_COMMIT_INTERVAL=0
export TEST_HORIZONTAL_OPENWIRE_MESSAGE_SIZE=20000
export TEST_HORIZONTAL_OPENWIRE_PARALLEL_SENDS=10
