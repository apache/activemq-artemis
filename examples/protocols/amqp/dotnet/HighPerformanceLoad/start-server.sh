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

# Setting the script to fail if anything goes wrong
set -e

rm -rf ./server1
../../../../../bin/artemis create ./server1 --user a --password a --role a --allow-anonymous --force
./target/server0/bin/artemis run
#sleep 1
#../../../../target/clustered-static-node1/bin/artemis run | tee server2.log &
#sleep 1
#../../../../target/clustered-static-node2/bin/artemis run | tee server3.log &
#sleep 1
