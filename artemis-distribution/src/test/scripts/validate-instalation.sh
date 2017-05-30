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

# This script will validate the distribution works with folders with spaces on Linux machines

echo validating installation on $1
rm -rf target
mkdir target
mkdir target/"$1"


# Setting the script to fail if anything goes wrong
set -e


export TEST_TARGET="./target/$1"

. ./installHome.sh


export ARTEMIS_INSTANCE="$CURRENT_DIR/target/$1/artemis_instance"
echo home used is $ARTEMIS_HOME
echo artemis instance is $ARTEMIS_HOME


cd "$ARTEMIS_HOME/bin"
./artemis create --silent --force "$ARTEMIS_INSTANCE"

cd "$ARTEMIS_INSTANCE/bin"
pwd

./artemis run &

sleep 5

./artemis producer
./artemis consumer --receive-timeout 10000 --break-on-null

./artemis stop

sleep 5
./artemis data print > data.log
./artemis data compact
./artemis data exp


./artemis-service start

sleep 5

./artemis producer
./artemis consumer --receive-timeout 10000 --break-on-null

./artemis-service stop

cd $CURRENT_DIR
rm -rf target
