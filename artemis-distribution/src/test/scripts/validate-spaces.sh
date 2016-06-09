#!/usr/bin/env bash
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

rm -rf target/with\ space
mkdir target
mkdir target/with\ space


# Setting the script to fail if anything goes wrong
set -e

export CURRENT_DIR=`pwd`

if [ $# -eq 0 ]; then
    export LOCAL_USED=`ls ../../../target/apache-artemis-*bin.zip`
    echo Unziping $LOCAL_USED
    unzip $LOCAL_USED -d "./target/with space"
    cd "./target/with space"
    export ARTEMIS_HOME="`pwd`/`ls`"
    echo home is $ARTEMIS_HOME
else
    if [ -z "$1" ] ; then
       echo "Couldn't find folder $1"
       exit -1
    fi
    export ARTEMIS_HOME="$CURRENT_DIR/target/with space/artemis_home"
    cp -r "$1" "$ARTEMIS_HOME"
fi

cd $CURRENT_DIR

export ARTEMIS_INSTANCE="$CURRENT_DIR/target/with space/artemis_instance"
echo home used is $ARTEMIS_HOME
echo artemis instance is $ARTEMIS_HOME


cd "$ARTEMIS_HOME/bin"
./artemis create --silent --force "$ARTEMIS_INSTANCE"

cd "$ARTEMIS_INSTANCE/bin"
pwd

./artemis run &

sleep 5

./artemis producer
./artemis consumer

./artemis stop

sleep 5
./artemis data print
./artemis data compact
./artemis data exp


./artemis-service start

sleep 5

./artemis producer
./artemis consumer

./artemis-service stop

cd $CURRENT_DIR
rm -rf target
