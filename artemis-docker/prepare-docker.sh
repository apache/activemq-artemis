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

#This is a script to Prepare an artemis folder to generate the Release.


error () {
   echo ""
   echo "$@"
   echo ""
   echo "Usage: ./prepare-docker.sh ARTEMIS_HOME_LOCATION"
   echo ""
   echo "example:"
   echo "./prepare-docker.sh ../artemis-distribution/target/apache-artemis-2.7.0-SNAPSHOT-bin/apache-artemis-2.7.0-SNAPSHOT"
   echo ""
   exit 64
}

if [ ! "$#" -eq 1 ]
then
   error "Cannot match arguments"
fi

target=$1

if [ ! -d $target ]
then
  error "Directory $target does not exist"
fi

if [ -d $target/docker ]
then
  rm -rf $target/docker
fi
mkdir $target/docker
cp ./{Dockerfile-centos,Dockerfile-ubuntu,docker-run.sh} $target/docker

echo "Docker file support files at : $target/docker"
