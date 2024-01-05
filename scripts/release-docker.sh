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

set -e

if [ $# -ne 2 ]
  then
    echo "Please supply version and repository name as parameters, e.g. ./release-docker 2.30.0 apache"
    exit
fi

VERSION=$1
REPO=$2

cd ../artemis-docker
rm -Rf _TMP_/
./prepare-docker.sh --from-release --artemis-version ${VERSION}
cd _TMP_/artemis/${VERSION}
docker pull eclipse-temurin:21-jre-alpine
docker pull eclipse-temurin:21-jre
docker build -f ./docker/Dockerfile-alpine-21-jre -t ${REPO}/activemq-artemis:${VERSION}-alpine .
docker tag ${REPO}/activemq-artemis:${VERSION}-alpine ${REPO}/activemq-artemis:latest-alpine
docker build -f ./docker/Dockerfile-ubuntu-21-jre -t ${REPO}/activemq-artemis:${VERSION} .
docker tag ${REPO}/activemq-artemis:${VERSION} ${REPO}/activemq-artemis:latest
docker login
docker push ${REPO}/activemq-artemis:${VERSION}-alpine
docker push ${REPO}/activemq-artemis:${VERSION}
docker push ${REPO}/activemq-artemis:latest-alpine
docker push ${REPO}/activemq-artemis:latest