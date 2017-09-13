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

#This is a script to help with the release process


error () {
   echo ""
   echo "$@"
   echo ""
   echo "Usage: ./download-release.sh repo-url version target"
   echo ""
   echo "example:"
   echo "./download-release.sh https://repo1.maven.org/maven2 2.3.0 ./target"
   echo ""
   exit 64
}

doDownload () {

  theFile="$1"
  completeURL="$repoURL/$theFile"

  echo $theFile

  echo "Downloading $theFile from $completeURL"
  curl $completeURL > $theFile

  echo "Downloading $theFile.asc"
  curl $completeURL.asc > $theFile.asc

  echo "Downloading $theFile.md5"
  curl $completeURL.md5 > $theFile.md5

  echo "Downloading $theFile.sha1"
  curl $completeURL.sha1 > $theFile.sha1
}

if [ "$#" != 3 ]; then
  error "Cannot match arguments"
fi

release=$2

if [ -d $3 ]; then
  cd $3
else
  error "Directory $3 does not exist"
fi

repoURL="$1/org/apache/activemq/apache-artemis/$2"

doDownload apache-artemis-$release-bin.tar.gz 
doDownload apache-artemis-$release-bin.zip
doDownload apache-artemis-$release-source-release.tar.gz
doDownload apache-artemis-$release-source-release.zip

