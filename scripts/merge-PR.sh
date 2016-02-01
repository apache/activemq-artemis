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


# Use this to simplify the rebasing of PRs. PRs will be rebased during the merge on this process.
# use: ./rebase-PR <PRID> textual description

# this script assumes the following remote entries on your config
#
# - origin being your github fork:: https://github.com/YOU/activemq-artemis.git
# - upstream being the github fork for apache:: https://github.com/apache/activemq-artemis.git
# - apache being the apache origin:: https://git-wip-us.apache.org/repos/asf/activemq-artemis.git
#
# Notice: you should add +refs/pull/*/head to your fetch config on upstream
#        as specified on https://github.com/apache/activemq-artemis/blob/master/docs/hacking-guide/en/maintainers.md

git fetch origin
git fetch apache
git fetch upstream 

git checkout apache/master -B master
git checkout upstream/pr/$1 -B $1
git pull --rebase apache master
git checkout master
git merge --no-ff $1 -m "This closes #$*"
git branch -D $1
