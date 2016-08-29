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

# more information about this script at https://github.com/apache/activemq-artemis/blob/master/docs/hacking-guide/en/history.md

git remote add hornetq https://github.com/hornetq/hornetq.git
git fetch hornetq
git checkout hornetq/apache-donation -B donation
git checkout apache/master -B master-donation
git rebase donation
git remote rm hornetq
git branch -D donation
