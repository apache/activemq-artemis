#!/usr/bin/env ruby
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

require 'qpid_proton'

address = "127.0.0.1:5672/testQueue"

messenger = Qpid::Proton::Messenger.new
messenger.start

msg = Qpid::Proton::Message.new
msg.address = address
msg.subject = "The time is #{Time.new}"
msg.body = "Hello world!"
msg.correlation_id = "554545"

begin
    messenger.put(msg)
rescue Qpid::Proton::ProtonError => error
    puts "ERROR: #{error.message}"
    exit
end

begin
    messenger.send
rescue Qpid::Proton::ProtonError => error
    puts "ERROR: #{error.message}"
    puts error.backtrace.join("\n")
    exit
end

puts "SENT: " + msg.body

messenger.stop
